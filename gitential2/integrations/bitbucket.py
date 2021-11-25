from typing import Optional, Callable, List, Tuple
from datetime import datetime
from urllib.parse import urlparse

from structlog import get_logger

from authlib.integrations.requests_client import OAuth2Session

from pydantic.datetime_parse import parse_datetime

from gitential2.datatypes import UserInfoCreate, RepositoryCreate, GitProtocol, RepositoryInDB
from gitential2.datatypes.authors import AuthorAlias

from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestState,
    PullRequestData,
    PullRequestCommit,
    PullRequestComment,
)
from gitential2.utils import calc_repo_namespace
from .base import BaseIntegration, OAuthLoginMixin, GitProviderMixin
from .common import log_api_error
from ..utils.is_bugfix import calculate_is_bugfix

logger = get_logger(__name__)


class BitBucketIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    def get_client(self, token, update_token) -> OAuth2Session:
        return self.get_oauth2_client(token=token, update_token=update_token)

    def oauth_register(self):
        return {
            "api_base_url": "https://api.bitbucket.org/2.0/",
            "access_token_url": "https://bitbucket.org/site/oauth2/access_token",
            "authorize_url": "https://bitbucket.org/site/oauth2/authorize",
            "userinfo_endpoint": "https://api.bitbucket.org/2.0/user",
            "client_kwargs": {"scope": "email repository pullrequest account"},
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
            "token_endpoint": "https://bitbucket.org/site/oauth2/access_token",
        }

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        user_info_dict = {
            "integration_name": self.name,
            "integration_type": "bitbucket",
            "sub": data["account_id"],
            "name": data["display_name"],
            "preferred_username": data["username"],
            "address": data.get("location"),
            "website": data.get("website"),
            "picture": _get_avatar(data),
            "profile": _get_profile(data),
            "extra": data,
        }
        client = self.get_oauth2_client(token=token)
        response = client.get(self.oauth_register()["api_base_url"] + "user/emails")
        if response.status_code != 200:
            log_api_error(response)
        response.raise_for_status()
        user_info_dict.update(_get_email(response.json()))

        return UserInfoCreate(**user_info_dict)

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        return True, self.refresh_token(token, update_token)

    def refresh_token(self, token, update_token):
        client = self.get_oauth2_client(token=token, update_token=update_token)
        urls = self.oauth_register()
        new_token = client.refresh_token(urls["token_endpoint"], refresh_token=token["refresh_token"])
        client.close()
        return new_token

    def _collect_raw_pull_requests(self, repository: RepositoryInDB, client) -> list:
        api_base_url = self.oauth_register()["api_base_url"]
        workspace, repo_slug = self._get_bitbucket_workspace_and_repo_slug(repository)
        prs = _walk_paginated_results(
            client,
            f"{api_base_url}repositories/{workspace}/{repo_slug}/pullrequests?state=MERGED&state=SUPERSEDED&state=OPEN&state=DECLINED",
        )
        return prs

    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        return raw_pr["id"], parse_datetime(raw_pr["updated_on"])

    def _collect_raw_pull_request(self, repository: RepositoryInDB, pr_number: int, client) -> dict:
        api_base_url = self.oauth_register()["api_base_url"]
        workspace, repo_slug = self._get_bitbucket_workspace_and_repo_slug(repository)
        pr_url = f"{api_base_url}repositories/{workspace}/{repo_slug}/pullrequests/{pr_number}"

        resp = client.get(pr_url)
        resp.raise_for_status()
        pr_details = resp.json()

        commits = _walk_paginated_results(client, pr_details["links"]["commits"]["href"])
        review_comments = _walk_paginated_results(client, pr_details["links"]["comments"]["href"])
        diffstat = _walk_paginated_results(client, pr_details["links"]["diffstat"]["href"])
        return {"pr": pr_details, "commits": commits, "review_comments": review_comments, "diffstat": diffstat}

    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        pull_request = self._transform_to_pr(raw_data, repository=repository, author_callback=author_callback)
        commits = self._transform_to_commits(raw_data["commits"], raw_data, repository)
        comments = self._transform_to_comments(
            raw_data["review_comments"], raw_data, repository, author_callback=author_callback
        )
        return PullRequestData(pr=pull_request, comments=comments, commits=commits, labels=[])

    def _get_bitbucket_workspace_and_repo_slug(self, repositor: RepositoryInDB):
        clone_url = repositor.clone_url
        if clone_url.startswith("https://"):
            parsed_url = urlparse(clone_url)
            path_parts = parsed_url.path.split("/")
            return "/".join(path_parts[:-1]).strip("/"), path_parts[-1].replace(".git", "")
        else:
            logger.error("Don't know how to parse clone_url for workspace, and repo slug", clone_url=clone_url)
            raise ValueError("Don't know how to parse clone_url for workspace, and repo slug")

    def _transform_to_pr(self, raw_data, repository, author_callback):
        def _calc_first_commit_authored_at(raw_commits):
            author_times = [c["date"] for c in raw_commits]
            author_times.sort()
            return author_times[0] if author_times else None

        def _calc_first_reaction_at(raw_pr, review_comments):
            human_note_creation_times = [rc["created_on"] for rc in review_comments]
            human_note_creation_times.sort()
            return (
                human_note_creation_times[0]
                if human_note_creation_times
                else raw_pr.get("merged_on", raw_pr.get("closed_on"))
            )

        def _calc_addition_and_deletion_changed_files(diffstat):
            additions, deletions, changed_files = 0, 0, 0
            for change in diffstat:
                changed_files += 1
                additions += change["lines_added"]
                deletions += change["lines_removed"]

            return additions, deletions, changed_files

        additions, deletions, changed_files = _calc_addition_and_deletion_changed_files(raw_data["diffstat"])

        state = PullRequestState.from_bitbucket(raw_data["pr"]["state"])

        user_author_id = author_callback(
            AuthorAlias(name=raw_data["pr"]["author"]["display_name"], login=raw_data["pr"]["author"]["nickname"])
        )

        merged_by_author_id = (
            author_callback(
                AuthorAlias(
                    name=raw_data["pr"]["closed_by"]["display_name"], login=raw_data["pr"]["closed_by"]["nickname"]
                )
            )
            if state == PullRequestState.merged and raw_data["pr"].get("closed_by", {})
            else None
        )

        return PullRequest(
            repo_id=repository.id,
            number=raw_data["pr"]["id"],
            title=raw_data["pr"].get("title", "<missing title>"),
            platform="bitbucket",
            id_platform=raw_data["pr"]["id"],
            api_resource_uri=raw_data["pr"]["links"]["self"]["href"],
            state_platform=raw_data["pr"]["state"],
            state=state,
            created_at=raw_data["pr"]["created_on"],
            closed_at=raw_data["pr"].get("updated_on") if state == PullRequestState.closed else None,
            updated_at=raw_data["pr"]["updated_on"],
            merged_at=raw_data["pr"].get("updated_on") if state == PullRequestState.merged else None,
            additions=additions,
            deletions=deletions,
            changed_files=changed_files,
            draft=False,
            user=raw_data["pr"]["author"]["nickname"],
            user_id_external=str(raw_data["pr"]["author"]["uuid"]),
            user_name_external=raw_data["pr"]["author"]["display_name"],
            user_username_external=raw_data["pr"]["author"]["nickname"],
            user_aid=user_author_id,
            commits=len(raw_data["commits"]),
            merged_by_aid=merged_by_author_id,
            merged_by=raw_data["pr"]["closed_by"]["nickname"] if merged_by_author_id else None,
            first_reaction_at=_calc_first_reaction_at(raw_data["pr"], raw_data["review_comments"]),
            first_commit_authored_at=_calc_first_commit_authored_at(raw_data["commits"]),
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["pr"].get("title", "<missing title>")),
        )

    def _transform_to_commits(
        self,
        commits_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
    ) -> List[PullRequestCommit]:
        ret = []
        for commit_raw in commits_raw:
            commit = PullRequestCommit(
                repo_id=repository.id,
                pr_number=raw_data["pr"]["id"],
                commit_id=commit_raw["hash"],
                author_name=commit_raw["author"]["raw"],
                author_email=commit_raw["author"]["raw"],
                author_date=commit_raw["date"],
                author_login=None,
                committer_name=commit_raw["author"]["raw"],
                committer_email=commit_raw["author"]["raw"],
                committer_date=commit_raw["date"],
                committer_login=None,
                created_at=commit_raw["date"],
                updated_at=commit_raw["date"],
                extra=commit_raw,
            )
            ret.append(commit)
        return ret

    def _transform_to_comments(
        self,
        comments_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
        author_callback: Callable,
    ) -> List[PullRequestComment]:
        ret = []
        for comment_raw in comments_raw:
            author_aid = (
                author_callback(
                    AuthorAlias(name=comment_raw["user"]["display_name"], login=comment_raw["user"]["nickname"])
                )
                if comment_raw["user"].get("nickname") and comment_raw["user"].get("display_name")
                else None
            )

            comment = PullRequestComment(
                repo_id=repository.id,
                pr_number=raw_data["pr"]["id"],
                comment_type=str(comment_raw["type"]),
                comment_id=str(comment_raw["id"]),
                author_id_external=comment_raw["user"]["uuid"],
                author_name_external=comment_raw["user"]["display_name"],
                author_username_external=comment_raw["user"]["nickname"],
                author_aid=author_aid,
                content=comment_raw["content"]["raw"],
                extra=comment_raw,
                created_at=comment_raw["created_on"],
                updated_at=comment_raw["updated_on"],
                published_at=comment_raw["updated_on"],
            )
            ret.append(comment)
        return ret

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        api_base_url = self.oauth_register()["api_base_url"]
        repository_list = _walk_paginated_results(client, f"{api_base_url}repositories?role=member&pagelen=100")
        client.close()
        return [self._repo_to_create_repo(repo) for repo in repository_list]

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        # Not API for this on BitBucket :(
        return []

    def _repo_to_create_repo(self, repo_dict) -> RepositoryCreate:
        protocol, clone_url = _get_protocol_and_clone_url(repo_dict["links"]["clone"])

        return RepositoryCreate(
            clone_url=clone_url,
            protocol=protocol,
            name=repo_dict["name"],
            namespace=calc_repo_namespace(clone_url),
            private=repo_dict.get("is_private", False),
            integration_type="bitbucket",
            integration_name=self.name,
            extra=repo_dict,
        )


def _get_protocol_and_clone_url(clone_links):
    for clone_link in clone_links:
        if clone_link["name"] == "https":
            return GitProtocol.https, clone_link["href"]

    # No https, return with the first one
    href = clone_links[0]["href"]
    return GitProtocol.https if href.startswith("https") else GitProtocol.ssh, href


# def _walk_paginated_results(client, starting_url, acc=None):
#     response = client.get(starting_url)
#     acc = acc or []

#     if response.status_code != 200:
#         log_api_error(response)
#         return acc

#     data = response.json()
#     if "values" in data:
#         acc += data["values"]
#     if "next" in data:
#         return _walk_paginated_results(client, data["next"], acc)
#     else:
#         return acc


def _walk_paginated_results(client, starting_url, acc=None):
    acc = acc or []
    next_url = starting_url
    while True:
        response = client.get(next_url)

        if response.status_code != 200:
            log_api_error(response)
            break
        data = response.json()
        if "values" in data:
            acc += data["values"]

        if "next" in data:
            next_url = data["next"]
        else:
            break

    return acc


def _get_profile(data):
    return "https://bitbucket.org/{}/".format(data["username"])


def _get_avatar(data):
    avatar = data["links"].get("avatar")
    if avatar:
        return avatar.get("href")
    return None


def _get_email(data):
    confirmed_emails = []
    values = data["values"]
    for value in values:
        if value["is_primary"]:
            return {"email": value["email"], "email_verified": value["is_confirmed"]}
        if value["is_confirmed"]:
            confirmed_emails.append(value["email"])

    if confirmed_emails:
        return {"email": confirmed_emails[0], "email_verified": True}

    if values:
        return {"email": values[0], "email_verified": False}

    return {}
