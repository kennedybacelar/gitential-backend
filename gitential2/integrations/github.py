from typing import Callable, Optional, List
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger
from gitential2.datatypes import UserInfoCreate, RepositoryInDB, RepositoryCreate, GitProtocol
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestState,
    PullRequestData,
    PullRequestCommit,
    PullRequestComment,
)
from gitential2.extraction.output import OutputHandler
from .base import CollectPRsResult, OAuthLoginMixin, BaseIntegration, GitProviderMixin
from .common import log_api_error, walk_next_link
from ..utils.is_bugfix import calculate_is_bugfix

logger = get_logger(__name__)


class GithubIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        if not data.get("email"):
            logger.warning("GitHub: Getting all emails because of private email setting.", userinfo=data)
            client = self.get_oauth2_client(token=token)
            response = client.get(self.oauth_register()["api_base_url"] + "user/emails")
            response.raise_for_status()
            emails = response.json()
            data["email"] = next(email["email"] for email in emails if email["primary"])

        logger.info("user info data:", data=data)
        return UserInfoCreate(
            integration_name=self.name,
            integration_type="github",
            sub=str(data["id"]),
            name=data["name"],
            email=data["email"],
            preferred_username=data["login"],
            profile=data["html_url"],
            picture=data["avatar_url"],
            website=data.get("blog"),
            extra=data,
        )

    def oauth_register(self):
        api_base_url = self.settings.options.get("api_base_url", "https://api.github.com/")
        return {
            "api_base_url": api_base_url,
            "access_token_url": self.settings.options.get(
                "access_token_url", "https://github.com/login/oauth/access_token"
            ),
            "authorize_url": self.settings.options.get("authorize_url", "https://github.com/login/oauth/authorize"),
            "client_kwargs": {"scope": "user:email repo"},
            "userinfo_endpoint": self.settings.options.get("userinfo_endpoint", api_base_url + "user"),
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def refresh_token_if_expired(self, token, update_token: Callable) -> bool:
        return False

    def get_rate_limit(self, token, update_token: Callable):
        api_base_url = self.oauth_register()["api_base_url"]
        client = self.get_oauth2_client(token=token, update_token=update_token)
        response = client.get(f"{api_base_url}rate_limit")
        if response.status_code == 200:
            rate_limit, headers = response.json(), response.headers

            logger.info("Github API rate limit", rate_limit=rate_limit, headers=headers)
            return rate_limit.get("core", None)
        return None

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        prs_we_already_have: Optional[dict] = None,
        limit: int = 200,
    ) -> CollectPRsResult:
        api_base_url = self.oauth_register()["api_base_url"]

        pr_list_url = f"{api_base_url}repos/{repository.namespace}/{repository.name}/pulls?per_page=100&state=all"
        client = self.get_oauth2_client(token=token, update_token=update_token)
        prs = walk_next_link(client, pr_list_url)

        def _is_pr_up_to_date(pr: dict) -> bool:
            pr_number = pr["number"]
            return (
                prs_we_already_have is not None
                and pr_number in prs_we_already_have
                and parse_datetime(prs_we_already_have[pr_number]) == parse_datetime(pr["updated_at"])
            )

        prs_needs_update = [pr for pr in prs if not _is_pr_up_to_date(pr)]

        rate_limit = self.get_rate_limit(token, update_token)
        if rate_limit and rate_limit["remaining"] < 1000:
            logger.warn(
                "Skipping pr collection because API rate limit",
                rate_limit=rate_limit,
                repository_name=repository.name,
                repository_id=repository.id,
            )
            return CollectPRsResult(prs_collected=[], prs_left=[pr["number"] for pr in prs_needs_update], prs_failed=[])

        counter = 0
        ret = CollectPRsResult(prs_collected=[], prs_left=[], prs_failed=[])
        for pr in prs_needs_update:
            pr_number = pr["number"]
            if counter >= limit:
                ret.prs_left.append(pr_number)
            else:
                pr_data = self.collect_pull_request(repository, token, update_token, output, pr_number)
                if pr_data:
                    ret.prs_collected.append(pr_number)
                else:
                    ret.prs_failed.append(pr_number)
            counter += 1

        return ret

    def collect_pull_request(
        self, repository: RepositoryInDB, token: dict, update_token: Callable, output: OutputHandler, pr_number: int
    ) -> Optional[PullRequestData]:
        api_base_url = self.oauth_register()["api_base_url"]
        pr_url = f"{api_base_url}repos/{repository.namespace}/{repository.name}/pulls/{pr_number}"
        client = self.get_oauth2_client(token=token, update_token=update_token)
        raw_data = None
        try:
            raw_data = self._collect_single_pr_data_raw(client, pr_url)
            pull_request = self._transform_to_pr(raw_data, repository=repository)
            commits = self._write_pr_commits(raw_data["commits"], raw_data, repository, output)
            comments = self._write_pr_comments(raw_data["review_comments"], raw_data, repository, output)
            output.write(ExtractedKind.PULL_REQUEST, pull_request)
            return PullRequestData(pr=pull_request, comments=comments, commits=commits, labels=[])
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to extract PR", pr_number=pr_number, raw_data=raw_data)
            return None
        finally:
            client.close()

    def _collect_single_pr_data_raw(self, client, pr_url):
        resp = client.get(pr_url)
        resp.raise_for_status()
        pr_details = resp.json()
        commits = walk_next_link(client, pr_details["_links"]["commits"]["href"])
        review_comments = walk_next_link(client, pr_details["_links"]["review_comments"]["href"])
        return {
            "pr": pr_details,
            "commits": commits,
            "review_comments": review_comments,
        }

    def _transform_to_pr(self, raw_data, repository):
        def _calc_first_commit_authored_at(raw_commits):
            author_times = [c["commit"]["author"]["date"] for c in raw_commits]
            author_times.sort()
            return author_times[0] if author_times else None

        def _calc_first_reaction_at(raw_pr, review_comments):
            human_note_creation_times = [rc["created_at"] for rc in review_comments]
            human_note_creation_times.sort()
            return (
                human_note_creation_times[0]
                if human_note_creation_times
                else raw_pr.get("merged_at", raw_pr.get("closed_at"))
            )

        return PullRequest(
            repo_id=repository.id,
            number=raw_data["pr"]["number"],
            title=raw_data["pr"].get("title", "<missing title>"),
            platform="github",
            id_platform=raw_data["pr"]["id"],
            api_resource_uri=raw_data["pr"]["url"],
            state_platform=raw_data["pr"]["state"],
            state=PullRequestState.from_github(raw_data["pr"]["state"], raw_data["pr"].get("merged_at")),
            created_at=raw_data["pr"]["created_at"],
            closed_at=raw_data["pr"].get("closed_at"),
            updated_at=raw_data["pr"]["updated_at"],
            merged_at=raw_data["pr"].get("merged_at"),
            additions=raw_data["pr"]["additions"],
            deletions=raw_data["pr"]["deletions"],
            changed_files=raw_data["pr"]["changed_files"],
            draft=raw_data["pr"]["draft"],
            user=raw_data["pr"]["user"]["login"],
            commits=len(raw_data["commits"]),
            merged_by=raw_data["pr"]["merged_by"]["login"]
            if "merged_by" in raw_data["pr"] and raw_data["pr"]["merged_by"] is not None
            else None,
            first_reaction_at=_calc_first_reaction_at(raw_data["pr"], raw_data["review_comments"]),
            first_commit_authored_at=_calc_first_commit_authored_at(raw_data["commits"]),
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["pr"].get("title", "<missing title>")),
        )

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        api_base_url = self.oauth_register()["api_base_url"]

        repository_list = walk_next_link(client, f"{api_base_url}user/repos")

        client.close()
        return [self._repo_to_create_repo(repo) for repo in repository_list]

    def _repo_to_create_repo(self, repo_dict) -> RepositoryCreate:

        return RepositoryCreate(
            clone_url=repo_dict["clone_url"],
            protocol=GitProtocol.https if repo_dict["clone_url"].startswith("https") else GitProtocol.ssh,
            name=repo_dict["name"],
            namespace=repo_dict["owner"]["login"],
            private=repo_dict.get("private", False),
            integration_type="github",
            integration_name=self.name,
            extra=repo_dict,
        )

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        api_base_url = self.oauth_register()["api_base_url"]
        response = client.get(f"{api_base_url}search/repositories?q={query}")

        if response.status_code == 200:
            repository_list = response.json().get("items", [])
            return [self._repo_to_create_repo(repo) for repo in repository_list]
        else:
            log_api_error(response)
            return []

    def _write_pr_commits(
        self,
        commits_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
        output: OutputHandler,
    ) -> List[PullRequestCommit]:
        ret = []
        for commit_raw in commits_raw:
            # print(commit_raw)
            # print("-----")
            commit = PullRequestCommit(
                repo_id=repository.id,
                pr_number=raw_data["pr"]["number"],
                commit_id=commit_raw["sha"],
                # author data
                author_name=commit_raw["commit"]["author"]["name"],
                author_email=commit_raw["commit"]["author"].get("email", ""),
                author_date=commit_raw["commit"]["author"]["date"],
                author_login=commit_raw.get("author", {}).get("login", "") if commit_raw.get("author") else None,
                # committer data
                committer_name=commit_raw["commit"]["committer"]["name"],
                committer_email=commit_raw["commit"]["committer"].get("email", ""),
                committer_date=commit_raw["commit"]["committer"]["date"],
                committer_login=commit_raw.get("committer", {}).get("login", "")
                if commit_raw.get("commiter")
                else None,
                # general dates
                created_at=commit_raw["commit"]["author"]["date"],
                updated_at=commit_raw["commit"]["committer"]["date"],
            )
            output.write(ExtractedKind.PULL_REQUEST_COMMIT, commit)
            ret.append(commit)
        return ret

    def _write_pr_comments(
        self,
        notes_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
        output: OutputHandler,
    ) -> List[PullRequestComment]:
        ret = []
        for note_raw in notes_raw:
            # print(note_raw)
            comment = PullRequestComment(
                repo_id=repository.id,
                pr_number=raw_data["pr"]["number"],
                comment_type="review_comments",
                comment_id=str(note_raw["id"]),
                author_id_external=note_raw["user"]["id"] if note_raw.get("user") else None,
                author_name_external=None,
                author_username_external=note_raw["user"]["login"] if note_raw.get("user") else None,
                author_aid=None,
                content=note_raw["body"],
                extra=note_raw,
                created_at=note_raw["created_at"],
                upated_at=note_raw["updated_at"],
            )
            output.write(ExtractedKind.PULL_REQUEST_COMMENT, comment)
            ret.append(comment)
        return ret
