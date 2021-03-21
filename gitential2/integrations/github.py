from typing import Callable, Optional, List
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger
from gitential2.datatypes import UserInfoCreate, RepositoryInDB, RepositoryCreate, GitProtocol
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.pull_requests import PullRequest, PullRequestState
from gitential2.extraction.output import OutputHandler
from .base import OAuthLoginMixin, BaseIntegration, GitProviderMixin
from .common import log_api_error, walk_next_link

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

        logger.info("user info data:", data)
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
        return {
            "api_base_url": "https://api.github.com/",
            "access_token_url": "https://github.com/login/oauth/access_token",
            "authorize_url": "https://github.com/login/oauth/authorize",
            "client_kwargs": {"scope": "user:email repo"},
            "userinfo_endpoint": "https://api.github.com/user",
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        prs_we_already_have: Optional[dict] = None,
    ):
        api_base_url = self.oauth_register()["api_base_url"]

        pr_list_url = f"{api_base_url}repos/{repository.namespace}/{repository.name}/pulls?per_page=100&state=all"
        client = self.get_oauth2_client(token=token, update_token=update_token)
        results = walk_next_link(client, pr_list_url)

        for pr in results:
            pr_number = pr["number"]

            if (
                prs_we_already_have
                and pr_number in prs_we_already_have
                and parse_datetime(prs_we_already_have[pr_number]) == parse_datetime(pr["updated_at"])
            ):
                logger.debug("Skipping PR, not updated.", pr_number=pr_number, repository=repository.name)
                continue

            raw_data = self._collect_single_pr_data_raw(client, pr)
            try:
                pull_request = self._transform_to_pr(raw_data, repository=repository)
                output.write(ExtractedKind.PULL_REQUEST, pull_request)
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to extract PR", pr_number=pr_number, raw_data=raw_data)

    def _collect_single_pr_data_raw(self, client, pr):
        resp = client.get(pr["url"])
        resp.raise_for_status()
        pr_details = resp.json()
        commits = walk_next_link(client, pr["_links"]["commits"]["href"])
        review_comments = walk_next_link(client, pr["_links"]["review_comments"]["href"])
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
        )

    def list_available_private_repositories(self, token, update_token) -> List[RepositoryCreate]:
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
        self,
        query: str,
        token,
        update_token,
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
