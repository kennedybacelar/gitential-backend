from typing import Optional, Callable, List
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger
from gitential2.datatypes import UserInfoCreate, RepositoryCreate, GitProtocol, RepositoryInDB
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestComment,
    PullRequestCommit,
    PullRequestData,
    PullRequestState,
)
from gitential2.extraction.output import OutputHandler
from .base import BaseIntegration, CollectPRsResult, OAuthLoginMixin, GitProviderMixin
from .common import log_api_error, walk_next_link
from ..utils.is_bugfix import calculate_is_bugfix

logger = get_logger(__name__)


class GitlabIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    def __init__(self, name, settings):
        super().__init__(name, settings)
        self.base_url = self.settings.base_url or "https://gitlab.com"
        self.api_base_url = "{}/api/v4".format(self.base_url)
        self.authorize_url = "{}/oauth/authorize".format(self.base_url)
        self.token_url = "{}/oauth/token".format(self.base_url)

    def oauth_register(self):

        return {
            "api_base_url": self.api_base_url,
            "access_token_url": self.token_url,
            "authorize_url": self.authorize_url,
            "userinfo_endpoint": self.api_base_url + "/user",
            "client_kwargs": {"scope": "api read_repository email read_user profile"},
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def refresh_token_if_expired(self, token, update_token: Callable) -> bool:
        return False

    def refresh_token(self, token):
        client = self.get_oauth2_client(token=token)
        new_token = client.refresh_token(self.token_url, refresh_token=token["refresh_token"])
        client.close()
        return new_token

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        return UserInfoCreate(
            integration_name=self.name,
            integration_type="gitlab",
            sub=str(data["id"]),
            name=data["name"],
            email=data.get("email"),
            preferred_username=data["username"],
            profile=data["web_url"],
            picture=data["avatar_url"],
            website=data.get("website_url"),
            extra=data,
        )

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:

        client = self.get_oauth2_client(token=token, update_token=update_token)
        projects = walk_next_link(client, f"{self.api_base_url}/projects?membership=1&pagination=keyset&order_by=id")
        client.close()
        return [self._project_to_repo_create(p) for p in projects]

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        response = client.get(f"{self.api_base_url}/search?scope=projects&search={query}")
        client.close()

        if response.status_code == 200:
            projects = response.json()
            return [self._project_to_repo_create(p) for p in projects]
        else:
            log_api_error(response)
            return []

    def _project_to_repo_create(self, project):
        return RepositoryCreate(
            clone_url=project["http_url_to_repo"],
            protocol=GitProtocol.https,
            name=project["path"],
            namespace=project["namespace"]["full_path"],
            private=project.get("visibility") == "private",
            integration_type="gitlab",
            integration_name=self.name,
            extra=project,
        )

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        prs_we_already_have: Optional[dict] = None,
        limit: int = 200,
    ):
        client = self.get_oauth2_client(token=token, update_token=update_token)
        ret = CollectPRsResult(prs_collected=[], prs_left=[], prs_failed=[])

        def _is_mr_up_to_date(mr: dict) -> bool:
            return (
                prs_we_already_have is not None
                and mr["iid"] in prs_we_already_have
                and parse_datetime(prs_we_already_have[mr["iid"]]) == parse_datetime(mr["updated_at"])
            )

        if repository.extra and "id" in repository.extra:
            project_id = repository.extra["id"]
            merge_requests = walk_next_link(
                client, f"{self.api_base_url}/projects/{project_id}/merge_requests?state=all&per_page=100&view=simple"
            )
            mrs_to_get = [mr for mr in merge_requests if not _is_mr_up_to_date(mr)]

            counter = 0
            for mr in mrs_to_get:
                iid = mr["iid"]
                if counter >= limit:
                    ret.prs_left.append(iid)
                else:
                    pr_data = self.collect_pull_request(repository, token, update_token, output, iid)
                    if pr_data:
                        ret.prs_collected.append(iid)
                    else:
                        ret.prs_failed.append(iid)
                counter += 1
        return ret

    def collect_pull_request(
        self, repository: RepositoryInDB, token: dict, update_token: Callable, output: OutputHandler, pr_number: int
    ) -> Optional[PullRequestData]:
        if repository.extra and "id" in repository.extra:
            client = self.get_oauth2_client(token=token, update_token=update_token)
            project_id = repository.extra["id"]
            raw_data = self._collect_single_pr_data_raw(client, project_id, pr_number)
            try:
                commits = self._write_pr_commits(raw_data["mr_commits"], raw_data, repository, output)
                comments = self._write_pr_comments(raw_data["mr_notes"], raw_data, repository, output)
                pull_request = self._transform_to_pr(raw_data, repository=repository)
                output.write(ExtractedKind.PULL_REQUEST, pull_request)
                return PullRequestData(pr=pull_request, comments=comments, commits=commits, labels=[])
            except Exception:  # pylint: disable=broad-except
                logger.exception("Failed to extract pull requests", repository=repository, raw_data_mr=raw_data["mr"])
                return None
        else:
            logger.warning(
                "GitLab repository without project_id", repository_id=repository.id, repository_name=repository.name
            )
            return None

    def _collect_single_pr_data_raw(self, client, project_id, iid):
        merge_request = client.get(f"{self.api_base_url}/projects/{project_id}/merge_requests/{iid}").json()
        merge_request_changes = client.get(
            f"{self.api_base_url}/projects/{project_id}/merge_requests/{iid}/changes?access_raw_diffs=yes"
        ).json()
        merge_request_commits = walk_next_link(
            client,
            f"{self.api_base_url}/projects/{project_id}/merge_requests/{iid}/commits",
        )
        merge_request_notes = walk_next_link(
            client,
            f"{self.api_base_url}/projects/{project_id}/merge_requests/{iid}/notes",
        )
        return {
            "project_id": project_id,
            "iid": iid,
            "mr": merge_request,
            "mr_changes": merge_request_changes,
            "mr_commits": merge_request_commits,
            "mr_notes": merge_request_notes,
        }

    def _transform_to_pr(self, raw_data: dict, repository: RepositoryInDB) -> PullRequest:
        def _calc_first_reaction_at(raw_notes):
            human_note_creation_times = [note["created_at"] for note in raw_notes if not note["system"]]
            human_note_creation_times.sort()
            return human_note_creation_times[0] if human_note_creation_times else None

        def _calc_first_commit_authored_at(raw_commits):
            author_times = [c["created_at"] for c in raw_commits]
            author_times.sort()
            return author_times[0] if author_times else None

        def _calc_addition_and_deletion_changed_files(raw_changes):
            additions, deletions, changed_files = 0, 0, 0
            for change in raw_changes["changes"]:
                changed_files += 1
                for line in change["diff"].split("\n"):
                    if line.startswith(("---", "@@")):
                        continue
                    if line.startswith("+"):
                        additions += 1
                    elif line.startswith("-"):
                        deletions += 1

            return additions, deletions, changed_files

        additions, deletions, changed_files = _calc_addition_and_deletion_changed_files(raw_data["mr_changes"])

        return PullRequest(
            repo_id=repository.id,
            number=raw_data["iid"],
            title=raw_data["mr"].get("title", "<missing title>"),
            platform="gitlab",
            id_platform=raw_data["mr"]["id"],
            api_resource_uri=f"{self.api_base_url}/projects/{raw_data['project_id']}/merge_requests/{raw_data['iid']}",
            state_platform=raw_data["mr"]["state"],
            state=PullRequestState.from_gitlab(raw_data["mr"]["state"]),
            created_at=raw_data["mr"]["created_at"],
            closed_at=raw_data["mr"]["created_at"],
            updated_at=raw_data["mr"]["updated_at"],
            merged_at=raw_data["mr"]["merged_at"],
            additions=additions,
            deletions=deletions,
            changed_files=changed_files,
            draft=raw_data["mr"]["work_in_progress"],
            user=raw_data["mr"]["author"]["username"],
            user_id_external=str(raw_data["mr"]["author"]["id"]),
            user_name_external=raw_data["mr"]["author"]["name"],
            user_username_external=raw_data["mr"]["author"]["username"],
            user_aid=None,
            commits=len(raw_data["mr_commits"]),
            merged_by=raw_data["mr"]["merged_by"]["username"] if raw_data["mr"]["merged_by"] else None,
            first_reaction_at=_calc_first_reaction_at(raw_data["mr_notes"]) or raw_data["mr"]["merged_at"],
            first_commit_authored_at=_calc_first_commit_authored_at(raw_data["mr_commits"]),
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["mr"].get("title", "<missing title>")),
        )

    def _write_pr_commits(
        self,
        commits_raw: list,
        raw_data: dict,
        repository: RepositoryInDB,
        output: OutputHandler,
    ) -> List[PullRequestCommit]:
        ret = []
        for commit_raw in commits_raw:
            commit = PullRequestCommit(
                repo_id=repository.id,
                pr_number=raw_data["iid"],
                commit_id=commit_raw["id"],
                author_name=commit_raw["author_name"],
                author_email=commit_raw.get("author_email", ""),
                author_date=commit_raw["authored_date"],
                author_login=None,
                committer_name=commit_raw["committer_name"],
                committer_email=commit_raw.get("committer_email", ""),
                committer_date=commit_raw["committed_date"],
                committer_login=None,
                created_at=commit_raw["created_at"],
                updated_at=commit_raw["created_at"],
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
                pr_number=raw_data["iid"],
                comment_type=str(note_raw["type"]),
                comment_id=str(note_raw["id"]),
                author_id_external=note_raw["author"]["id"],
                author_name_external=note_raw["author"]["name"],
                author_username_external=note_raw["author"]["username"],
                author_aid=None,
                content=note_raw["body"],
                extra=note_raw,
            )
            output.write(ExtractedKind.PULL_REQUEST_COMMENT, comment)
            ret.append(comment)
        return ret
