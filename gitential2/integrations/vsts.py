from typing import Optional, Callable, List, Tuple
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs, urlparse
from structlog import get_logger
from authlib.integrations.requests_client import OAuth2Session
from pydantic.datetime_parse import parse_datetime
from email_validator import validate_email, EmailNotValidError

from gitential2.datatypes import UserInfoCreate, RepositoryCreate, RepositoryInDB, GitProtocol
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.datatypes.its import (
    ITSIssueHeader,
    ITSIssueAllData,
    ITSIssue,
    ITSIssueComment,
    ITSIssueChange,
    ITSIssueTimeInStatus,
    ITSIssueStatusCategory,
)

from gitential2.datatypes.pull_requests import PullRequest, PullRequestComment, PullRequestCommit, PullRequestState

from ..utils.is_bugfix import calculate_is_bugfix
from .base import BaseIntegration, OAuthLoginMixin, GitProviderMixin, PullRequestData, ITSProviderMixin

from .common import log_api_error


logger = get_logger(__name__)


class VSTSIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration, ITSProviderMixin):
    base_url = "https://app.vssps.visualstudio.com"

    def get_client(self, token, update_token) -> OAuth2Session:
        return self.get_oauth2_client(token=token, update_token=update_token)

    def _auth_client_secret_uri(self, client, method, uri, headers, body):
        logger.debug(
            "vsts._auth_client_secret_uri inputs", client=client, method=method, uri=uri, headers=headers, body=body
        )
        body_original = parse_qs(body, encoding="utf8")

        body_ = {
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self.settings.oauth.client_secret,
            "redirect_uri": self.settings.options.get("redirect_url"),
        }

        if b"code" in body_original:
            body_["grant_type"] = "urn:ietf:params:oauth:grant-type:jwt-bearer"
            body_["assertion"] = body_original[b"code"][0].decode()

        elif "refresh_token" in body_original:
            body_["grant_type"] = "refresh_token"
            body_["assertion"] = body_original["refresh_token"][0]

        body_str = "&".join([f"{k}={v}" for (k, v) in body_.items()])

        headers["content-length"] = str(len(body_str))
        logger.debug("vsts._auth_client_secret_uri outputs", uri=uri, headers=headers, body_str=body_str)
        return uri, headers, body_str

    def oauth_register(self):
        return {
            "api_base_url": self.base_url,
            "access_token_url": f"{self.base_url}/oauth2/token",
            "authorize_url": f"{self.base_url}/oauth2/authorize",
            "userinfo_endpoint": f"{self.base_url}/_apis/profile/profiles/me?api-version=4.1",
            "client_kwargs": {
                "scope": "vso.code vso.project vso.work",
                "response_type": "Assertion",
                "token_endpoint_auth_method": self._auth_client_secret_uri,
            },
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        return UserInfoCreate(
            integration_type="vsts",
            integration_name=self.name,
            sub=data["id"],
            preferred_username=data["displayName"],
            email=data["emailAddress"],
            extra=data,
        )

    def _collect_raw_pull_requests(self, repository: RepositoryInDB, client) -> list:
        organization, project, repo = _get_project_organization_and_repository(repository)
        pull_requests = _paginate_with_skip_top(
            client,
            f"https://dev.azure.com/{organization}/{project}/_apis/git/pullrequests?api-version=6.0&searchCriteria.repositoryId={repo}&searchCriteria.status=all",
        )
        return pull_requests

    def _raw_pr_number_and_updated_at(self, raw_pr: dict) -> Tuple[int, datetime]:
        return (
            raw_pr["pullRequestId"],
            parse_datetime(raw_pr["closedDate"])
            if "closedDate" in raw_pr
            else datetime.utcnow().replace(tzinfo=timezone.utc),
        )

    def _collect_raw_pull_request(self, repository: RepositoryInDB, pr_number: int, client) -> dict:
        def _get_json_response(url):
            resp = client.get(url)
            resp.raise_for_status()
            return resp.json()

        organization, project, repo = _get_project_organization_and_repository(repository)

        pr_details = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/pullrequests/{pr_number}?api-version=6.0"
        )
        iterations = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo}/pullRequests/{pr_number}/iterations?api-version=6.0"
        )
        threads = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo}/pullRequests/{pr_number}/threads?api-version=6.0"
        )

        commits = _get_json_response(
            f"https://dev.azure.com/{organization}/{project}/_apis/git/repositories/{repo}/pullRequests/{pr_number}/commits?api-version=6.0"
        )

        return {"pr": pr_details, "threads": threads, "commits": commits, "iterations": iterations}

    def _tranform_to_pr_data(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequestData:
        return PullRequestData(
            pr=self._tranform_to_pr(repository, pr_number, raw_data, author_callback),
            comments=self._tranform_to_comments(repository, pr_number, raw_data, author_callback),
            commits=self._transform_to_commits(repository, pr_number, raw_data),
            labels=[],
        )

    def _tranform_to_pr(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> PullRequest:

        all_commit_dates = [c["author"]["date"] for c in raw_data.get("commits", {}).get("value", [])]
        all_commit_dates.sort()
        first_commit_authored_at = all_commit_dates[0] if all_commit_dates else None

        all_interactions_dates = []

        for thread in raw_data.get("threads", {}).get("value", []):
            for comment in thread.get("comments", []):
                if "publishedDate" in comment and comment["publishedDate"]:
                    all_interactions_dates.append(comment["publishedDate"])

        if raw_data["pr"].get("closedDate"):
            all_interactions_dates.append(raw_data["pr"].get("closedDate"))

        all_interactions_dates.sort()
        first_interaction_at = all_interactions_dates[0] if all_interactions_dates else None

        return PullRequest(
            repo_id=repository.id,
            number=pr_number,
            title=raw_data["pr"].get("title", "<missing title>"),
            platform="vsts",
            id_platform=raw_data["pr"]["pullRequestId"],
            api_resource_uri=raw_data["pr"]["url"],
            state_platform=raw_data["pr"]["status"],
            state=PullRequestState.from_vsts(raw_data["pr"]["status"]),
            created_at=raw_data["pr"]["creationDate"],
            closed_at=raw_data["pr"].get("closedDate"),
            updated_at=raw_data["pr"].get("closedDate") or None,
            merged_at=raw_data["pr"].get("closedDate") if raw_data["pr"]["status"] == "completed" else None,
            additions=0,
            deletions=0,
            changed_files=0,
            draft=raw_data["pr"]["isDraft"],
            user=raw_data["pr"]["createdBy"]["uniqueName"],
            user_name_external=raw_data["pr"]["createdBy"]["displayName"],
            user_username_external=raw_data["pr"]["createdBy"]["uniqueName"],
            user_aid=author_callback(to_author_alias(raw_data["pr"]["createdBy"])),
            commits=len(raw_data["commits"].get("value", [])),
            merged_by=raw_data["pr"]["closedBy"]["uniqueName"]
            if "closedBy" in raw_data["pr"]
            and PullRequestState.from_vsts(raw_data["pr"]["status"]) == PullRequestState.merged
            else None,
            merged_by_aid=author_callback(to_author_alias(raw_data["pr"]["closedBy"]))
            if "closedBy" in raw_data["pr"]
            and PullRequestState.from_vsts(raw_data["pr"]["status"]) == PullRequestState.merged
            else None,
            first_reaction_at=first_interaction_at,
            first_commit_authored_at=first_commit_authored_at,
            extra=raw_data,
            is_bugfix=calculate_is_bugfix([], raw_data["pr"].get("title", "<missing title>")),
        )

    def _tranform_to_comments(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict, author_callback: Callable
    ) -> List[PullRequestComment]:
        ret = []
        for thread in raw_data.get("threads", {}).get("value", []):
            for comment in thread.get("comments", []):
                pr_comment = PullRequestComment(
                    repo_id=repository.id,
                    pr_number=pr_number,
                    comment_type=str(comment["commentType"]),
                    comment_id="-".join([str(thread["id"]), str(comment["id"])]),
                    thread_id=str(thread["id"]),
                    parent_comment_id=str(comment["parentCommentId"]),
                    author_id_external=comment["author"]["id"],
                    author_name_external=comment["author"]["displayName"],
                    author_username_external=comment["author"]["uniqueName"],
                    author_aid=author_callback(to_author_alias(comment["author"])),
                    content=comment.get("content", ""),
                    extra=comment,
                    created_at=comment["publishedDate"],
                    updated_at=comment["lastUpdatedDate"],
                    published_at=comment["publishedDate"],
                )
                ret.append(pr_comment)

        return ret

    def _transform_to_commits(
        self, repository: RepositoryInDB, pr_number: int, raw_data: dict
    ) -> List[PullRequestCommit]:
        ret = []
        for commit_raw in raw_data.get("commits", {}).get("value", []):
            commit = PullRequestCommit(
                repo_id=repository.id,
                pr_number=pr_number,
                commit_id=commit_raw["commitId"],
                author_name=commit_raw.get("author", {}).get("name", None),
                author_email=commit_raw.get("author", {}).get("email", None),
                author_date=commit_raw.get("author", {}).get("date", None),
                author_login=None,
                committer_name=commit_raw.get("committer", {}).get("name", None),
                committer_email=commit_raw.get("committer", {}).get("email", None),
                committer_date=commit_raw.get("committer", {}).get("date", None),
                committer_login=None,
                created_at=commit_raw.get("committer", {}).get("date", None),
                updated_at=commit_raw.get("committer", {}).get("date", None),
                extra=commit_raw,
            )
            ret.append(commit)
        return ret

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        new_token = self.refresh_token(token)
        update_token(new_token)
        return True, new_token

    def refresh_token(self, token):
        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        token = client.refresh_token(self.oauth_register()["access_token_url"], refresh_token=token["refresh_token"])
        client.close()
        return {f: token[f] for f in ["access_token", "refresh_token", "expires_at"]}

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:

        if not provider_user_id:
            logger.warn("Cannot list vsts repositories, provider_user_id is missing", token=token)
            return []
        # token = self.refresh_token(token)

        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        api_base_url = self.oauth_register()["api_base_url"]

        accounts_resp = client.get(f"{api_base_url}/_apis/accounts?memberId={provider_user_id}&api-version=6.0")
        if accounts_resp.status_code != 200:
            log_api_error(accounts_resp)
            return []

        accounts = accounts_resp.json().get("value", [])
        repos = []
        for account in accounts:
            account_repo_url = f"https://{account['accountName']}.visualstudio.com/DefaultCollection/_apis/git/repositories?api-version=1.0"
            repo_resp = client.get(account_repo_url)

            if repo_resp.status_code != 200:
                log_api_error(repo_resp)
                continue

            response_json = repo_resp.json()
            if "value" in response_json:
                repos += [self._repo_to_create_repo(repo, account) for repo in response_json["value"]]
        return repos

    def _repo_to_create_repo(self, repo_dict, account_dict):
        return RepositoryCreate(
            clone_url=repo_dict["webUrl"],
            protocol=GitProtocol.https,
            name=repo_dict["name"],
            namespace=account_dict["accountName"],
            private=repo_dict["project"]["visibility"] == "private",
            integration_type="vsts",
            integration_name=self.name,
            extra=repo_dict,
        )

    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        return []

    def _get_all_accounts(self, client, provider_user_id: Optional[str]) -> List[dict]:

        api_base_url = self.oauth_register()["api_base_url"]
        accounts_resp = client.get(f"{api_base_url}/_apis/accounts?memberId={provider_user_id}&api-version=6.0")

        if accounts_resp.status_code != 200:
            log_api_error(accounts_resp)
            return []

        accounts = accounts_resp.json().get("value", [])
        return accounts

    def _get_all_teams(self, client, organization: str) -> List[dict]:

        all_teams_per_organization_url = f"https://dev.azure.com/{organization}/_apis/teams?api-version=4.1-preview.2"
        # Organization>Settings>Security>Policies>Third-party application access vai OAuth

        teams_resp = client.get(all_teams_per_organization_url)

        if teams_resp.status_code != 200:
            log_api_error(teams_resp)
            return []

        teams_resp_json = teams_resp.json()["value"]
        return teams_resp_json

    def list_available_its_projects(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[ITSProjectCreate]:

        if not provider_user_id:
            logger.warn("Cannot list vsts repositories, provider_user_id is missing", token=token)
            return []

        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )

        accounts = self._get_all_accounts(client, provider_user_id)
        ret = []

        for account in accounts:
            organization = account["accountName"]
            teams_resp_json = self._get_all_teams(client, organization)

            for team in teams_resp_json:
                team["organization"] = organization
                ret.append(self._transform_to_its_project(team))
        return ret

    def _transform_to_its_project(self, project_dict: dict) -> ITSProjectCreate:
        # print(project_dict)
        return ITSProjectCreate(
            name=project_dict["name"],
            namespace=f"{project_dict['organization']}/{project_dict['projectName']}",
            private=True,
            api_url=project_dict["identityUrl"],
            key=project_dict["id"],
            integration_type="vsts",
            integration_name=self.name,
            integration_id=project_dict["id"],
            extra=None,
        )

    def _raw_fetching_all_issues_per_project(
        self, token, its_project: ITSProjectInDB, fields: List[str] = None, date_from: Optional[datetime] = None
    ) -> List[dict]:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)

        organization, project = _get_organization_and_project_from_its_project(its_project.namespace)
        team = its_project.name

        fields = fields or [
            "System.Id, System.WorkItemType, System.Description, System.AssignedTo, System.State, System.AreaPath,System.Tags, System.CommentCount, System.ChangedDate"
        ]
        if_date_from = (
            f" AND System.ChangedDate > '{date_from.year}-{date_from.month}-{date_from.day}'" if date_from else ""
        )

        body_work_items_by_teams = {
            "query": f"SELECT {','.join(fields)} FROM workitems WHERE [System.TeamProject] = '{project}'{if_date_from} ORDER BY [System.ChangedDate] DESC"
        }

        work_items_url = (
            f"https://dev.azure.com/{organization}/{project}/{team}/_apis/wit/wiql?api-version=6.0-preview.2"
        )

        wit_by_teams_response = client.post(work_items_url, json=body_work_items_by_teams)
        if wit_by_teams_response.status_code != 200:
            log_api_error(wit_by_teams_response)
            return []

        all_work_items_per_its_project = wit_by_teams_response.json().get("workItems", [])

        if all_work_items_per_its_project:
            full_list_of_work_items_ids = []
            for single_work_item in all_work_items_per_its_project:
                full_list_of_work_items_ids.append(single_work_item["id"])

            max_number_work_items_per_batch_request = 200
            ret = []
            for work_item_ids in range(0, len(full_list_of_work_items_ids), max_number_work_items_per_batch_request):

                sliced_list_of_work_items_ids = full_list_of_work_items_ids[
                    work_item_ids : work_item_ids + max_number_work_items_per_batch_request
                ]

                body_query_work_items_details_batch = {"ids": sliced_list_of_work_items_ids}

                get_work_items_details_batch_url = (
                    f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemsbatch?api-version=6.0"
                )

                wit_by_details_batch_response = client.post(
                    get_work_items_details_batch_url, json=body_query_work_items_details_batch
                )
                if wit_by_details_batch_response.status_code != 200:
                    log_api_error(wit_by_details_batch_response)
                    return []

                ret.extend(wit_by_details_batch_response.json()["value"])
            return ret
        return []

    def _get_single_work_item_all_data(self, token, its_project: ITSProjectInDB, issue_id_or_key: str) -> dict:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        organization, project = _get_organization_and_project_from_its_project(its_project.namespace)

        single_work_item_details_url = (
            f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitems/{issue_id_or_key}?api-version=6.0"
        )

        single_work_item_details_response = client.get(single_work_item_details_url)

        if single_work_item_details_response.status_code != 200:
            log_api_error(single_work_item_details_response)
            return {}

        single_work_item_details_response_json = single_work_item_details_response.json()
        return single_work_item_details_response_json

    def _get_issue_comments(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> List[ITSIssueComment]:

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        organization, project = _get_organization_and_project_from_its_project(its_project.namespace)

        issue_comments_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workItems/{issue_id_or_key}/comments?api-version=6.0-preview.3"

        issue_comments_response = client.get(issue_comments_url)

        if issue_comments_response.status_code != 200:
            log_api_error(issue_comments_response)
            return []

        ret = []
        list_issue_comments_response = issue_comments_response.json().get("comments", [])
        for single_comment in list_issue_comments_response:
            ret.append(
                _transform_to_its_ITSIssueComment(
                    comment_dict=single_comment, its_project=its_project, developer_map_callback=developer_map_callback
                )
            )
        return ret

    def _mapping_status_id(
        self, token, its_project: ITSProjectInDB, work_item_type: str = None, wit_system_state: str = None
    ) -> Optional[str]:

        if not work_item_type:
            return None

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)
        organization, project = _get_organization_and_project_from_its_project(its_project.namespace)

        single_work_item_details_url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemtypes/{work_item_type}/states?api-version=6.0-preview.1"

        status_original_id_response = client.get(single_work_item_details_url)

        if status_original_id_response.status_code != 200:
            log_api_error(status_original_id_response)
            return None

        list_of_status_original_id_response = status_original_id_response.json().get("value")
        if list_of_status_original_id_response:
            for single_status in list_of_status_original_id_response:
                if wit_system_state == single_status["name"]:
                    return single_status["category"]

    def _work_item_type_id(self, token, its_project: ITSProjectInDB, work_item_type: str = None) -> Optional[str]:

        if not work_item_type:
            return None

        client = self.get_oauth2_client(token=token, token_endpoint_auth_method=self._auth_client_secret_uri)

        organization, project = _get_organization_and_project_from_its_project(its_project.namespace)
        single_work_item_type_url = (
            f"https://dev.azure.com/{organization}/{project}/_apis/wit/workitemtypes/{work_item_type}?api-version=6.0"
        )

        single_work_item_type_response = client.get(single_work_item_type_url)

        if single_work_item_type_response.status_code != 200:
            log_api_error(single_work_item_type_response)
            return None

        resp = single_work_item_type_response.json().get("referenceName")
        return resp

    def _transform_to_its_issue(
        self,
        token,
        issue_dict: dict,
        its_project: ITSProjectInDB,
        developer_map_callback: Callable,
        comment: Optional[ITSIssueComment],
    ) -> ITSIssue:

        status_category_api_mapped = self._mapping_status_id(
            token=token,
            its_project=its_project,
            work_item_type=issue_dict["fields"].get("System.WorkItemType"),
            wit_system_state=issue_dict["fields"].get("System.State"),
        )

        return ITSIssue(
            id=get_db_issue_id(issue_dict, its_project),
            itsp_id=its_project.id,
            api_url=issue_dict["url"],
            api_id=issue_dict["id"],
            key=issue_dict["id"],
            status_name=issue_dict["fields"].get("System.State"),
            status_id=status_category_api_mapped,
            status_category_api=status_category_api_mapped,
            status_category=_parse_status_category(status_category_api_mapped),
            issue_type_name=issue_dict["fields"].get("System.WorkItemType"),
            issue_type_id=self._work_item_type_id(
                token=token, its_project=its_project, work_item_type=issue_dict["fields"].get("System.WorkItemType")
            ),
            resolution_name=issue_dict["fields"]["System.Reason"]
            if issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate")
            else None,
            resolution_id=None,
            resolution_date=issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate"),
            priority_name=issue_dict["fields"].get("Microsoft.VSTS.Common.Priority"),
            priority_id=None,
            priority_order=None,
            summary=issue_dict["fields"].get("System.Title", ""),
            description=issue_dict["fields"].get("System.Description", ""),
            creator_api_id=None,
            creator_email=issue_dict["fields"]["System.CreatedBy"].get("uniqueName"),
            creator_name=issue_dict["fields"]["System.CreatedBy"].get("displayName"),
            creator_dev_id=developer_map_callback(to_author_alias(issue_dict["fields"].get("System.CreatedBy")))
            if issue_dict["fields"].get("System.CreatedBy")
            else None,
            reporter_api_id=None,
            reporter_email=issue_dict["fields"]["System.CreatedBy"].get("uniqueName"),
            reporter_name=issue_dict["fields"]["System.CreatedBy"].get("displayName"),
            reporter_dev_id=developer_map_callback(to_author_alias(issue_dict["fields"].get("System.CreatedBy")))
            if issue_dict["fields"].get("System.CreatedBy")
            else None,
            assignee_api_id=issue_dict["fields"]["System.AssignedTo"].get("id")
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            assignee_email=issue_dict["fields"]["System.AssignedTo"].get("uniqueName")
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            assignee_name=issue_dict["fields"]["System.AssignedTo"].get("displayName")
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            assignee_dev_id=developer_map_callback(to_author_alias(issue_dict["fields"].get("System.AssignedTo")))
            if issue_dict["fields"].get("System.AssignedTo")
            else None,
            labels=_parse_labels(issue_dict["fields"].get("System.Tags")),
            is_started=bool(issue_dict["fields"].get("Microsoft.VSTS.Common.ActivatedDate")),
            started_at=parse_datetime(issue_dict["fields"]["Microsoft.VSTS.Common.ActivatedDate"])
            if issue_dict["fields"].get("ActivatedDate")
            else None,
            is_closed=bool(issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate")),
            closed_at=parse_datetime(issue_dict["fields"]["Microsoft.VSTS.Common.ClosedDate"])
            if issue_dict["fields"].get("Microsoft.VSTS.Common.ClosedDate")
            else None,
            comment_count=issue_dict["fields"].get("System.CommentCount"),
            last_comment_at=parse_datetime(comment.created_at),
            change_count=issue_dict.get("rev"),
            last_change_at=parse_datetime(issue_dict["fields"].get("System.ChangedDate")),
            story_points=None,
            created_at=parse_datetime(issue_dict["fields"].get("System.CreatedDate")),
            updated_at=parse_datetime(issue_dict["fields"].get("System.ChangedDate")),
        )

    def list_recently_updated_issues(
        self, token, its_project: ITSProjectInDB, date_from: Optional[datetime] = None
    ) -> List[ITSIssueHeader]:

        number_of_days_since_last_change_to_be_considered_recent = 7

        date_from = date_from or (
            datetime.today() - timedelta(number_of_days_since_last_change_to_be_considered_recent)
        )
        ret: List[ITSIssueHeader] = []

        wit_by_details_batch_response_json = self._raw_fetching_all_issues_per_project(
            token=token, its_project=its_project, date_from=date_from
        )

        for single_issue in wit_by_details_batch_response_json:
            ret.append(
                _transform_to_its_issues_header(
                    issue_dict=single_issue,
                    its_project=its_project,
                )
            )
        return ret

    def list_all_issues_for_project(self, token, its_project: ITSProjectInDB) -> List[ITSIssueHeader]:

        wit_by_details_batch_response_json = self._raw_fetching_all_issues_per_project(
            token=token, its_project=its_project
        )
        ret = []

        for single_issue in wit_by_details_batch_response_json:
            ret.append(
                _transform_to_its_issues_header(
                    issue_dict=single_issue,
                    its_project=its_project,
                )
            )
        return ret

    def get_all_data_for_issue(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> ITSIssueAllData:

        # raw data of single work item
        single_work_item_details_response_json = self._get_single_work_item_all_data(
            token=token, its_project=its_project, issue_id_or_key=issue_id_or_key
        )

        comments: List[ITSIssueComment] = self._get_issue_comments(
            token=token,
            its_project=its_project,
            issue_id_or_key=issue_id_or_key,
            developer_map_callback=developer_map_callback,
        )

        issue: ITSIssue = self._transform_to_its_issue(
            token=token,
            issue_dict=single_work_item_details_response_json,
            its_project=its_project,
            developer_map_callback=developer_map_callback,
            comment=comments[0] if comments else None,
        )
        # changes= To be implemented
        # times_in_statuses= To be implemented
        return _transform_to_its_ITSIssueAllData(issue=issue, comments=comments, changes=[], times_in_statuses=[])


def _get_organization_and_project_from_its_project(its_project_namespace: str) -> Tuple[str, str]:
    if len(its_project_namespace.split("/")) == 2:
        splitted = its_project_namespace.split("/")
        return (splitted[0], splitted[1])
    raise ValueError(f"Don't know how to parse vsts {its_project_namespace} namespace")


def get_db_issue_id(issue_dict: dict, its_project: ITSProjectInDB) -> str:
    return f"{its_project.id}-{issue_dict['id']}"


def _transform_to_its_issues_header(issue_dict: dict, its_project: ITSProjectInDB) -> ITSIssueHeader:
    return ITSIssueHeader(
        id=get_db_issue_id(issue_dict, its_project),
        itsp_id=its_project.id,
        api_url=issue_dict["url"],
        api_id=issue_dict["id"],
        key=issue_dict["id"],
        status_name=issue_dict["fields"].get("System.State"),
        status_id=None,
        status_category=_parse_status_category(issue_dict["fields"].get("System.WorkItemType")),
        summary=issue_dict["fields"].get("System.Title"),
        created_at=parse_datetime(issue_dict["fields"].get("System.CreatedDate")),
        updated_at=parse_datetime(issue_dict["fields"].get("System.ChangedDate")),
    )


def _transform_to_its_ITSIssueComment(
    comment_dict: dict, its_project: ITSProjectInDB, developer_map_callback: Callable
) -> ITSIssueComment:
    return ITSIssueComment(
        id=comment_dict["id"],
        issue_id=comment_dict["workItemId"],
        itsp_id=its_project.id,
        author_api_id=comment_dict["createdBy"].get("id"),
        author_email=comment_dict["createdBy"].get("uniqueName"),
        author_name=comment_dict["createdBy"].get("displayName"),
        author_dev_id=developer_map_callback(to_author_alias(comment_dict["createdBy"])),
        comment=comment_dict.get("text"),
        created_at=parse_datetime(comment_dict["createdDate"]) if comment_dict.get("createdDate") else None,
        updated_at=parse_datetime(comment_dict["modifiedDate"]) if comment_dict.get("modifiedDate") else None,
    )


def _transform_to_its_ITSIssueAllData(
    issue: ITSIssue,
    comments: List[ITSIssueComment],
    changes: List[ITSIssueChange],
    times_in_statuses: List[ITSIssueTimeInStatus],
) -> ITSIssueAllData:
    return ITSIssueAllData(
        issue=issue,
        comments=comments,
        changes=changes,
        times_in_statuses=times_in_statuses,
    )


def _parse_labels(labels: str) -> List[str]:
    if labels:
        return [label.strip() for label in labels.split(";")]
    return []


def _parse_status_category(status_category_api: str) -> ITSIssueStatusCategory:
    assignment_state_category_api_to_its = {
        "Proposed": "new",
        "InProgress": "in_progress",
        "Resolved": "done",
        "Completed": "done",
        "Removed": "done",
    }
    if status_category_api in assignment_state_category_api_to_its:
        return ITSIssueStatusCategory(assignment_state_category_api_to_its[status_category_api])
    return ITSIssueStatusCategory("unknown")


def _get_project_organization_and_repository(repository: RepositoryInDB) -> Tuple[str, str, str]:

    if repository.extra and "project" in repository.extra:
        repository_url = repository.extra["url"]
        return _parse_azure_repository_url(repository_url)
    else:
        return _parse_clone_url(repository.clone_url)


def _parse_azure_repository_url(url: str) -> Tuple[str, str, str]:
    parsed_url = urlparse(url)

    if parsed_url.hostname and parsed_url.path:
        _splitted_path = parsed_url.path.split("/")

        if "visualstudio.com" in parsed_url.hostname and "_apis/git/repositories" in parsed_url.path:
            # "https://ORGANIZATION_NAME.visualstudio.com/PROJECT_ID/_apis/git/repositories/REPOSITORY_ID"
            organization_name = parsed_url.hostname.split(".")[0]
            project_id = _splitted_path[1]
            repository_id = _splitted_path[-1]
            return organization_name, project_id, repository_id
        elif "dev.azure.com" in parsed_url.hostname:
            organization_name = _splitted_path[1]

    raise ValueError(f"Don't know how to parse AZURE Resource URL: {url}")


# pylint: disable=unused-argument
def _parse_clone_url(url: str) -> Tuple[str, str, str]:
    return ("", "", "")


def _paginate_with_skip_top(client, starting_url, top=100) -> list:
    ret: list = []
    skip = 0

    while True:
        url = starting_url + f"&$top={top}&$skip={skip}"
        resp = client.get(url)
        if resp.status_code != 200:
            return ret
        elif resp.status_code == 200:
            json_resp = resp.json()
            count = json_resp["count"]
            value = json_resp["value"]
            ret += value
            if count >= top:
                skip = skip + top
            else:
                return ret


def to_author_alias(raw_user):
    name = raw_user.get("displayName")
    uniq_name = raw_user.get("uniqueName")
    try:
        valid = validate_email(uniq_name)
        email = valid.email
        return AuthorAlias(name=name, email=email)
    except EmailNotValidError:
        return AuthorAlias(name=name, login=uniq_name)
