from typing import Callable, Tuple, List, Optional, Dict
from pydantic import BaseModel, Field
from structlog import get_logger
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.datatypes.its import ITSIssue, ITSIssueHeader
from gitential2.datatypes.userinfos import UserInfoCreate
from gitential2.utils import add_url_params
from .base import BaseIntegration, ITSProviderMixin, OAuthLoginMixin


logger = get_logger(__name__)


class AtlassianSite(BaseModel):
    id: str
    name: str
    url: str
    scopes: List[str]
    avatar_url: str = Field(..., alias="avatarUrl")


class JiraIntegration(ITSProviderMixin, OAuthLoginMixin, BaseIntegration):
    def oauth_register(self) -> dict:
        return {
            "access_token_url": "https://auth.atlassian.com/oauth/token",
            "authorize_url": "https://auth.atlassian.com/authorize?audience=api.atlassian.com",
            "userinfo_endpoint": "https://api.atlassian.com/me",
            "client_kwargs": {
                "scope": "read:me read:jira-user read:jira-work offline_access",
            },
            "client_id": self.settings.oauth.client_id if self.settings.oauth else None,
            "client_secret": self.settings.oauth.client_secret if self.settings.oauth else None,
        }

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        return False, token

    def list_accessible_resources(self, token) -> List[AtlassianSite]:
        client = self.get_oauth2_client(token=token)
        resp = client.get("https://api.atlassian.com/oauth/token/accessible-resources")
        client.close()
        return [AtlassianSite.parse_obj(item) for item in resp.json()]

    def list_available_jira_projects(self, token) -> List[Tuple[AtlassianSite, dict]]:
        sites = self.list_accessible_resources(token)
        client = self.get_oauth2_client(token=token)
        ret = []
        for site in sites:
            if "read:jira-work" in site.scopes:
                site_id = site.id
                resp = client.get(f"https://api.atlassian.com/ex/jira/{site_id}/rest/api/2/project")
                resp_json = resp.json()
                for item in resp_json:
                    ret.append((site, item))
            else:
                logger.warning("No Jira scope given for site", site=site)
        return ret

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:

        return UserInfoCreate(
            integration_name=self.name,
            integration_type="jira",
            sub=str(data["account_id"]),
            name=data["name"],
            email=data.get("email"),
            preferred_username=data["nickname"],
            picture=data.get("picture"),
            extra=data,
        )

    def list_available_its_projects(self, token: dict) -> List[ITSProjectCreate]:
        jira_projects = self.list_available_jira_projects(token)
        ret = []
        for site, project_dict in jira_projects:
            ret.append(self._transform_to_its_project(site, project_dict))
        return ret

    def _transform_to_its_project(self, site: AtlassianSite, project_dict: dict) -> ITSProjectCreate:
        # print(project_dict)
        return ITSProjectCreate(
            name=project_dict["name"],
            namespace=site.name,
            private=project_dict["isPrivate"],
            api_url=project_dict["self"],
            key=project_dict["key"],
            integration_type="jira",
            integration_name=self.name,
            integration_id=project_dict["id"],
            extra=project_dict,
        )

    # def list_boards(self, token, project_api_url) -> list:
    #     # We're waiting for Atlassian to enable OAuth2 for these endpoints
    #     # https://jira.atlassian.com/browse/JSWCLOUD-18874

    #     #  from pprint import pprint

    #     base_url = _get_rest_api_base_url_from_project_api_url(project_api_url)
    #     # pprint(base_url)
    #     client = self.get_oauth2_client(token=token)
    #     res = client.get(base_url + "/rest/agile/latest/board").json()
    #     # pprint(res)
    #     return []

    # def list_recently_updated_issues(self, token, project_api_url, date_from: datetime) -> list:
    #     base_url = _get_rest_api_base_url_from_project_api_url(project_api_url)
    #     client = self.get_oauth2_client(token=token)
    #     query = "project = GTD AND updated >= -60d ORDER BY updated DESC"
    #     # query = "project = GTD ORDER BY updated DESC"
    #     fields = ["created", "status", "updated", "summary"]
    #     res = get_all_pages_from_paginated(
    #         client, base_url + f"/rest/api/3/search?jql={query}&fields={','.join(fields)}", values_key="issues"
    #     )
    #     from pprint import pprint

    #     pprint(res)

    def get_all_data_for_issue(
        self, token, its_project: ITSProjectInDB, issue_id_or_key: str, developer_map_callback: Callable
    ) -> ITSIssue:

        client = self.get_oauth2_client(token=token)
        base_url = _get_rest_api_base_url_from_project_api_url(its_project.api_url)
        priority_orders = self.get_site_priority_orders(token, its_project)
        # fields = self.get_site_fields(token, its_project)

        issue_api_url = base_url + f"/rest/api/3/issue/{issue_id_or_key}?fields=*all"
        resp = client.get(issue_api_url)
        resp.raise_for_status()
        issue_dict = resp.json()
        return transform_dict_to_issue(
            issue_dict, its_project, developer_map_callback=developer_map_callback, priority_orders=priority_orders
        )

    def get_site_priority_orders(self, token, its_project: ITSProjectInDB) -> Dict[str, int]:
        base_url = _get_rest_api_base_url_from_project_api_url(its_project.api_url)
        priorities = self.http_get_json_and_cache(url=base_url + "/rest/api/2/priority", token=token)
        return {prio["name"]: idx for idx, prio in enumerate(priorities, start=1)}

    def get_site_fields(self, token, its_project: ITSProjectInDB):
        base_url = _get_rest_api_base_url_from_project_api_url(its_project.api_url)
        fields = self.http_get_json_and_cache(url=base_url + "/rest/api/2/field", token=token)
        # print("++++++++++++++++")
        # from pprint import pprint

        # pprint(fields)

        return fields

    def list_all_issues_for_project(self, token, its_project: ITSProjectInDB) -> List[ITSIssueHeader]:
        client = self.get_oauth2_client(token=token)
        query_for_project = f'project = "{its_project.key}" ORDER BY created DESC'
        fields = ["created", "status", "updated", "summary"]
        base_url = _get_rest_api_base_url_from_project_api_url(its_project.api_url)

        issue_header_dicts = get_all_pages_from_paginated(
            client,
            base_url + f"/rest/api/3/search?jql={query_for_project}&fields={','.join(fields)}",
            values_key="issues",
        )
        client.close()
        return [
            transform_dict_to_issue_header(issue_header_dict, its_project) for issue_header_dict in issue_header_dicts
        ]


def transform_dict_to_issue_header(issue_header_dict: dict, its_project: ITSProjectInDB) -> ITSIssueHeader:
    return ITSIssueHeader(
        id=f"{its_project.id}-{issue_header_dict['id']}",
        created_at=issue_header_dict["fields"]["created"],
        updated_at=issue_header_dict["fields"]["updated"],
        itsp_id=its_project.id,
        api_url=issue_header_dict["self"],
        api_id=issue_header_dict["id"],
        key=issue_header_dict["key"],
        status_name=issue_header_dict["fields"]["status"]["name"],
        status_id=issue_header_dict["fields"]["status"]["id"],
        status_category=issue_header_dict["fields"]["status"]["statusCategory"]["key"],
        summary=issue_header_dict["fields"]["summary"],
    )


def parse_account(
    account_dict: Optional[dict], developer_map_callback: Callable
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    account_dict = account_dict or {}
    api_id = account_dict.get("accountId")
    email = account_dict.get("emailAddress")
    name = account_dict.get("displayName")
    dev_id = developer_map_callback(AuthorAlias(name=name, email=email)) if email or name else None
    return api_id, email, name, dev_id


def transform_dict_to_issue(
    issue_dict: dict, its_project: ITSProjectInDB, developer_map_callback: Callable, priority_orders: Dict[str, int]
) -> ITSIssue:

    creator_api_id, creator_email, creator_name, creator_dev_id = parse_account(
        issue_dict["fields"].get("creator"), developer_map_callback
    )
    reporter_api_id, reporter_email, reporter_name, reporter_dev_id = parse_account(
        issue_dict["fields"].get("reporter"), developer_map_callback
    )
    assignee_api_id, assignee_email, assignee_name, assignee_dev_id = parse_account(
        issue_dict["fields"].get("assignee"), developer_map_callback
    )

    return ITSIssue(
        id=f"{its_project.id}-{issue_dict['id']}",
        created_at=issue_dict["fields"]["created"],
        updated_at=issue_dict["fields"]["updated"],
        itsp_id=its_project.id,
        api_url=issue_dict["self"],
        api_id=issue_dict["id"],
        key=issue_dict["key"],
        # Current status
        status_name=issue_dict["fields"]["status"]["name"],
        status_id=issue_dict["fields"]["status"]["id"],
        status_category=issue_dict["fields"]["status"]["statusCategory"]["key"],
        # Issue Type
        issue_type_name=issue_dict["fields"]["issuetype"]["name"],
        issue_type_id=issue_dict["fields"]["issuetype"]["id"],
        # Get parent (if any)
        parent_id=f"{its_project.id}-{issue_dict['fields']['parent']['id']}"
        if issue_dict["fields"].get("parent", {})
        else None,
        # Get resolution
        resolution_name=issue_dict["fields"]["resolution"]["name"] if issue_dict["fields"].get("resolution") else None,
        resolution_id=issue_dict["fields"]["resolution"]["id"] if issue_dict["fields"].get("resolution") else None,
        resolution_date=issue_dict["fields"].get("resolutiondate"),
        # Get issue priority
        priority_name=issue_dict["fields"]["priority"]["name"],
        priority_id=issue_dict["fields"]["priority"]["id"],
        priority_order=priority_orders.get(issue_dict["fields"]["priority"]["name"]),
        # Summary
        summary=issue_dict["fields"]["summary"],
        # creator
        creator_api_id=creator_api_id,
        creator_email=creator_email,
        creator_name=creator_name,
        creator_dev_id=creator_dev_id,
        # reporter
        reporter_api_id=reporter_api_id,
        reporter_email=reporter_email,
        reporter_name=reporter_name,
        reporter_dev_id=reporter_dev_id,
        # assignee
        assignee_api_id=assignee_api_id,
        assignee_email=assignee_email,
        assignee_name=assignee_name,
        assignee_dev_id=assignee_dev_id,
        # extra
        extra=issue_dict,
    )


def _get_rest_api_base_url_from_project_api_url(api_url: str) -> str:
    if api_url.startswith("https://api.atlassian.com/ex/jira/"):
        splitted = api_url.split("/")
        return "https://api.atlassian.com/ex/jira/" + splitted[5]
    raise ValueError(f"Don't know how to parse jira project api url: {api_url}")


def get_all_pages_from_paginated(client, start_url: str, start_at=0, max_results=25, values_key="values") -> list[dict]:
    def _get_resp(client, url, start_at, max_results):
        resp = client.get(url)
        if resp.status_code == 200:
            # print(url, resp, resp.json().keys())
            resp_json = resp.json()
            items = resp_json.get(values_key, [])
            start_at = resp_json.get("startAt", start_at)
            max_results = resp_json.get("maxResults", max_results)
            total = resp_json.get("total", 0)
            return items, start_at, max_results, total
        else:
            return [], 0, 0, 0

    ret = []
    total = 0

    while True:
        start_url = add_url_params(start_url, {"startAt": start_at, "maxResults": max_results})
        items, start_at, max_results, total = _get_resp(client, start_url, start_at, max_results)

        if items:
            ret += items

        if start_at + max_results < total:
            start_at = start_at + max_results
        else:
            break

    return ret
