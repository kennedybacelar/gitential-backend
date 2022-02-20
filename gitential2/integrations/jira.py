from typing import Callable, Tuple, List, Optional
from pydantic import BaseModel, Field
from structlog import get_logger
from gitential2.datatypes.its_projects import ITSProjectCreate
from gitential2.datatypes.userinfos import UserInfoCreate
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

    def list_available_its_projects(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[ITSProjectCreate]:

        jira_projects = self.list_available_jira_projects(token)
        ret = []
        for site, project_dict in jira_projects:
            ret.append(self._transform_to_its_project(site, project_dict))
        return ret

    def _transform_to_its_project(self, site: AtlassianSite, project_dict: dict) -> ITSProjectCreate:
        print(project_dict)
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
