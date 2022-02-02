from typing import Callable, Tuple, List
from pydantic import BaseModel, Field
from gitential2.datatypes.userinfos import UserInfoCreate
from .base import BaseIntegration, OAuthLoginMixin


class AtlassianSite(BaseModel):
    id: str
    name: str
    url: str
    scopes: List[str]
    avatar_url: str = Field(..., alias="avatarUrl")


class JiraProject(BaseModel):
    """
    {'expand': 'description,lead,issueTypes,url,projectKeys,permissions,insight',
    'self': 'https://api.atlassian.com/ex/jira/71a97d8e-8582-42ae-b0c3-95d25b8b3c14/rest/api/2/project/10016',
    'id': '10016',
    'key': 'LOVE',
    'name': 'Gitential Project Delivery',
    'avatarUrls': {
        '48x48': 'https://api.atlassian.com/ex/jira/71a97d8e-8582-42ae-b0c3-95d25b8b3c14/rest/api/2/universal_avatar/view/type/project/avatar/10400',
        '24x24': 'https://api.atlassian.com/ex/jira/71a97d8e-8582-42ae-b0c3-95d25b8b3c14/rest/api/2/universal_avatar/view/type/project/avatar/10400?size=small',
        '16x16': 'https://api.atlassian.com/ex/jira/71a97d8e-8582-42ae-b0c3-95d25b8b3c14/rest/api/2/universal_avatar/view/type/project/avatar/10400?size=xsmall',
        '32x32': 'https://api.atlassian.com/ex/jira/71a97d8e-8582-42ae-b0c3-95d25b8b3c14/rest/api/2/universal_avatar/view/type/project/avatar/10400?size=medium'},
    'projectTypeKey': 'software',
    'simplified': True,
    'style': 'next-gen',
    'isPrivate': False,
    'properties': {},
    'entityId': '5d461c16-7ef7-442c-b073-f802f45b9197',
    'uuid': '5d461c16-7ef7-442c-b073-f802f45b9197'}"""


class JiraIntegration(OAuthLoginMixin, BaseIntegration):
    # def __init__(self, name, settings):
    #     super().__init__(name, settings)
    #     # self.base_url = self.settings.base_url or "https://gitlab.com"
    #     # self.api_base_url = f"{self.base_url}/api/v4"
    #     # self.authorize_url = f"{self.base_url}/oauth/authorize"
    #     # self.token_url = f"{self.base_url}/oauth/token"
    """
    https://auth.atlassian.com/authorize?audience=api.atlassian.com&client_id=uBMcWdotssP2J9uwCQhht9KxLFTe9wM1&scope=read%3Ajira-user%20read%3Ajira-work&redirect_uri=https%3A%2F%2Flocalhost%3A8443%2Fv2%2Fauth%2Fjira&state=${YOUR_USER_BOUND_VALUE}&response_type=code&prompt=consent



    """

    def oauth_register(self) -> dict:
        return {
            # "api_base_url": self.api_base_url,
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

    def list_accessible_resources(self, token, update_token: Callable) -> List[AtlassianSite]:
        client = self.get_oauth2_client(token=token, update_token=update_token)
        resp = client.get("https://api.atlassian.com/oauth/token/accessible-resources")
        client.close()
        return [AtlassianSite.parse_obj(item) for item in resp.json()]

    def list_available_projects(self, token, update_token: Callable):
        sites = self.list_accessible_resources(token, update_token)
        client = self.get_oauth2_client(token=token, update_token=update_token)
        for site in sites:
            if "read:jira-work" in site.scopes:
                site_id = site.id
                resp = client.get(f"https://api.atlassian.com/ex/jira/{site_id}/rest/api/2/project")
                resp_json = resp.json()
                print(resp_json)
            else:
                print("no jira access for site", site)

    def refresh_token(self, token, update_token):
        client = self.get_oauth2_client(
            token=token,
            update_token=update_token,
        )
        token = client.refresh_token(self.oauth_register()["access_token_url"], refresh_token=token["refresh_token"])
        client.close()
        return {f: token[f] for f in ["access_token", "refresh_token", "expires_at"]}

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
