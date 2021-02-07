from typing import Optional
from gitential2.datatypes import UserInfoCreate

from .base import BaseIntegration, OAuthLoginMixin


class GitlabIntegration(OAuthLoginMixin, BaseIntegration):
    def __init__(self, name, settings):
        super().__init__(name, settings)
        self.base_url = self.settings.base_url or "https://gitlab.com"
        self.api_base_url = "{}/api/v4/".format(self.base_url)
        self.authorize_url = "{}/oauth/authorize".format(self.base_url)
        self.token_url = "{}/oauth/token".format(self.base_url)

    def oauth_register(self):

        return {
            "api_base_url": self.api_base_url,
            "access_token_url": self.token_url,
            "authorize_url": self.authorize_url,
            "userinfo_endpoint": "user",
            "client_kwargs": {"scope": "api read_repository email read_user"},
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

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

    def list_available_private_repositories(self, token, update_token):
        def _get_next_link(link_header) -> Optional[str]:
            link, rel = link_header.split(";")
            if "next" in rel.lower():
                return link.strip("<>")
            return None

        def _keyset_pagination(client, starting_url, acc=None):
            acc = acc or []
            response = client.request("GET", starting_url)
            items, headers = response.json(), response.headers
            acc = acc + items
            if headers.get("Link"):
                next_url = _get_next_link(headers.get("Link"))
                return _keyset_pagination(client, next_url, acc)
            else:
                return acc

        def _project_dict(project):
            return {
                "clone_url": project["http_url_to_repo"],
                "id": project["id"],
                "name": project["path"],
                "source": project["namespace"]["full_path"],
                "private": project["visibility"] == "private",
            }

        client = self.get_oauth2_client(token=token, update_token=update_token)
        projects = _keyset_pagination(
            client, f"{self.api_base_url}/projects?membership=1&pagination=keyset&order_by=id"
        )
        client.close()
        return [_project_dict(p) for p in projects]


# """
# example_result = {
#     "token": {
#         "access_token": "ada51140982179da9c24016cad52c69004eab4c762035a984fe9ce261a44d060",
#         "token_type": "Bearer",
#         "refresh_token": "ac57d2fe49691b823d2a38b074066bbefffecfa54ef08e0e5e755b5f05437ed9",
#         "scope": "read_user",
#         "created_at": 1611422247,
#     },
#     "user_info": {
#         "id": 2,
#         "name": "Laszlo Andrasi",
#         "username": "laco",
#         "state": "active",
#         "avatar_url": "https://secure.gravatar.com/avatar/9846a4a493c991ae8c71384192c804f4?s=80&d=identicon",
#         "web_url": "https://gitlab.ops.gitential.com/laco",
#         "created_at": "2021-01-09T15:00:07.133Z",
#         "bio": "",
#         "bio_html": "",
#         "location": null,
#         "public_email": "",
#         "skype": "",
#         "linkedin": "",
#         "twitter": "",
#         "website_url": "",
#         "organization": null,
#         "job_title": "",
#         "work_information": null,
#         "last_sign_in_at": "2021-01-19T17:07:38.429Z",
#         "confirmed_at": "2021-01-09T15:00:06.827Z",
#         "last_activity_on": "2021-01-23",
#         "email": "laszlo.andrasi@gitential.com",
#         "theme_id": 1,
#         "color_scheme_id": 1,
#         "projects_limit": 100000,
#         "current_sign_in_at": "2021-01-23T13:14:13.329Z",
#         "identities": [],
#         "can_create_group": true,
#         "can_create_project": true,
#         "two_factor_enabled": false,
#         "external": false,
#         "private_profile": false,
#         "shared_runners_minutes_limit": null,
#         "extra_shared_runners_minutes_limit": null,
#     },
# }
# """

# class GitLabSource(RepositorySource):
#     def authentication_url(self, frontend_url):
#         return "".join(
#             [
#                 self._settings.base_url,
#                 "/oauth/authorize?",
#                 f"client_id={self._settings.client_id}",
#                 "&scope=openid",
#                 "&response_type=code",
#                 "&state=FIXME_PUT_SOMETHING_USEFUL_HERE",
#                 f"&redirect_uri={self._app_settings.base_url}/v2/login/{self.name}/",
#             ]
#         )

#     def get_available_private_repositories(self, credentials: Credential) -> List[GitRepository]:
#         pass

#     def get_available_public_repositories(self, query: str, credentials: Credential) -> List[GitRepository]:
#         pass


# # http://localhost:8080/v2/login/gitlab?code=c3351ad52fe85c44125b43243525685cc59e7e1e15f2d66b3cc4b13efb199d86&state=FIXME_PUT_SOMETHING_USEFUL_HERE
