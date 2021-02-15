from gitential2.datatypes import UserInfoCreate
from .base import OAuthLoginMixin, BaseIntegration


class GithubIntegration(OAuthLoginMixin, BaseIntegration):
    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        pass

    def oauth_register(self):
        return {
            "api_base_url": "https://api.github.com/",
            "access_token_url": "https://github.com/login/oauth/access_token",
            "authorize_url": "https://github.com/login/oauth/authorize",
            "client_kwargs": {"scope": "user:email"},
            "userinfo_endpoint": "https://api.github.com/user",
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }


# """
# example_result = {
#     "token": {
#         "access_token": "e6b27307e25d30f61bde69284d1d5cf6e7a64bbf",
#         "token_type": "bearer",
#         "scope": "user:email",
#     },
#     "user_info": {
#         "login": "laco",
#         "id": 472365,
#         "node_id": "MDQ6VXNlcjQ3MjM2NQ==",
#         "avatar_url": "https://avatars.githubusercontent.com/u/472365?v=4",
#         "gravatar_id": "",
#         "url": "https://api.github.com/users/laco",
#         "html_url": "https://github.com/laco",
#         "followers_url": "https://api.github.com/users/laco/followers",
#         "following_url": "https://api.github.com/users/laco/following{/other_user}",
#         "gists_url": "https://api.github.com/users/laco/gists{/gist_id}",
#         "starred_url": "https://api.github.com/users/laco/starred{/owner}{/repo}",
#         "subscriptions_url": "https://api.github.com/users/laco/subscriptions",
#         "organizations_url": "https://api.github.com/users/laco/orgs",
#         "repos_url": "https://api.github.com/users/laco/repos",
#         "events_url": "https://api.github.com/users/laco/events{/privacy}",
#         "received_events_url": "https://api.github.com/users/laco/received_events",
#         "type": "User",
#         "site_admin": false,
#         "name": "László Andrási",
#         "company": "Software Development Consultant",
#         "blog": "https://laszloandrasi.com",
#         "location": "Budapest",
#         "email": "mail@laszloandrasi.com",
#         "hireable": true,
#         "bio": "Lead Developer / Software Engineer",
#         "twitter_username": null,
#         "public_repos": 49,
#         "public_gists": 5,
#         "followers": 25,
#         "following": 40,
#         "created_at": "2010-11-08T12:36:18Z",
#         "updated_at": "2021-01-23T23:44:21Z",
#     },
# }
# """
