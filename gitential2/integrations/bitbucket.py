from typing import Optional, Callable, List

# from pydantic.datetime_parse import parse_datetime
from structlog import get_logger
from gitential2.datatypes import UserInfoCreate, RepositoryCreate, GitProtocol, RepositoryInDB

# from gitential2.datatypes.extraction import ExtractedKind
# from gitential2.datatypes.pull_requests import PullRequest, PullRequestState
from gitential2.extraction.output import OutputHandler
from gitential2.utils import rchop
from .base import BaseIntegration, OAuthLoginMixin, GitProviderMixin
from .common import log_api_error

logger = get_logger(__name__)


class BitBucketIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
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

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        prs_we_already_have: Optional[dict] = None,
    ):
        pass

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
            namespace=rchop(repo_dict["full_name"], "/" + repo_dict["name"]),
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


def _walk_paginated_results(client, starting_url, acc=None):
    response = client.get(starting_url)
    acc = acc or []

    if response.status_code != 200:
        log_api_error(response)
        return acc

    data = response.json()
    if "values" in data:
        acc += data["values"]
    if "next" in data:
        return _walk_paginated_results(client, data["next"], acc)
    else:
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
