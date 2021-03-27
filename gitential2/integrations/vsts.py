from typing import Optional, Callable, List
from urllib.parse import parse_qs

# from pydantic.datetime_parse import parse_datetime
from structlog import get_logger


from gitential2.datatypes import UserInfoCreate, RepositoryCreate, RepositoryInDB, GitProtocol

# from gitential2.datatypes.extraction import ExtractedKind
# from gitential2.datatypes.pull_requests import PullRequest, PullRequestState
from gitential2.extraction.output import OutputHandler

from .base import BaseIntegration, OAuthLoginMixin, GitProviderMixin

from .common import log_api_error

logger = get_logger(__name__)


class VSTSIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    base_url = "https://app.vssps.visualstudio.com"

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
                "scope": "vso.code vso.identity",
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

    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        prs_we_already_have: Optional[dict] = None,
    ):
        pass

    def refresh_token(self, token, update_token):
        client = self.get_oauth2_client(
            token=token, update_token=update_token, token_endpoint_auth_method=self._auth_client_secret_uri
        )
        token = client.refresh_token(self.oauth_register()["access_token_url"], refresh_token=token["refresh_token"])
        client.close()
        return {f: token[f] for f in ["access_token", "refresh_token", "expires_at"]}

    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:

        if not provider_user_id:
            logger.warn("Cannot list vsts repositories, provider_user_id is missing", token=token)
            return []
        token = self.refresh_token(token, update_token)

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
