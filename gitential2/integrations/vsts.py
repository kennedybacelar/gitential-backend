from typing import Optional, Callable, List
from urllib.parse import parse_qs

# from pydantic.datetime_parse import parse_datetime
from structlog import get_logger


from gitential2.datatypes import UserInfoCreate, RepositoryCreate, RepositoryInDB

# from gitential2.datatypes.extraction import ExtractedKind
# from gitential2.datatypes.pull_requests import PullRequest, PullRequestState
from gitential2.extraction.output import OutputHandler

from .base import BaseIntegration, OAuthLoginMixin, GitProviderMixin

# from .common import log_api_error

logger = get_logger(__name__)

# https://app.vssps.visualstudio.com/oauth2/authorize
# ?response_type=code
# &client_id=C195AA32-A678-41FD-B946-199D94DD8B4F
# &redirect_uri=https%3A%2F%2Flaco.ngrok.io%2Flogin%3Fsource%3Dvsts
# &scope=vso.code
# &state=eLz5CopZ1EP7TND5ETbPZjz4AEOnMT

# https://app.vssps.visualstudio.com/oauth2/authorize
# ?client_id=046A49EA-B74F-4E1D-90E2-328C699E8875
# &response_type=Assertion
# &state=wrae&scope=vso.code+vso.project
# &redirect_uri=https%3A%2F%2Fapi.gitential.com%2Flogin

# https://laco.ngrok.io/login
# ?code=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Im9PdmN6NU1fN3AtSGpJS2xGWHo5M3VfVjBabyJ9.eyJhdWkiOiIxN2NiODM0OS0wZTI1LTQ2NWQtOTZiNS01ZmJhNjYwMGMwNjQiLCJuYW1laWQiOiI4ZjdjZjliOS01NzdmLTZiMmQtOTk4YS0wYTA3Yjk4MzVmMjEiLCJzY3AiOiJ2c28uY29kZSB2c28uaWRlbnRpdHkgdnNvLmF1dGhvcml6YXRpb25fZ3JhbnQiLCJpc3MiOiJhcHAudnN0b2tlbi52aXN1YWxzdHVkaW8uY29tIiwiYXVkIjoiYXBwLnZzdG9rZW4udmlzdWFsc3R1ZGlvLmNvbSIsIm5iZiI6MTYxNjc5MDYzMywiZXhwIjoxNjE2NzkxNTMzfQ.JOzsdIgGBXvsVtyzNtmTSiMnZn55Mu63w8d5s4GppcOhsyt16i9ueRl0lsNRcAj45QIh66I2FiaY6N19zJzlnq9ahtOykrwajzZEnCrJs_KsSDBlwz-HVtaohOsADgxWw6A3Njr5d5BTfsU373nbQmyBBkofucG-69Nw6AwqL6cjPqFu81lcCFU6ZeOpLFjmW8xmrwYMJbGulCcn5EQUbn79zu5o9sa_bnqQzpUmRpoVTrhLCsnFpChEzq0bTghs1zVUxuGEB4lBkBnvhjhR_MdS8GKnQcBbTekMH7ToUq2yT-KVzTRFmcM49ZsJ-6_Jf_vFO6lSqrXjO-Lfgvsymg
# &state=yvl1BPMeWu7dQwTJwExH3pX00buds6


class VSTSIntegration(OAuthLoginMixin, GitProviderMixin, BaseIntegration):
    def _auth_client_secret_uri(self, client, method, uri, headers, body):
        logger.debug(
            "vsts._auth_client_secret_uri inputs", client=client, method=method, uri=uri, headers=headers, body=body
        )
        body_original = parse_qs(body)

        body_ = {
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self.settings.oauth.client_secret,
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": body_original[b"code"][0].decode(),
            "redirect_uri": body_original[b"redirect_uri"][0].decode(),
        }
        body_str = "&".join([f"{k}={v}" for (k, v) in body_.items()])

        headers["content-length"] = str(len(body_str))
        logger.debug("vsts._auth_client_secret_uri outputs", uri=uri, headers=headers, body_str=body_str)
        return uri, headers, body_str

    def oauth_register(self):
        base_url = "https://app.vssps.visualstudio.com"
        return {
            "api_base_url": base_url,
            "access_token_url": f"{base_url}/oauth2/token",
            "authorize_url": f"{base_url}/oauth2/authorize",
            "userinfo_endpoint": f"{base_url}/_apis/profile/profiles/me?api-version=4.1",
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

    def list_available_private_repositories(self, token, update_token) -> List[RepositoryCreate]:
        return []

    def search_public_repositories(
        self,
        query: str,
        token,
        update_token,
    ) -> List[RepositoryCreate]:
        return []
