from typing import List
from gitential2.datatypes.repositories import GitRepository
from .common import RepositorySource


class Credential:
    pass


class GitLabSource(RepositorySource):
    def authentication_url(self, frontend_url):
        return "".join(
            [
                self._settings.base_url,
                "/oauth/authorize?",
                f"client_id={self._settings.client_id}",
                "&scope=openid",
                "&response_type=code",
                "&state=FIXME_PUT_SOMETHING_USEFUL_HERE",
                f"&redirect_uri={self._app_settings.base_url}/v2/login/{self.name}/",
            ]
        )

    def get_available_private_repositories(self, credentials: Credential) -> List[GitRepository]:
        pass

    def get_available_public_repositories(self, query: str, credentials: Credential) -> List[GitRepository]:
        pass


# http://localhost:8080/v2/login/gitlab?code=c3351ad52fe85c44125b43243525685cc59e7e1e15f2d66b3cc4b13efb199d86&state=FIXME_PUT_SOMETHING_USEFUL_HERE
