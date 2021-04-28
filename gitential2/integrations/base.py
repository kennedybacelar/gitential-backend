from abc import ABC, abstractmethod

from typing import Callable, List, Optional
from authlib.integrations.requests_client import OAuth2Session

from gitential2.settings import IntegrationSettings
from gitential2.datatypes import UserInfoCreate, RepositoryInDB
from gitential2.datatypes.repositories import RepositoryCreate
from gitential2.extraction.output import OutputHandler


class BaseIntegration:
    def __init__(self, name, settings: IntegrationSettings):
        self.name = name
        self.settings = settings
        self.integration_type = settings.type_

    @property
    def is_oauth(self) -> bool:
        return False


class OAuthLoginMixin(ABC):
    @property
    def is_oauth(self) -> bool:
        return True

    @abstractmethod
    def oauth_register(self) -> dict:
        pass

    def get_oauth2_client(self, **kwargs):
        params = self.oauth_register()
        params.update(kwargs)
        return OAuth2Session(**params)

    @abstractmethod
    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        pass

    @abstractmethod
    def refresh_token_if_expired(self, token, update_token: Callable) -> bool:
        pass


class GitProviderMixin(ABC):
    @abstractmethod
    def collect_pull_requests(
        self,
        repository: RepositoryInDB,
        token: dict,
        update_token: Callable,
        output: OutputHandler,
        prs_we_already_have: Optional[dict] = None,
        limit: int = 200,
    ):
        pass

    @abstractmethod
    def list_available_private_repositories(
        self, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        pass

    @abstractmethod
    def search_public_repositories(
        self, query: str, token, update_token, provider_user_id: Optional[str]
    ) -> List[RepositoryCreate]:
        pass
