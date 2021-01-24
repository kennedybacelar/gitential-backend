from abc import ABC, abstractmethod
from authlib.integrations.requests_client import OAuth2Session

from gitential2.settings import IntegrationSettings


class BaseIntegration:
    def __init__(self, name, settings: IntegrationSettings):
        self.name = name
        self.settings = settings

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
