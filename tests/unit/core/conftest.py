import pytest
from gitential2.settings import GitentialSettings, IntegrationSettings, IntegrationType
from gitential2.backends import InMemGitentialBackend
from gitential2.integrations.base import BaseIntegration, OAuthLoginMixin
from gitential2.datatypes import UserInfoCreate


@pytest.fixture
def minimal_settings():
    return GitentialSettings(secret="test" * 8, integrations={})


@pytest.fixture
def inmem_backend(minimal_settings):
    return InMemGitentialBackend(minimal_settings)


class DummyIntegration(OAuthLoginMixin, BaseIntegration):
    def oauth_register(self) -> dict:
        return {}

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        return UserInfoCreate(
            integration_name=self.name,
            integration_type="dummy",
            sub=str(data["id"]),
            name=data["name"],
            email=data.get("email"),
            preferred_username=data.get("username"),
            profile=data.get("web_url"),
            picture=data.get("avatar_url"),
            website=data.get("website_url"),
            extra=data,
        )


@pytest.fixture
def dummy_integration():
    return DummyIntegration(name="dummy", settings=IntegrationSettings(type=IntegrationType.dummy))


@pytest.fixture
def dummy_fernet():
    class DummyFernet:
        def encrypt_string(self, s: str) -> str:
            return s

        def decrypt_string(self, s: str) -> str:
            return s

    return DummyFernet()
