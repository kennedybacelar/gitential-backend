import pytest
from gitential2.core import GitentialContext, handle_authorize
from gitential2.kvstore import InMemKeyValueStore
from gitential2.license import dummy_license


def test_handle_auth_empty_database_create_new_user(minimal_settings, inmem_backend, dummy_integration, dummy_fernet):
    gitential = GitentialContext(
        settings=minimal_settings,
        integrations={"dummy": dummy_integration},
        backend=inmem_backend,
        fernet=dummy_fernet,
        kvstore=InMemKeyValueStore(minimal_settings),
        license_=dummy_license,
    )
    result = handle_authorize(
        gitential,
        integration_name="dummy",
        token={"access_token": "access_token"},
        user_info={"id": 12345, "name": "Mr User Example", "username": "user", "email": "user@example.com"},
    )
    assert result["user"].email == "user@example.com"
    assert result["user"].login == "user"
    assert result["user"].id is not None


@pytest.mark.skip(reason="Not implemented")
def test_handle_auth_user_exists_updates_info_and_token():
    pass


@pytest.mark.skip(reason="Not implemented")
def test_handle_auth_user_exists_new_user_info_and_credential_added():
    pass


@pytest.mark.skip(reason="Not implemented")
def test_handle_auth_with_current_user_and_existing_user_different():
    pass
