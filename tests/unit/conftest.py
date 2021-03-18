import os
import pytest
from gitential2.secrets import FernetVault
from gitential2.settings import GitentialSettings, BackendType, ExtractionSettings


@pytest.fixture(scope="session")
def secrets():
    try:
        fernet_key = os.environ["GITENTIAL2_TEST_SECRET"].encode()
    except KeyError:
        raise ValueError(
            "Testing needs the GITENTIAL2_TEST_SECRET env variable."
            "Please ask one of your team members if you lost it."
        )

    vault = FernetVault(fernet_key)
    vault_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "secrets.json")
    vault.load(vault_file_path)

    # vault["..."] = ...
    # vault.save(vault_file_path)
    return vault


@pytest.fixture
def settings():
    return GitentialSettings(
        backend=BackendType.in_memory,
        extraction=ExtractionSettings(
            executor="process_pool",
            show_progress=False,
        ),
        integrations={},
        secret="test" * 8,
    )
