from unittest.mock import patch, mock_open
import pytest
from pydantic import ValidationError
from gitential2.settings import load_settings, GitentialSettings


def test_load_settings():
    read_data = """
secret: "abcdefghabcdefghabcdefghabcdefgh"
log_level: debug
executor: process_pool
show_progress: true
integrations:
  gitlab:
    type: gitlab
    base_url: https://gitlab.ops.gitential.com
    oauth:
      client_id: "client_id"
      client_secret: "client_secret"

    login: true
    """

    with patch("gitential2.settings.open", mock_open(read_data=read_data)):
        settings = load_settings()
        assert settings.fernet_key == b"YWJjZGVmZ2hhYmNkZWZnaGFiY2RlZmdoYWJjZGVmZ2g="
        assert isinstance(settings, GitentialSettings)
        assert settings.secret == "abcdefghabcdefghabcdefghabcdefgh"
        assert settings.log_level == "debug"
        assert settings.executor == "process_pool"
        assert "gitlab" in settings.integrations
        assert settings.integrations["gitlab"].base_url == "https://gitlab.ops.gitential.com"
        assert settings.integrations["gitlab"].oauth.client_id == "client_id"
        assert settings.integrations["gitlab"].oauth.client_secret == "client_secret"

        assert settings.integrations["gitlab"].login
        assert settings.integrations["gitlab"].login_text is None
        assert settings.backend == "in_memory"
        assert settings.connections.database_url is None


def test_load_settings_with_validation_error():
    read_data = """
log_level: something
"""

    with patch("gitential2.settings.open", mock_open(read_data=read_data)):
        with pytest.raises(ValidationError):
            load_settings()
