from unittest.mock import patch, mock_open
import pytest
from pydantic import ValidationError
from gitential2.settings import load_settings, GitentialSettings


def test_load_settings():
    read_data = """
log_level: debug
executor: process_pool
show_progress: true
repository_sources:
  gitlab:
    source_type: gitlab
    base_url: https://gitlab.ops.gitential.com
    client_id: "client_id"
    client_secret: "client_secret"
    use_as_login: true
    """

    with patch("gitential2.settings.open", mock_open(read_data=read_data)):
        settings = load_settings()
        assert isinstance(settings, GitentialSettings)
        assert settings.log_level == "debug"
        assert settings.executor == "process_pool"
        assert "gitlab" in settings.repository_sources
        assert settings.repository_sources["gitlab"].base_url == "https://gitlab.ops.gitential.com"
        assert settings.repository_sources["gitlab"].client_id == "client_id"
        assert settings.repository_sources["gitlab"].client_secret == "client_secret"
        assert settings.repository_sources["gitlab"].client_secret == "client_secret"
        assert settings.repository_sources["gitlab"].use_as_login
        assert settings.repository_sources["gitlab"].login_text is None
        assert settings.backend == "in_memory"
        assert settings.backend_connection is None


def test_load_settings_with_validation_error():
    read_data = """
log_level: something
"""

    with patch("gitential2.settings.open", mock_open(read_data=read_data)):
        with pytest.raises(ValidationError):
            load_settings()
