import os
from typing import List, Optional, Dict, Union
from enum import Enum

import yaml
from pydantic import BaseSettings


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warn = "warn"
    error = "error"
    critical = "critical"


class IntegrationType(str, Enum):
    gitlab = "gitlab"
    github = "github"
    linkedin = "linkedin"


class Executor(str, Enum):
    process_pool = "process_pool"
    single_tread = "single_thread"


class OAuthClientSettings(BaseSettings):
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class IntegrationSettings(BaseSettings):
    type_: IntegrationType
    base_url: Optional[str] = None
    oauth: Optional[OAuthClientSettings] = None
    login: bool = False
    login_text: Optional[str] = None
    signup_text: Optional[str] = None
    options: Dict[str, Union[str, int, float, bool]] = {}

    class Config:
        fields = {"type_": "type"}


class BackendType(str, Enum):
    in_memory = "in_memory"
    sql = "sql"


class GitentialSettings(BaseSettings):
    secret: str = "gitential.secret.change.me"
    base_url: str = "http://localhost:8080"
    log_level: LogLevel = LogLevel.info
    executor: Executor = Executor.process_pool
    process_pool_size: int = 8
    show_progress: bool = False
    integrations: Dict[str, IntegrationSettings]
    backend: BackendType = "in_memory"
    backend_connection: Optional[str] = None


def load_settings(settings_file=None):
    settings_file = settings_file or os.environ.get("GITENTIAL_SETTINGS", "settings.yml")
    with open(settings_file, "r") as f:
        config_dict = yaml.safe_load(f)
        return GitentialSettings.parse_obj(config_dict)
