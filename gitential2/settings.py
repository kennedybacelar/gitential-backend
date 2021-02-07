import os
from base64 import b64encode
from typing import Optional, Dict, Union
from enum import Enum

import yaml
from pydantic import BaseModel, validator


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warn = "warn"
    error = "error"
    critical = "critical"


class IntegrationType(str, Enum):
    dummy = "dummy"
    gitlab = "gitlab"
    github = "github"
    linkedin = "linkedin"


class Executor(str, Enum):
    process_pool = "process_pool"
    single_tread = "single_thread"


class OAuthClientSettings(BaseModel):
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class IntegrationSettings(BaseModel):
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


class GitentialSettings(BaseModel):
    secret: str
    base_url: str = "http://localhost:8080"
    log_level: LogLevel = LogLevel.info
    executor: Executor = Executor.process_pool
    process_pool_size: int = 8
    show_progress: bool = False
    integrations: Dict[str, IntegrationSettings]
    backend: BackendType = BackendType.in_memory
    backend_connection: Optional[str] = None

    @validator("secret")
    def secret_validation(cls, v):
        if len(v) < 32:
            raise ValueError("Secret must be at least 32 bytes long")
        return v

    @property
    def fernet_key(self) -> bytes:
        s: str = self.secret[:32]
        return b64encode(s.encode())


def load_settings(settings_file=None):
    settings_file = settings_file or os.environ.get("GITENTIAL_SETTINGS", "settings.yml")
    with open(settings_file, "r") as f:
        config_dict = yaml.safe_load(f)
        return GitentialSettings.parse_obj(config_dict)
