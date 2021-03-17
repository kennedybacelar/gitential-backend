import os
from base64 import b64encode
from typing import Optional, Dict, Union, List
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
    type: IntegrationType
    base_url: Optional[str] = None
    oauth: Optional[OAuthClientSettings] = None
    login: bool = False
    login_text: Optional[str] = None
    signup_text: Optional[str] = None
    login_top_text: Optional[str] = None
    options: Dict[str, Union[str, int, float, bool]] = {}

    @property
    def type_(self):
        return self.type


class BackendType(str, Enum):
    in_memory = "in_memory"
    sql = "sql"


class KeyValueStoreType(str, Enum):
    in_memory = "in_memory"
    redis = "redis"


class CelerySettings(BaseModel):
    broker_url: Optional[str] = None
    result_backend_url: Optional[str] = None


class ConnectionSettings(BaseModel):
    database_url: Optional[str] = None
    redis_url: Optional[str] = "redis://localhost:6379/0"


class HTMLElementPosition(str, Enum):
    beforebegin = "beforebegin"
    afterbegin = "afterbegin"
    beforeend = "beforeend"
    afterend = "afterend"


class HTMLInjection(BaseModel):
    parent: str = "head"
    tag: str = ""
    content: str = ""
    position: HTMLElementPosition = HTMLElementPosition.beforeend
    attributes: Dict[str, Union[int, bool, str]] = {}


class RecaptchaSettings(BaseModel):
    site_key: str = ""
    secret_key: str = ""


class FrontendSettings(BaseModel):
    inject_html: List[HTMLInjection] = []


class EmailSettings(BaseModel):
    sender: str = "gitential@gitential.com"
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None


class GitentialSettings(BaseModel):
    secret: str
    connections: ConnectionSettings = ConnectionSettings()
    email: EmailSettings = EmailSettings()
    recaptcha: RecaptchaSettings = RecaptchaSettings()
    integrations: Dict[str, IntegrationSettings]
    base_url: str = "http://localhost:7999"
    backend: BackendType = BackendType.in_memory
    kvstore: KeyValueStoreType = KeyValueStoreType.redis
    celery: CelerySettings = CelerySettings()
    log_level: LogLevel = LogLevel.info
    executor: Executor = Executor.process_pool
    process_pool_size: int = 8
    show_progress: bool = False
    frontend: FrontendSettings = FrontendSettings()

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
