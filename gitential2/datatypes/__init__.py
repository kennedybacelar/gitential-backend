from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Dict, Optional
from pathlib import Path
from pydantic.dataclasses import dataclass


class Kind(Enum):
    E_COMMIT = "e_commit"
    E_PATCH = "e_patch"
    E_PATCH_REWRITE = "e_patch_rewrite"


class RepositoryCredentials(ABC):
    pass


@dataclass
class KeypairCredentials(RepositoryCredentials):
    username: str = "git"
    pubkey: Optional[str] = None
    privkey: Optional[str] = None
    passphrase: Optional[str] = None


@dataclass
class UserPassCredential(RepositoryCredentials):
    username: str
    password: str


@dataclass
class LocalGitRepository:
    repo_id: int
    directory: Path
