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


@dataclass
class GitRepositoryState:
    branches: Dict[str, str]
    tags: Dict[str, str]

    @property
    def commit_ids(self):
        return list(self.branches.values()) + list(self.tags.values())


@dataclass
class GitRepositoryStateChange:
    old_state: GitRepositoryState
    new_state: GitRepositoryState

    @property
    def new_branches(self):
        return {b: cid for b, cid in self.new_state.branches.items() if b not in self.old_state.branches}


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
    directory: Path


@dataclass
class ECommit:
    commit_id: str
    atime: datetime
    aemail: str
    aname: str
    ctime: datetime
    cemail: str
    cname: str
    message: str
    nparents: int
    tree_id: str


@dataclass
class EPatch:
    commit_id: str
    parent_commit_id: str
    status: str
    newpath: str
    oldpath: str
    newsize: int
    oldsize: int
    is_binary: bool
    lang: str
    langtype: str

    loc_i: int
    loc_d: int
    comp_i: int
    comp_d: int
    loc_i_std: float
    loc_d_std: float
    comp_i_std: float
    comp_d_std: float

    nhunks: int
    nrewrites: int
    rewrites_loc: int


@dataclass
class EPatchRewrite:
    commit_id: str
    atime: datetime
    aemail: str
    newpath: str
    rewritten_commit_id: str
    rewritten_atime: datetime
    rewritten_aemail: str
    loc_d: int
