from enum import Enum
from gitential2.datatypes.common import CoreModel
from typing import Optional
from pathlib import Path
from datetime import datetime
from pydantic import BaseModel


class LocalGitRepository(BaseModel):
    repo_id: Optional[int] = None
    directory: Path


class ExtractedKind(str, Enum):
    EXTRACTED_COMMIT = "extracted_commit"
    EXTRACTED_PATCH = "extracted_patch"
    EXTRACTED_PATCH_REWRITE = "extracted_patch_rewrite"
    PULL_REQUEST = "pull_request"


class ExtractedCommit(CoreModel):
    repo_id: int
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

    def get_id(self):
        return (self.repo_id, self.commit_id)


class Langtype(Enum):
    UNKNOWN = 0
    PROGRAMMING = 1
    MARKUP = 2
    PROSE = 3
    DATA = 4


class ExtractedPatch(BaseModel):
    repo_id: int
    commit_id: str
    parent_commit_id: str
    status: str
    newpath: str
    oldpath: str
    newsize: int
    oldsize: int
    is_binary: bool
    lang: str
    langtype: Langtype

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


class ExtractedPatchRewrite(BaseModel):
    repo_id: int
    commit_id: str
    atime: datetime
    aemail: str
    newpath: str
    rewritten_commit_id: str
    rewritten_atime: datetime
    rewritten_aemail: str
    loc_d: int
