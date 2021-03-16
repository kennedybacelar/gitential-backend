from enum import Enum
from typing import Optional, List
from pydantic import Field

from .common import IDModelMixin, CoreModel, DateTimeModelMixin, ExtraFieldMixin
from .repositories import RepositoryCreate, RepositoryPublic, RepositoryStatus


class ProjectExportDatatype(str, Enum):
    commits = "commits"
    patches = "patches"
    pull_requests = "pull-requests"


class ProjectBase(ExtraFieldMixin, CoreModel):
    name: Optional[str]
    shareable: bool = False
    pattern: Optional[str] = None


class ProjectCreate(ProjectBase):
    name: str = Field(..., min_length=2, max_length=128)


class ProjectCreateWithRepositories(ProjectCreate):
    repos: List[RepositoryCreate]


class ProjectUpdate(ProjectBase):
    pass


class ProjectUpdateWithRepositories(ProjectUpdate):
    repos: List[RepositoryCreate]


class ProjectInDB(IDModelMixin, DateTimeModelMixin, ProjectBase):
    pass


class ProjectPublic(IDModelMixin, DateTimeModelMixin, ProjectBase):
    pass


class ProjectPublicWithRepositories(ProjectPublic):
    repos: List[RepositoryPublic]


class ProjectStatus(CoreModel):
    id: int
    name: str
    status: str
    done: bool
    repos: List[RepositoryStatus]
