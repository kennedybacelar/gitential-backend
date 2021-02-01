from typing import Optional, List
from pydantic import BaseModel, constr

from .common import IDModelMixin, CoreModel, DateTimeModelMixin
from .repositories import RepositoryCreate, RepositoryPublic


class ProjectBase(CoreModel):
    name: Optional[str]
    shareable: bool = False
    pattern: Optional[str] = None


class ProjectCreate(ProjectBase):
    name: constr(min_length=2, max_length=128)


class ProjectCreateWithRepositories(ProjectCreate):
    repositories: List[RepositoryCreate]


class ProjectUpdate(ProjectBase):
    pass


class ProjectUpdateWithRepositories(ProjectUpdate):
    repositories: List[RepositoryCreate]


class ProjectInDB(IDModelMixin, DateTimeModelMixin, ProjectBase):
    pass


class ProjectPublic(IDModelMixin, DateTimeModelMixin, ProjectBase):
    pass


class ProjectPublicWithRepositories(ProjectPublic):
    repositories: List[RepositoryPublic]
