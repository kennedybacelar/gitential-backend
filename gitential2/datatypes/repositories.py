from typing import Optional, Union, Dict
from pydantic import BaseModel, Field

from .common import IDModelMixin, DateTimeModelMixin, CoreModel, ExtraFieldMixin


class RepositoryBase(ExtraFieldMixin, CoreModel):
    clone_url: str
    name: str = ""
    namespace: str = ""
    private: bool = False
    source: str = ""


class RepositoryCreate(RepositoryBase):
    pass


class RepositoryUpdate(RepositoryBase):
    pass


class RepositoryInDB(IDModelMixin, DateTimeModelMixin, RepositoryBase):
    pass


class RepositoryPublic(IDModelMixin, DateTimeModelMixin, RepositoryBase):
    pass


class GitRepository(BaseModel):
    id: Optional[int] = None
    clone_url: str
    name: str = ""
    namespace: str = ""
    private: bool = False
    source: str = ""
    extra: Dict[str, Union[str, int, float]] = Field(default_factory=dict)

    @property
    def repo_id(self):
        return self.id


class GitRepositoryState(BaseModel):
    branches: Dict[str, str]
    tags: Dict[str, str]

    @property
    def commit_ids(self):
        return list(self.branches.values()) + list(self.tags.values())


class GitRepositoryStateChange(BaseModel):
    old_state: GitRepositoryState
    new_state: GitRepositoryState

    @property
    def new_branches(self):
        return {b: cid for b, cid in self.new_state.branches.items() if b not in self.old_state.branches}
