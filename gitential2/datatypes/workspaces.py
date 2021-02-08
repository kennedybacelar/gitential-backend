from typing import Optional
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class WorkspaceBase(ExtraFieldMixin, CoreModel):
    name: Optional[str] = None
    created_by: Optional[int] = None


class WorkspaceCreate(WorkspaceBase):
    name: str


class WorkspaceUpdate(WorkspaceBase):
    pass


class WorkspaceInDB(IDModelMixin, DateTimeModelMixin, WorkspaceBase):
    name: str
    created_by: int


class WorkspacePublic(IDModelMixin, DateTimeModelMixin, WorkspaceBase):
    pass
