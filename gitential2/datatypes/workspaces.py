from typing import Optional
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class WorkspaceBase(ExtraFieldMixin, CoreModel):
    name: Optional[str] = None
    owner_id: Optional[int] = None


class WorkspaceCreate(WorkspaceBase):
    name: str
    owner_id: int


class WorkspaceUpdate(WorkspaceBase):
    pass


class WorkspaceInDB(IDModelMixin, DateTimeModelMixin, WorkspaceBase):
    pass


class WorkspacePublic(IDModelMixin, DateTimeModelMixin, WorkspaceBase):
    pass
