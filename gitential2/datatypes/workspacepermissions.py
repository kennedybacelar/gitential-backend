from enum import Enum
from typing import Optional
from .common import IDModelMixin, DateTimeModelMixin, CoreModel


class WorkspaceRole(Enum):
    owner = 1
    collaborator = 2


class WorkspacePermissionBase(CoreModel):
    user_id: Optional[int] = None
    workspace_id: Optional[int] = None
    role: WorkspaceRole = WorkspaceRole.owner
    primary: bool = False


class WorkspacePermissionCreate(WorkspacePermissionBase):
    user_id: int
    workspace_id: int


class WorkspacePermissionUpdate(WorkspacePermissionBase):
    pass


class WorkspacePermissionInDB(IDModelMixin, DateTimeModelMixin, WorkspacePermissionBase):
    pass


class WorkspacePermissionPublic(IDModelMixin, DateTimeModelMixin, WorkspacePermissionBase):
    pass


class WorkspaceWithPermission(CoreModel):
    id: int
    name: str
    role: WorkspaceRole
    primary: bool
    user_id: int
