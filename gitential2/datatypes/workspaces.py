from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel


class WorkspaceBase(BaseModel):
    name: Optional[str] = None
    owner_id: Optional[int] = None
    created_at: Optional[datetime] = None


class WorkspaceCreate(WorkspaceBase):
    name: str


class WorkspaceUpdate(WorkspaceBase):
    pass


class WorkspaceInDbBase(WorkspaceBase):
    id: int
    name: str
    owner_id: int
    created_at: datetime = None

    class Config:
        orm_mode = True


class Workspaces(WorkspaceInDbBase):
    workspaces: List[WorkspaceInDbBase]


class Workspace(WorkspaceInDbBase):
    pass
