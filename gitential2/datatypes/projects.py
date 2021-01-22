from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
from .repositories import GitRepository


class Project(BaseModel):
    id: int
    name: str
    shareable: bool
    pattern: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class ProjectDetail(Project):
    repositories: List[GitRepository]
