from typing import Optional
from .common import IDModelMixin, CoreModel


class TeamMemberBase(CoreModel):
    team_id: Optional[int] = None
    author_id: Optional[int] = None


class TeamMemberCreate(TeamMemberBase):
    team_id: int
    author_id: int


class TeamMemberUpdate(TeamMemberBase):
    team_id: int
    author_id: int


class TeamMemberInDB(IDModelMixin, TeamMemberBase):
    team_id: int
    author_id: int