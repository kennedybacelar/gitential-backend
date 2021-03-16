from datetime import date
from typing import Optional, List
from .common import CoreModel, IDModelMixin, DateTimeModelMixin
from .authors import AuthorInDB


class SprintBase(CoreModel):
    date: date
    weeks: int
    pattern: str


class Sprint(SprintBase):
    pass


class TeamBase(CoreModel):
    name: str
    sprints_enabled: bool = False
    sprint: Optional[Sprint] = None


class TeamCreate(TeamBase):
    pass


class TeamUpdate(TeamBase):
    pass


class TeamInDB(IDModelMixin, DateTimeModelMixin, TeamBase):
    pass


class TeamPublic(TeamInDB):
    pass


class TeamWithAuthors(TeamInDB):
    authors: List[AuthorInDB]


class TeamPublicWithAuthors(TeamPublic):
    authors: List[AuthorInDB]