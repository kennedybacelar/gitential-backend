from datetime import date
from typing import Set, List, Optional, Tuple

from pydantic import BaseModel

from gitential2.datatypes.export import ExportableModel
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class AuthorAlias(CoreModel):
    name: Optional[str] = None
    email: Optional[str] = None
    login: Optional[str] = None

    def is_empty(self):
        return (not self.name) and (not self.email) and (not self.login)


class AuthorBase(ExtraFieldMixin, CoreModel):
    active: bool
    name: Optional[str]
    email: Optional[str]
    aliases: List[AuthorAlias]

    @property
    def all_emails(self) -> Set[str]:
        emails = {a.email for a in self.aliases if a.email}
        if self.email not in emails and self.email:
            emails.add(self.email)
        return emails


class AuthorCreate(AuthorBase):
    pass


class AuthorUpdate(AuthorBase):
    pass


class AuthorInDB(IDModelMixin, DateTimeModelMixin, AuthorBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "author", "authors"

    def export_fields(self) -> List[str]:
        return ["id", "created_at", "updated_at", "active", "name", "email", "aliases"]


class AuthorPublic(AuthorInDB):
    pass


class IdAndTitle(BaseModel):
    id: int
    title: str


class AuthorPublicExtended(AuthorInDB):
    teams: Optional[List[IdAndTitle]]
    projects: Optional[List[IdAndTitle]]


class AuthorsPublicExtendedSearchResult(BaseModel):
    total: Optional[int] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    authors_list: List[AuthorPublicExtended]


class DateRange(BaseModel):
    start: date
    end: date


class AuthorFilters(BaseModel):
    limit: Optional[int] = 5
    offset: Optional[int] = 0
    sort_by_name_is_desc: Optional[bool] = False
    date_range: Optional[DateRange] = None
    developer_names: Optional[List[str]] = []
    developer_emails: Optional[List[str]] = []
    developer_ids: Optional[List[int]] = []
    project_ids: Optional[List[int]] = []
    team_ids: Optional[List[int]] = []
    repository_ids: Optional[List[int]] = []
