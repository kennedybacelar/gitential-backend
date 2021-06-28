from typing import Set, List, Optional, Tuple
from gitential2.datatypes.export import ExportableModel

from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class AuthorAlias(CoreModel):
    name: Optional[str] = None
    email: Optional[str] = None
    login: Optional[str] = None


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
        return ("author", "authors")

    def export_fields(self) -> List[str]:
        return ["id", "created_at", "updated_at", "active", "name", "email", "aliases"]


class AuthorPublic(AuthorInDB):
    pass
