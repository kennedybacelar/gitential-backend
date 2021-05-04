from typing import Set, List, Optional
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class AuthorAlias(CoreModel):
    name: str
    email: str


class AuthorBase(ExtraFieldMixin, CoreModel):
    active: bool
    name: Optional[str]
    email: Optional[str]
    aliases: List[AuthorAlias]

    @property
    def all_emails(self) -> Set[str]:
        emails = {a.email for a in self.aliases}
        if self.email not in emails and self.email:
            emails.add(self.email)
        return emails


class AuthorCreate(AuthorBase):
    pass


class AuthorUpdate(AuthorBase):
    pass


class AuthorInDB(IDModelMixin, DateTimeModelMixin, AuthorBase):
    pass


class AuthorPublic(AuthorInDB):
    pass
