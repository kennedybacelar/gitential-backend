from typing import List, Optional
from .common import CoreModel, IDModelMixin, DateTimeModelMixin, ExtraFieldMixin


class AuthorAlias(CoreModel):
    name: str
    email: str


class AuthorBase(ExtraFieldMixin, CoreModel):
    active: bool
    name: Optional[str]
    email: Optional[str]
    aliases: List[AuthorAlias]


class AuthorCreate(AuthorBase):
    pass


class AuthorUpdate(AuthorBase):
    pass


class AuthorInDB(IDModelMixin, DateTimeModelMixin, AuthorBase):
    pass


class AuthorPublic(AuthorInDB):
    pass