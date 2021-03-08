from typing import List
from .common import CoreModel, IDModelMixin, DateTimeModelMixin


class AuthorAlias(CoreModel):
    name: str
    email: str


class AuthorBase(CoreModel):
    active: bool
    aliases: List[AuthorAlias]


class AuthorCreate(AuthorBase):
    pass


class AuthorUpdate(AuthorBase):
    pass


class AuthorInDB(IDModelMixin, DateTimeModelMixin, AuthorBase):
    pass
