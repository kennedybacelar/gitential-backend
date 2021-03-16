from typing import Optional
from pydantic import Field
from .common import CoreModel, ExtraFieldMixin, IDModelMixin, DateTimeModelMixin
from .userinfos import UserInfoBase


class UserBase(ExtraFieldMixin, CoreModel):
    login: Optional[str] = Field(None, max_length=128)
    email: Optional[str] = Field(None, max_length=256)
    is_admin: bool = False
    marketing_consent_accepted: bool = False
    first_name: Optional[str] = Field(None, max_length=256)
    last_name: Optional[str] = Field(None, max_length=256)
    company_name: Optional[str] = Field(None, max_length=256)
    position: Optional[str] = Field(None, max_length=256)
    development_team_size: Optional[str] = Field(None, max_length=32)
    registration_ready: bool = False
    login_ready: bool = False
    is_active: bool = True

    @classmethod
    def from_user_info(cls, user_info: UserInfoBase):
        return cls(login=user_info.preferred_username or user_info.sub, email=user_info.email)


class UserCreate(UserBase):
    email: str


class UserUpdate(UserBase):
    pass


class UserInDB(IDModelMixin, DateTimeModelMixin, UserBase):
    pass


class UserPublic(IDModelMixin, DateTimeModelMixin, UserBase):
    pass


class UserHeader(IDModelMixin, CoreModel):
    login: Optional[str] = Field(None, max_length=128)
