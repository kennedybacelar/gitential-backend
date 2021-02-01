from typing import Optional
from pydantic import Field
from .common import CoreModel, ExtraFieldMixin, IDModelMixin, DateTimeModelMixin
from .userinfos import UserInfoBase


class UserBase(ExtraFieldMixin, CoreModel):
    login: str = Field(..., max_length=128)
    email: str = Field(..., max_length=256)
    is_admin: bool = False
    tc_consent_accepted_at: Optional[str] = None
    marketing_consent_accepted: bool = False

    @classmethod
    def from_user_info(cls, user_info: UserInfoBase):
        return cls(login=user_info.sub, email=user_info.email)


class UserCreate(UserBase):
    pass


class UserUpdate(UserBase):
    login: Optional[str]
    email: Optional[str]


class UserInDB(IDModelMixin, DateTimeModelMixin, UserBase):
    pass


class UserPublic(IDModelMixin, DateTimeModelMixin, UserBase):
    pass
