from enum import Enum
from typing import Optional
from datetime import datetime
from .common import IDModelMixin, DateTimeModelMixin, CoreModel, ExtraFieldMixin


class CredentialType(str, Enum):
    token = "token"
    keypair = "keypair"
    passphrase = "passphrase"


class CredentialBasePublic(ExtraFieldMixin, CoreModel):
    owner_id: Optional[int] = None
    type: Optional[CredentialType] = None
    integration_name: Optional[str] = None
    integration_type: Optional[str] = None
    name: Optional[str] = None

    expires_at: Optional[datetime] = None


class CredentialBaseSecret(CoreModel):
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    public_key: Optional[str] = None
    private_key: Optional[str] = None
    passphrase: Optional[str] = None


class CredentialBase(CredentialBasePublic, CredentialBaseSecret):
    @classmethod
    def from_token(cls, token: dict, integration_name: Optional[str] = None, integration_type: Optional[str] = None):
        return cls(
            type=CredentialType.token,
            integration_name=integration_name,
            integration_type=integration_type,
            name=f"{integration_name} token",
            token=token["access_token"],
            refresh_token=token.get("refresh_token"),
            expires_at=token.get("expires_at"),
        )


class CredentialCreate(CredentialBase):
    owner_id: int
    type: CredentialType
    name: str


class CredentialUpdate(CredentialBase):
    pass


class CredentialInDB(IDModelMixin, DateTimeModelMixin, CredentialBase):
    pass


class CredentialPublic(IDModelMixin, DateTimeModelMixin, CredentialBasePublic):
    pass
