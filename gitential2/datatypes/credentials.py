from enum import Enum
from typing import Optional, cast
from datetime import datetime
from gitential2.secrets import Fernet
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
    token: Optional[bytes] = None
    refresh_token: Optional[bytes] = None
    public_key: Optional[bytes] = None
    private_key: Optional[bytes] = None
    passphrase: Optional[bytes] = None


class CredentialBase(CredentialBasePublic, CredentialBaseSecret):
    # pylint: disable=too-many-arguments
    @classmethod
    def from_token(
        cls,
        token: dict,
        fernet: Fernet,
        owner_id: int,
        integration_name: Optional[str] = None,
        integration_type: Optional[str] = None,
    ):
        return cls(
            type=CredentialType.token,
            integration_name=integration_name,
            integration_type=integration_type,
            owner_id=owner_id,
            name=f"{integration_name} token",
            token=fernet.encrypt_string(token["access_token"]).encode(),
            refresh_token=fernet.encrypt_string(cast(str, token.get("refresh_token"))).encode()
            if token.get("refresh_token")
            else None,
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
