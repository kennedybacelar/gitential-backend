# pylint: disable=unsubscriptable-object
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic, List
from pydantic import BaseModel
from gitential2.settings import GitentialSettings
from gitential2.datatypes import (
    UserCreate,
    UserUpdate,
    UserInDB,
    WorkspaceCreate,
    WorkspaceUpdate,
    WorkspaceInDB,
    UserInfoCreate,
    UserInfoUpdate,
    UserInfoInDB,
    CredentialCreate,
    CredentialUpdate,
    CredentialInDB,
    WorkspacePermissionCreate,
    WorkspacePermissionUpdate,
    WorkspacePermissionInDB,
    WorkspaceWithPermission,
)


IdType = TypeVar("IdType")
CreateType = TypeVar("CreateType", bound=BaseModel)
UpdateType = TypeVar("UpdateType", bound=BaseModel)
InDBType = TypeVar("InDBType", bound=BaseModel)


class BaseRepository(ABC, Generic[IdType, CreateType, UpdateType, InDBType]):
    @abstractmethod
    def get(self, id_: IdType) -> Optional[InDBType]:
        pass

    @abstractmethod
    def create(self, obj: CreateType) -> InDBType:
        pass

    @abstractmethod
    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        pass

    @abstractmethod
    def delete(self, id_: IdType) -> int:
        pass


class UserRepository(BaseRepository[int, UserCreate, UserUpdate, UserInDB]):
    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        pass


class UserInfoRepository(BaseRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    @abstractmethod
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        pass


class CredentialRepository(BaseRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]):
    @abstractmethod
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        pass


class WorkspaceRepository(BaseRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    pass


class WorkspacePermissionRepository(
    BaseRepository[int, WorkspacePermissionCreate, WorkspacePermissionUpdate, WorkspacePermissionInDB]
):
    @abstractmethod
    def get_for_user(self, user_id: int) -> List[WorkspacePermissionInDB]:
        pass


class GitentialBackend(ABC):
    def __init__(self, settings: GitentialSettings):
        self.settings = settings

    @property
    @abstractmethod
    def users(self) -> UserRepository:
        pass

    @property
    @abstractmethod
    def user_infos(self) -> UserInfoRepository:
        pass

    @property
    @abstractmethod
    def credentials(self) -> CredentialRepository:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> WorkspaceRepository:
        pass

    @property
    @abstractmethod
    def workspace_permissions(self) -> WorkspacePermissionRepository:
        pass

    @abstractmethod
    def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
        pass
