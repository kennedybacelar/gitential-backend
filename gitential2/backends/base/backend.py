from abc import ABC, abstractmethod
from typing import List
from gitential2.settings import GitentialSettings
from gitential2.datatypes import (
    WorkspaceWithPermission,
)
from .repositories import (
    UserRepository,
    UserInfoRepository,
    CredentialRepository,
    WorkspaceRepository,
    WorkspacePermissionRepository,
    ProjectRepository,
    RepositoryRepository,
    ProjectRepositoryRepository,
)


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

    @property
    @abstractmethod
    def projects(self) -> ProjectRepository:
        pass

    @property
    @abstractmethod
    def repositories(self) -> RepositoryRepository:
        pass

    @property
    @abstractmethod
    def project_repositories(self) -> ProjectRepositoryRepository:
        pass

    @abstractmethod
    def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
        pass

    @abstractmethod
    def initialize_workspace(self, workspace_id: int):
        pass
