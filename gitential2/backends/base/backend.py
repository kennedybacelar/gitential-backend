from abc import ABC, abstractmethod
from gitential2.settings import GitentialSettings

from .repositories import (
    UserRepository,
    UserInfoRepository,
    CredentialRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
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
    def workspace_members(self) -> WorkspaceMemberRepository:
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
    def initialize_workspace(self, workspace_id: int):
        pass
