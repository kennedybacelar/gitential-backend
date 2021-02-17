from typing import List, Optional
from abc import ABC, abstractmethod

from gitential2.secrets import Fernet
from gitential2.settings import GitentialSettings
from gitential2.backends import GitentialBackend

from gitential2.datatypes.repositories import RepositoryInDB


from gitential2.datatypes import (
    UserInDB,
    WorkspacePublic,
    WorkspaceCreate,
    ProjectInDB,
    ProjectCreateWithRepositories,
    RepositoryCreate,
    WorkspaceMemberPublic,
    WorkspaceMemberInDB,
    WorkspaceUpdate,
    WorkspaceInDB,
)


class WorkspaceCtrl(ABC):
    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def list_projects(self) -> List[ProjectInDB]:
        pass

    @abstractmethod
    def create_project(self, project_create: ProjectCreateWithRepositories) -> ProjectInDB:
        pass

    @abstractmethod
    def get_project(self, project_id: int) -> ProjectInDB:
        pass

    @abstractmethod
    def delete_project(self, project_id: int) -> bool:
        pass

    @abstractmethod
    def list_connected_repository_sources(self) -> List[str]:
        pass

    @abstractmethod
    def list_available_repositories(self) -> List[RepositoryCreate]:
        pass

    @abstractmethod
    def list_project_repositories(self, project_id: int) -> List[RepositoryInDB]:
        pass

    @abstractmethod
    def schedule_project_refresh(self, project_id: int):
        pass

    @abstractmethod
    def schedule_repository_refresh(self, repo_id: int):
        pass

    @abstractmethod
    def refresh_repository(self, repository_id: int):
        pass


class Gitential(ABC):
    @property
    @abstractmethod
    def settings(self) -> GitentialSettings:
        pass

    @property
    @abstractmethod
    def backend(self) -> GitentialBackend:
        pass

    @property
    @abstractmethod
    def integrations(self) -> dict:
        pass

    @property
    @abstractmethod
    def fernet(self) -> Fernet:
        pass

    @abstractmethod
    def handle_authorize(self, integration_name: str, token, user_info: dict, current_user: Optional[UserInDB] = None):
        pass

    @abstractmethod
    def create_workspace(self, workspace: WorkspaceCreate, current_user: UserInDB, primary=False) -> WorkspaceInDB:
        pass

    @abstractmethod
    def update_workspace(self, workspace_id: int, workspace: WorkspaceUpdate, current_user: UserInDB) -> WorkspaceInDB:
        pass

    @abstractmethod
    def delete_workspace(self, workspace_id: int, current_user: UserInDB) -> int:
        pass

    @abstractmethod
    def get_accessible_workspaces(
        self, current_user: UserInDB, include_members: bool = False, include_projects: bool = False
    ) -> List[WorkspacePublic]:
        pass

    def get_workspace(
        self,
        workspace_id: int,
        current_user: UserInDB,
        include_members: bool = False,
        include_projects: bool = False,
        _membership: Optional[WorkspaceMemberInDB] = None,
    ):
        pass

    @abstractmethod
    def get_workspace_ctrl(self, workspace_id: int) -> WorkspaceCtrl:
        pass

    @abstractmethod
    def get_members(self, workspace_id: int, include_user_header=True) -> List[WorkspaceMemberPublic]:
        pass
