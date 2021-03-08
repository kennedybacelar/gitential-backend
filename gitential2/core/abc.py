# from typing import List, Optional, Tuple
# from abc import ABC, abstractmethod
# from gitential2.license import License
# from gitential2.secrets import Fernet
# from gitential2.settings import GitentialSettings
# from gitential2.backends import GitentialBackend
# from gitential2.kvstore import KeyValueStore

# from gitential2.datatypes.users import UserCreate, UserUpdate, UserInDB
# from gitential2.datatypes.subscriptions import SubscriptionInDB
# from gitential2.datatypes.repositories import RepositoryInDB, RepositoryStatus
# from gitential2.datatypes.projects import ProjectStatus
# from gitential2.datatypes.workspacemember import MemberInvite
# from gitential2.datatypes.stats import StatsRequest

# from gitential2.datatypes import (
#     WorkspacePublic,
#     WorkspaceCreate,
#     ProjectInDB,
#     ProjectCreateWithRepositories,
#     RepositoryCreate,
#     WorkspaceMemberPublic,
#     WorkspaceMemberInDB,
#     WorkspaceUpdate,
#     WorkspaceInDB,
# )
# from gitential2.datatypes.permissions import Entity, Action


# class WorkspaceCtrl(ABC):
#     @abstractmethod
#     def initialize(self):
#         pass

#     # Manage Projects

#     @abstractmethod
#     def list_projects(self) -> List[ProjectInDB]:
#         pass

#     @abstractmethod
#     def create_project(self, project_create: ProjectCreateWithRepositories) -> ProjectInDB:
#         pass

#     @abstractmethod
#     def get_project(self, project_id: int) -> ProjectInDB:
#         pass

#     @abstractmethod
#     def delete_project(self, project_id: int) -> bool:
#         pass

#     # Repository related actions

#     @abstractmethod
#     def list_connected_repository_sources(self) -> List[str]:
#         pass

#     @abstractmethod
#     def list_available_repositories(self) -> List[RepositoryCreate]:
#         pass

#     @abstractmethod
#     def list_project_repositories(self, project_id: int) -> List[RepositoryInDB]:
#         pass

#     @abstractmethod
#     def search_public_repositories(self, search: str) -> List[RepositoryCreate]:
#         pass

#     @abstractmethod
#     def list_repositories(self) -> List[RepositoryInDB]:
#         pass

#     # Schedule project and repository refresh, calculations

#     @abstractmethod
#     def schedule_project_refresh(self, project_id: int):
#         pass

#     @abstractmethod
#     def schedule_repository_refresh(self, repo_id: int):
#         pass

#     @abstractmethod
#     def refresh_repository(self, repository_id: int):
#         pass

#     @abstractmethod
#     def refresh_repository_commits(self, repository_id: int):
#         pass

#     @abstractmethod
#     def refresh_repository_pull_requests(self, repository_id: int):
#         pass

#     @abstractmethod
#     def get_project_status(self, project_id: int) -> ProjectStatus:
#         pass

#     @abstractmethod
#     def get_repo_status(self, repository_id: int) -> RepositoryStatus:
#         pass

#     @abstractmethod
#     def update_repository_status(self, repository_id: int, **kwargs) -> RepositoryStatus:
#         pass

#     @abstractmethod
#     def calculate_stats(self, request: StatsRequest):
#         pass

#     @abstractmethod
#     def recalculate_repository_values(self, repository_id: int):
#         pass


# class Gitential(ABC):
#     @property
#     @abstractmethod
#     def settings(self) -> GitentialSettings:
#         pass

#     # User mgmt
#     @abstractmethod
#     def get_user(self, user_id) -> Optional[UserInDB]:
#         pass

#     def create_user(self, user: UserCreate) -> UserInDB:
#         pass

#     def update_user(self, user: UserUpdate) -> UserInDB:
#         pass

#     def delete_user(self, user_id: int) -> int:
#         pass

#     @abstractmethod
#     def register_user(self, user: UserCreate) -> Tuple[UserInDB, SubscriptionInDB]:
#         pass

#     @property
#     @abstractmethod
#     def backend(self) -> GitentialBackend:
#         pass

#     @property
#     @abstractmethod
#     def integrations(self) -> dict:
#         pass

#     @property
#     @abstractmethod
#     def fernet(self) -> Fernet:
#         pass

#     @property
#     @abstractmethod
#     def kvstore(self) -> KeyValueStore:
#         pass

#     @property
#     @abstractmethod
#     def license(self) -> License:
#         pass

#     # Authorization flow

#     @abstractmethod
#     def handle_authorize(self, integration_name: str, token, user_info: dict, current_user: Optional[UserInDB] = None):
#         pass

#     # Workspace management

#     @abstractmethod
#     def create_workspace(self, workspace: WorkspaceCreate, current_user: UserInDB, primary=False) -> WorkspaceInDB:
#         pass

#     @abstractmethod
#     def update_workspace(self, workspace_id: int, workspace: WorkspaceUpdate, current_user: UserInDB) -> WorkspaceInDB:
#         pass

#     @abstractmethod
#     def delete_workspace(self, workspace_id: int, current_user: UserInDB) -> int:
#         pass

#     @abstractmethod
#     def get_accessible_workspaces(
#         self, current_user: UserInDB, include_members: bool = False, include_projects: bool = False
#     ) -> List[WorkspacePublic]:
#         pass

#     @abstractmethod
#     def get_workspace(
#         self,
#         workspace_id: int,
#         current_user: UserInDB,
#         include_members: bool = False,
#         include_projects: bool = False,
#         _membership: Optional[WorkspaceMemberInDB] = None,
#     ):
#         pass

#     @abstractmethod
#     def get_workspace_ctrl(self, workspace_id: int) -> WorkspaceCtrl:
#         pass

#     # Manage members

#     @abstractmethod
#     def get_members(self, workspace_id: int, include_user_header=True) -> List[WorkspaceMemberPublic]:
#         pass

#     @abstractmethod
#     def invite_members(self, workspace_id: int, invitations: List[MemberInvite]) -> int:
#         pass

#     @abstractmethod
#     def remove_member(self, workspace_id: int, workspace_member_id: int) -> int:
#         pass

#     @abstractmethod
#     def check_permission(self, current_user, entity: Entity, action: Action, **kwargs) -> bool:
#         pass
