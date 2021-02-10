from typing import List, Optional, cast
from abc import ABC, abstractmethod

# from uuid import UUID
from gitential2.settings import GitentialSettings
from gitential2.integrations import init_integrations, REPOSITORY_SOURCES


from gitential2.backends import init_backend, GitentialBackend
from gitential2.secrets import Fernet
from gitential2.datatypes import (
    UserPublic,
    UserCreate,
    UserInDB,
    UserInfoPublic,
    UserInfoUpdate,
    UserInfoCreate,
    CredentialCreate,
    CredentialUpdate,
    CredentialInDB,
    WorkspacePublic,
    WorkspaceInDB,
    WorkspacePermissionPublic,
    WorkspaceWithPermission,
    WorkspaceCreate,
    WorkspacePermissionCreate,
    WorkspaceRole,
    ProjectInDB,
    ProjectCreateWithRepositories,
    RepositoryCreate,
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
    def list_connected_repository_sources(self) -> List[str]:
        pass

    @abstractmethod
    def list_available_repositories(self) -> List[RepositoryCreate]:
        pass


class Gitential:
    def __init__(self, settings: GitentialSettings, integrations: dict, backend: GitentialBackend, fernet: Fernet):
        self.settings = settings
        self.integrations = integrations
        self.backend = backend
        self.fernet = fernet

    @classmethod
    def from_config(cls, settings: GitentialSettings):
        integrations = init_integrations(settings)
        backend: GitentialBackend = init_backend(settings)
        fernet = Fernet(settings)
        return cls(settings=settings, integrations=integrations, backend=backend, fernet=fernet)

    def handle_authorize(self, integration_name: str, token, user_info: dict, current_user: Optional[UserInDB] = None):
        integration = self.integrations[integration_name]

        # normalize the userinfo
        normalized_userinfo: UserInfoCreate = integration.normalize_userinfo(user_info, token=token)

        # update or create a user and the proper user_info in backend
        user, user_info, is_new_user = self._create_or_update_user_and_user_info(normalized_userinfo, current_user)

        # update or create credentials based on integration and user
        self._create_or_update_credential_from(user, integration_name, integration.integration_type, token)

        # Create workspace if new_user
        if is_new_user:
            self._create_primary_workspace_for(user)
        return {"ok": True, "user": user, "user_info": user_info, "is_new_user": is_new_user}

    def _create_or_update_user_and_user_info(
        self, normalized_userinfo: UserInfoCreate, current_user: Optional[UserInDB] = None
    ):
        existing_userinfo = self.backend.user_infos.get_by_sub_and_integration(
            sub=normalized_userinfo.sub, integration_name=normalized_userinfo.integration_name
        )
        if existing_userinfo:
            if current_user and existing_userinfo.user_id != current_user.id:
                raise ValueError("Authentication error...")

            user = self.backend.users.get(existing_userinfo.user_id)
            user_info = self.backend.user_infos.update(existing_userinfo.id, cast(UserInfoUpdate, normalized_userinfo))
            return user, user_info, False
        else:
            user = current_user or self.backend.users.create(UserCreate.from_user_info(normalized_userinfo))
            user_info_data = normalized_userinfo.dict(exclude_none=True)
            user_info_data.setdefault("user_id", user.id)
            user_info = self.backend.user_infos.create(normalized_userinfo.copy(update={"user_id": user.id}))
            return user, user_info, current_user is None

    def _create_or_update_credential_from(
        self, user: UserInDB, integration_name: str, integration_type: str, token: dict
    ):
        new_credential = CredentialCreate.from_token(
            token=token,
            fernet=self.fernet,
            owner_id=user.id,
            integration_name=integration_name,
            integration_type=integration_type,
        )

        existing_credential = self.backend.credentials.get_by_user_and_integration(
            owner_id=user.id, integration_name=integration_name
        )
        if existing_credential:
            self.backend.credentials.update(id_=existing_credential.id, obj=CredentialUpdate(**new_credential.dict()))
        else:
            self.backend.credentials.create(new_credential)

    def _create_primary_workspace_for(self, user: UserInDB):
        workspace = WorkspaceCreate(name=f"{user.login}'s workspace")
        return self.create_workspace(workspace, current_user=user, primary=True)

    def create_workspace(self, workspace: WorkspaceCreate, current_user: UserInDB, primary=False):
        workspace.created_by = current_user.id

        workspace_in_db = self.backend.workspaces.create(workspace)
        self.backend.workspace_permissions.create(
            WorkspacePermissionCreate(
                workspace_id=workspace_in_db.id, user_id=current_user.id, role=WorkspaceRole.owner, primary=primary
            )
        )
        self.get_workspace_ctrl(workspace_id=workspace_in_db.id).initialize()
        return workspace_in_db

    def get_accessible_workspaces(self, current_user: UserInDB) -> List[WorkspaceWithPermission]:
        return self.backend.get_accessible_workspaces(current_user.id)

    def get_workspace_ctrl(self, workspace_id: int) -> WorkspaceCtrl:
        return WorkspaceCtrlImpl(
            id_=workspace_id,
            backend=self.backend,
            core=self,
        )


class WorkspaceCtrlImpl(WorkspaceCtrl):
    def __init__(self, id_: int, backend: GitentialBackend, core: Gitential):
        self._ws: Optional[WorkspaceInDB] = None
        self._id = id_
        self.backend = backend
        self.core = core
        self.ws_backend = backend.initialize_workspace(id_)

    def initialize(self):
        if hasattr(self.ws_backend, "initialize"):
            self.ws_backend.initialize()

    @property
    def workspace(self) -> WorkspaceInDB:
        if self._ws is None:
            self._ws = self.backend.workspaces.get(id_=self._id)
            if self._ws is None:
                raise ValueError(f"Missing workspace: {self._id} ")
        return self._ws

    def get_credentials(self) -> List[CredentialInDB]:
        return self.backend.credentials.get_for_user(self.workspace.created_by)

    def list_projects(self) -> List[ProjectInDB]:
        return []

    def create_project(self, project_create: ProjectCreateWithRepositories) -> ProjectInDB:
        pass

    def list_connected_repository_sources(self) -> List[str]:
        return [
            credential.integration_name
            for credential in self.get_credentials()
            if (
                credential.integration_name
                and credential.integration_type in REPOSITORY_SOURCES
                and credential.integration_name in self.core.integrations
            )
        ]

    def list_available_repositories(self) -> List[RepositoryCreate]:
        def _fixme(*args, **kwargs):
            print("update token called", args, kwargs)

        results: List[RepositoryCreate] = []
        for credential in self.get_credentials():

            if (
                credential.integration_type in REPOSITORY_SOURCES
                and credential.integration_name in self.core.integrations
            ):

                integration = self.core.integrations[credential.integration_name]
                token = credential.to_token_dict(fernet=self.core.fernet)
                results += integration.list_available_private_repositories(token=token, update_token=_fixme)

        return results


#     def update_workspace(self, workspace):
#         pass

#     def delete_workspace(self, workspace):
#         pass

#     def list_workspaces(self):
#         pass

#     def list_users(self):
#         pass

#     def delete_user(self):
#         pass


# # class WorkspaceManager:
# #     def __init__(self, workspace: Workspace, backend: WorkspaceBackend):
# #         self.workspace = workspace
# #         self.backend = backend

# #     def list_repository_sources(self) -> List[RepositorySource]:
# #         pass

# #     def list_projects(self) -> List[Project]:
# #         return []

# #     def list_available_repositories(self) -> List[Repository]:
# #         return []

# #     def get_project(self, project_id: int) -> Project:
# #         pass

# #     def create_project(self, project: Project) -> Project:
# #         pass

# #     def update_project(self, project: Project) -> Project:
# #         pass

# #     def delete_project(self, project_id: int) -> bool:
# #         return False

# #     def list_project_repositories(self, project_id: int) -> List[Repository]:
# #         return []

# #     def analyze_project(self, project_id: int) -> bool:
# #         return True
