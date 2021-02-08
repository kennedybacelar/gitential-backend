from typing import List, Optional, cast

# from uuid import UUID
from gitential2.settings import GitentialSettings
from gitential2.integrations import init_integrations


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
    WorkspacePublic,
    WorkspacePermissionPublic,
    WorkspaceWithPermission,
    WorkspaceCreate,
    WorkspacePermissionCreate,
    WorkspaceRole,
)


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
        # tbd: update or create credentials based on integration and user

        # Create workspace if new_user
        if is_new_user:
            self._create_primary_workspace_for(user)
        return {"ok": True, "user": user}

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
        return workspace_in_db

    def get_accessible_workspaces(self, current_user: UserInDB) -> List[WorkspaceWithPermission]:
        return self.backend.get_accessible_workspaces(current_user.id)


#         # def _get_or_create_user(current_user, normalize_userinfo):
#         #     if current_user and existing_user and current_user.id != existing_user.id:
#         #         raise ValueError("Authentication error...")
#         #     elif

#         if not current_user and not existing_user:
#             # create new user
#             user = self.backend.users.create_or_update(UserCreate.from_user_info(normalized_user_info))
#             user_info = self.backend.user_infos.create_or_update(normalized_user_info)

#         # if current user and existing_user and current_user == existing user
#         # update token, user, userinfo
#         # elif current_user and existing_user and current_user != existoin user
#         # authentication error
#         # elif current_user and !existing_user:
#         # add new credentials to current_user
#         # elif !current_user and existing_user:
#         # add new credentials to existing user and log in
#         # elif !current_user and !existing_user
#         # create new user and new

#         is_new_user = False
#         user = current_user or self.backend.get_user(user_info=normalized_user_info)

#         if not user:
#             user = self.backend.upsert_user(User.from_user_info(user_info=normalized_user_info))
#             is_new_user = True

#         normalized_user_info.user_id = user.id
#         self.backend.upsert_user_info(normalized_user_info)

#         credential = Credential.from_token(token, integration_name=integration_name)
#         credential.owner_id = user.id
#         credential = self.backend.upsert_credential(credential)

#         if is_new_user:
#             workspace = self.backend.upsert_workspace(Workspace(name=f"{user.login}", owner_id=user.id))

#             self.backend.upsert_workspace_permission(
#                 WorkspacePermission(user_id=user.id, workspace_id=workspace.id, role=WorkspaceRole.owner, primary=True)
#             )

#         # integration = self.integrations[integration_name]
#         # if integration_name == "gitlab-internal":

#         #     repositories = integration.list_available_private_repositories(token=token, update_token=print)
#         #     print("!!!", repositories)
#         #     new_token = integration.refresh_token(token)
#         #     normalized_user_info = integration.normalize_userinfo(user_info)

#         # return {
#         #     "token": token,
#         #     "user_info": user_info,
#         #     "new_token": new_token,
#         #     "normalized_user_info": normalized_user_info,
#         # }
#         return {"ok": True, "user": user}

#     def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
#         return self.backend.get_accessible_workspaces(user_id)

#     def create_workspace(self, workspace):
#         pass

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


# # class Workspace:
# #     pass


# # class WorkspaceBackend:
# #     pass


# # class Credential:
# #     pass


# # class Project:
# #     pass


# # class Repository:
# #     pass


# # class RepositorySource:
# #     pass


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
