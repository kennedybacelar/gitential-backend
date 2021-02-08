import datetime as dt
from threading import Lock
from typing import Optional, Callable, List
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

from .common import (
    BaseRepository,
    IdType,
    CreateType,
    UpdateType,
    InDBType,
    UserRepository,
    UserInfoRepository,
    GitentialBackend,
    WorkspaceRepository,
    WorkspacePermissionRepository,
    CredentialRepository,
)


class InMemRepository(
    BaseRepository[IdType, CreateType, UpdateType, InDBType]
):  # pylint: disable=unsubscriptable-object
    def __init__(self, in_db_cls: Callable[..., InDBType]):
        self._state: dict = {}
        self._counter = 1
        self._in_db_cls = in_db_cls
        self._counter_lock = Lock()

    def get(self, id_: IdType) -> Optional[InDBType]:
        return self._state.get(id_)

    def create(self, obj: CreateType) -> InDBType:
        values = obj.dict()
        id_ = self._new_id()
        values["id"] = id_
        self._state[id_] = self._in_db_cls(**values)
        return self._state[id_]

    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = obj.dict(exclude_unset=True)
        original_obj = self._state[id_]
        if hasattr(original_obj, "updated_at") and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        updated_obj = original_obj.copy(update=update_dict)
        self._state[id_] = updated_obj
        return self._state[id_]

    def delete(self, id_: IdType) -> int:
        try:
            del self._state[id_]
            return 1
        except KeyError:
            return 0

    def _new_id(self):
        with self._counter_lock:
            ret = self._counter
            self._counter += 1
        return ret


class InMemUserRepository(UserRepository, InMemRepository[int, UserCreate, UserUpdate, UserInDB]):
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        return None


class InMemUserInfoRepository(UserInfoRepository, InMemRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        return None


class InMemWorkspaceRepository(
    WorkspaceRepository, InMemRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]
):
    pass


class InMemWorkspacePermissionRepository(
    WorkspacePermissionRepository,
    InMemRepository[int, WorkspacePermissionCreate, WorkspacePermissionUpdate, WorkspacePermissionInDB],
):
    def get_for_user(self, user_id: int) -> List[WorkspacePermissionInDB]:
        return [item for item in self._state.values() if item.user_id == user_id]


class InMemCredentialRepository(
    CredentialRepository, InMemRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]
):
    pass


class InMemGitentialBackend(GitentialBackend):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        self._users: UserRepository = InMemUserRepository(in_db_cls=UserInDB)
        self._user_infos: UserInfoRepository = InMemUserInfoRepository(in_db_cls=UserInfoInDB)
        self._workspaces: WorkspaceRepository = InMemWorkspaceRepository(in_db_cls=WorkspaceInDB)
        self._workspace_permissions: WorkspacePermissionRepository = InMemWorkspacePermissionRepository(
            in_db_cls=WorkspacePermissionInDB
        )
        self._credentials: CredentialRepository = InMemCredentialRepository(in_db_cls=CredentialInDB)

    @property
    def users(self) -> UserRepository:
        return self._users

    @property
    def user_infos(self) -> UserInfoRepository:
        return self._user_infos

    @property
    def workspaces(self) -> WorkspaceRepository:
        return self._workspaces

    @property
    def workspace_permissions(self) -> WorkspacePermissionRepository:
        return self._workspace_permissions

    @property
    def credentials(self) -> CredentialRepository:
        return self._credentials

    def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
        ret = []
        workspace_permissions = self.workspace_permissions.get_for_user(user_id)

        for wp in workspace_permissions:
            w = self.workspaces.get(wp.workspace_id)
            if w:
                ret.append(
                    WorkspaceWithPermission(
                        id=w.id,
                        name=w.name,
                        role=wp.role,
                        primary=wp.primary,
                        user_id=wp.user_id,
                    )
                )
        return ret


# from typing import Optional
# from threading import Lock

# # from uuid import uuid4, UUID

# from pydantic import BaseModel

# from gitential2.settings import GitentialSettings
# from gitential2.datatypes import (
#     UserInfoCreate,
#     UserCreate,
#     CredentialCreate,
#     WorkspacePermissionCreate,
#     WorkspaceCreate,
#     WorkspaceWithPermission,
# )

# from .common import GitentialBackend


# class InMemoryBackend(GitentialBackend):
#     def __init__(self, settings: GitentialSettings):
#         super().__init__(settings)
#         self._counter = 1
#         self._counter_lock = Lock()
#         self._state = {"users": {}, "user_infos": {}, "credentials": {}, "workspaces": {}, "workspace_permissions": {}}

#     def get_user(self, user_info: Optional[UserInfoCreate] = None) -> Optional[UserCreate]:
#         if user_info.email:
#             for user in self._state["users"].values():
#                 if user.email == user_info.email:
#                     return user
#         return None

#     def upsert_user(self, user: UserCreate) -> UserCreate:
#         find_existing = lambda other: other.email == user.email
#         return self._upsert_object(obj=user, table="users", exclude={"id", "created_at"}, find_existing=find_existing)

#     def upsert_credential(self, credential: CredentialCreate) -> CredentialCreate:
#         find_existing = (
#             lambda other: other.owner_id == credential.owner_id and other.integration == credential.integration
#         )
#         return self._upsert_object(
#             obj=credential, table="credentials", exclude={"id", "created_at"}, find_existing=find_existing
#         )

#     def upsert_user_info(self, user_info: UserInfoCreate) -> UserInfoCreate:
#         find_existing = lambda other: other.integration == user_info.integration and other.sub == user_info.sub
#         return self._upsert_object(
#             obj=user_info, table="user_infos", exclude={"id", "created_at"}, find_existing=find_existing
#         )

#     def upsert_workspace(self, workspace: WorkspaceCreate) -> WorkspaceCreate:
#         return self._upsert_object(obj=workspace, table="workspaces", exclude={"id", "created_at"})

#     def upsert_workspace_permission(self, workspace_permission: WorkspacePermissionCreate) -> WorkspacePermissionCreate:
#         find_existing = (
#             lambda other: other.user_id == workspace_permission.user_id
#             and other.workspace_id == workspace_permission.workspace_id
#         )
#         return self._upsert_object(
#             obj=workspace_permission,
#             table="workspace_permissions",
#             exclude={"id", "created_at"},
#             find_existing=find_existing,
#         )

#     def _new_id(self):
#         with self._counter_lock:
#             ret = self._counter
#             self._counter += 1
#         return ret

#     def _upsert_object(self, obj: BaseModel, table="users", find_existing=None, exclude=None, new_id=None):
#         new_id = new_id or self._new_id
#         find_existing = find_existing or (lambda other_obj: other_obj.id == obj.id)
#         exclude = exclude or {"id", "created_at"}

#         existing_obj = next((eo for eo in self._state[table].values() if find_existing(eo)), None)

#         if existing_obj:
#             updates = obj.dict(exclude=exclude)
#             self._state[table][existing_obj.id] = existing_obj.copy(update=updates)
#             return self._state[table][existing_obj.id]
#         else:
#             if obj.id is None:
#                 obj.id = new_id()
#             self._state[table][obj.id] = obj
#             return self._state[table][obj.id]

#     def get_accessible_workspaces(self, user_id: int) -> WorkspaceWithPermission:
#         ret = []
#         for workspace_permission in self._state["workspace_permissions"].values():
#             if workspace_permission.user_id == user_id:
#                 workspace = self._state["workspaces"][workspace_permission.workspace_id]
#                 ret.append(
#                     WorkspaceWithPermission(
#                         id=workspace.id,
#                         name=workspace.name,
#                         role=workspace_permission.role,
#                         primary=workspace_permission.primary,
#                         user_id=workspace_permission.user_id,
#                     )
#                 )
#         return ret
