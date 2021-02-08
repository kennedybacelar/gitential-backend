import uuid
import datetime as dt
from typing import Optional, List, Type
import sqlalchemy as sa
from sqlalchemy.sql import and_, select
from sqlalchemy_utils.types import UUIDType
from gitential2.settings import GitentialSettings

from gitential2.datatypes import (
    UserInDB,
    UserInfoInDB,
    CredentialInDB,
    WorkspaceInDB,
    WorkspacePermissionInDB,
    WorkspaceWithPermission,
)

from ..common import (
    GitentialBackend,
    IdType,
    CreateType,
    UpdateType,
    InDBType,
    BaseRepository,
    UserRepository,
    UserInfoRepository,
    CredentialRepository,
    WorkspaceRepository,
    WorkspacePermissionRepository,
)

from .tables import (
    users_table,
    user_infos_table,
    credentials_table,
    workspaces_table,
    workspace_permissions_table,
    metadata,
)
from .repositories import (
    SQLUserRepository,
    SQLUserInfoRepository,
    SQLCredentialRepository,
    SQLWorkspaceRepository,
    SQLWorkspacePermissionRepository,
)


class SQLGitentialBackend(GitentialBackend):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        self._engine = sa.create_engine(settings.backend_connection)
        self._metadata = metadata
        self._metadata.create_all(self._engine)

        self._users = SQLUserRepository(table=users_table, engine=self._engine, in_db_cls=UserInDB)
        self._user_infos = SQLUserInfoRepository(table=user_infos_table, engine=self._engine, in_db_cls=UserInfoInDB)
        self._credentials = SQLCredentialRepository(
            table=credentials_table, engine=self._engine, in_db_cls=CredentialInDB
        )
        self._workspaces = SQLWorkspaceRepository(table=workspaces_table, engine=self._engine, in_db_cls=WorkspaceInDB)
        self._workspace_permissions = SQLWorkspacePermissionRepository(
            table=workspace_permissions_table, engine=self._engine, in_db_cls=WorkspacePermissionInDB
        )

    @property
    def users(self) -> UserRepository:
        return self._users

    @property
    def user_infos(self) -> UserInfoRepository:
        return self._user_infos

    @property
    def credentials(self) -> CredentialRepository:
        return self._credentials

    @property
    def workspaces(self) -> WorkspaceRepository:
        return self._workspaces

    @property
    def workspace_permissions(self) -> WorkspacePermissionRepository:
        return self._workspace_permissions

    def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
        query = (
            select(
                [
                    workspaces_table.c.id,
                    workspaces_table.c.name,
                    workspace_permissions_table.c.role,
                    workspace_permissions_table.c.primary,
                    workspace_permissions_table.c.user_id,
                ]
            )
            .select_from(
                workspace_permissions_table.join(
                    workspaces_table, workspace_permissions_table.c.workspace_id == workspaces_table.c.id
                )
            )
            .where(workspace_permissions_table.c.user_id == user_id)
        )
        result = self._execute_query(query)
        return [WorkspaceWithPermission(**row) for row in result.fetchall()]

    def _execute_query(self, query):
        with self._engine.connect() as connection:
            result = connection.execute(query)
            return result


# def get_workspace_metadata():
#     metadata = sa.MetaData()
#     # Extracted Commits
#     extracted_commits = sa.Table(
#         "extracted_commits",
#         metadata,
#         sa.Column("repo_id", sa.Integer()),
#         sa.Column("commit_id", sa.String(40)),
#         sa.Column("atime", sa.DateTime()),
#         sa.Column("aemail", sa.String(128)),
#         sa.Column("aname", sa.String(128)),
#         sa.Column("ctime", sa.DateTime()),
#         sa.Column("cemail", sa.String(128)),
#         sa.Column("cname", sa.String(128)),
#         sa.Column("message", sa.Text()),
#         sa.Column("nparents", sa.Integer()),
#         sa.Column("tree_id", sa.String(40)),
#     )

#     # Extracted Patches
#     extracted_patches = sa.Table(
#         "extracted_patches",
#         metadata,
#         sa.Column("repo_id", sa.Integer()),
#         sa.Column("commit_id", sa.String(40)),
#         sa.Column("parent_commit_id", sa.String(40)),
#         sa.Column("status", sa.String(128)),
#         sa.Column("newpath", sa.String(256)),
#         sa.Column("oldpath", sa.String(256)),
#         sa.Column("newsize", sa.Integer()),
#         sa.Column("oldsize", sa.Integer()),
#         sa.Column("is_binary", sa.Boolean()),
#         sa.Column("lang", sa.String(32)),
#         sa.Column("langtype", sa.String(32)),
#         # Extracted plain metrics
#         sa.Column("loc_i", sa.Integer()),
#         sa.Column("loc_d", sa.Integer()),
#         sa.Column("comp_i", sa.Integer()),
#         sa.Column("comp_d", sa.Integer()),
#         sa.Column("loc_i_std", sa.Integer()),
#         sa.Column("loc_d_std", sa.Integer()),
#         sa.Column("comp_i_std", sa.Integer()),
#         sa.Column("comp_d_std", sa.Integer()),
#         sa.Column("nhunks", sa.Integer()),
#         sa.Column("nrewrites", sa.Integer()),
#         sa.Column("rewrites_loc", sa.Integer()),
#     )

#     # Extracted Patch Rewrites
#     extracted_patch_rewrites = sa.Table(
#         "extracted_patch_rewrites",
#         metadata,
#         sa.Column("repo_id", sa.Integer()),
#         sa.Column("commit_id", sa.String(40)),
#         sa.Column("atime", sa.DateTime()),
#         sa.Column("aemail", sa.String(128)),
#         sa.Column("newpath", sa.String(256)),
#         sa.Column("rewritten_atime", sa.DateTime()),
#         sa.Column("rewritten_aemail", sa.String(128)),
#         sa.Column("rewritten_commit_id", sa.String(40)),
#         sa.Column("loc_d", sa.Integer()),
#     )

#     # Calculated Patches

#     # Calculated Commits
#     return metadata
