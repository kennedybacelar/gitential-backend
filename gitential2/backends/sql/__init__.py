import datetime as dt
from typing import Optional, List
import sqlalchemy as sa
from sqlalchemy.sql import select

from gitential2.settings import GitentialSettings
from gitential2.datatypes import (
    UserInDB,
    UserInfoInDB,
    CredentialInDB,
    WorkspaceInDB,
    ProjectInDB,
    RepositoryInDB,
    ProjectRepositoryInDB,
    GitProtocol,
    WorkspaceMemberInDB,
)

from ..base import GitentialBackend
from ..base.mixins import WithRepositoriesMixin

from .tables import (
    users_table,
    user_infos_table,
    credentials_table,
    workspaces_table,
    workspace_members_table,
    metadata,
)
from .repositories import (
    SQLProjectRepositoryRepository,
    SQLRepositoryRepository,
    SQLUserRepository,
    SQLUserInfoRepository,
    SQLCredentialRepository,
    SQLWorkspaceRepository,
    SQLWorkspaceMemberRepository,
    SQLProjectRepository,
)


class SQLGitentialBackend(WithRepositoriesMixin, GitentialBackend):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        self._engine = sa.create_engine(settings.connections.database_url)
        self._metadata = metadata
        self._metadata.create_all(self._engine)

        self._users = SQLUserRepository(table=users_table, engine=self._engine, in_db_cls=UserInDB)
        self._user_infos = SQLUserInfoRepository(table=user_infos_table, engine=self._engine, in_db_cls=UserInfoInDB)
        self._credentials = SQLCredentialRepository(
            table=credentials_table, engine=self._engine, in_db_cls=CredentialInDB
        )
        self._workspaces = SQLWorkspaceRepository(table=workspaces_table, engine=self._engine, in_db_cls=WorkspaceInDB)
        self._workspace_members = SQLWorkspaceMemberRepository(
            table=workspace_members_table, engine=self._engine, in_db_cls=WorkspaceMemberInDB
        )

        self._workspace_tables = get_workspace_metadata(schema=None)
        self._projects = SQLProjectRepository(
            table=self._workspace_tables.tables["projects"], engine=self._engine, in_db_cls=ProjectInDB
        )

        self._repositories = SQLRepositoryRepository(
            table=self._workspace_tables.tables["repositories"], engine=self._engine, in_db_cls=RepositoryInDB
        )
        self._project_repositories = SQLProjectRepositoryRepository(
            table=self._workspace_tables.tables["project_repositories"],
            engine=self._engine,
            in_db_cls=ProjectRepositoryInDB,
        )

    # def get_accessible_workspaces(self, user_id: int) -> List[WorkspaceWithPermission]:
    #     query = (
    #         select(
    #             [
    #                 workspaces_table.c.id,
    #                 workspaces_table.c.name,
    #                 workspace_permissions_table.c.role,
    #                 workspace_permissions_table.c.primary,
    #                 workspace_permissions_table.c.user_id,
    #             ]
    #         )
    #         .select_from(
    #             workspace_permissions_table.join(
    #                 workspaces_table, workspace_permissions_table.c.workspace_id == workspaces_table.c.id
    #             )
    #         )
    #         .where(workspace_permissions_table.c.user_id == user_id)
    #     )
    #     result = self._execute_query(query)
    #     return [WorkspaceWithPermission(**row) for row in result.fetchall()]

    def _execute_query(self, query):
        with self._engine.connect() as connection:
            result = connection.execute(query)
            return result

    def _workspace_schema_name(self, workspace_id: int) -> str:
        return f"ws_{workspace_id}"

    def initialize_workspace(self, workspace_id: int):
        schema_name = self._workspace_schema_name(workspace_id)
        self._engine.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        workspace_metadata = get_workspace_metadata(schema_name)
        workspace_metadata.create_all(self._engine)


def get_workspace_metadata(schema: Optional[str] = None):
    metadata = sa.MetaData(schema=schema)

    # pylint: disable=unused-variable

    projects = sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(128), nullable=True),
        sa.Column("pattern", sa.String(256), nullable=True),
        sa.Column("shareable", sa.Boolean, default=False, nullable=False),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    repositories = sa.Table(
        "repositories",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("clone_url", sa.String(256), nullable=False, unique=True),
        sa.Column("protocol", sa.Enum(GitProtocol), default=GitProtocol.https),
        sa.Column("name", sa.String(128)),
        sa.Column("namespace", sa.String(128)),
        sa.Column("private", sa.Boolean, nullable=False, default=True),
        sa.Column("integration_type", sa.String(64), nullable=True),
        sa.Column("integration_name", sa.String(64), nullable=True),
        sa.Column("credential_id", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    project_repositories = sa.Table(
        "project_repositories",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_id", sa.Integer, sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("repo_id", sa.Integer, sa.ForeignKey("repositories.id"), nullable=False),
    )

    # Extracted Commits
    extracted_commits = sa.Table(
        "extracted_commits",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("aname", sa.String(128)),
        sa.Column("ctime", sa.DateTime()),
        sa.Column("cemail", sa.String(128)),
        sa.Column("cname", sa.String(128)),
        sa.Column("message", sa.Text()),
        sa.Column("nparents", sa.Integer()),
        sa.Column("tree_id", sa.String(40)),
    )

    # Extracted Patches
    extracted_patches = sa.Table(
        "extracted_patches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("parent_commit_id", sa.String(40)),
        sa.Column("status", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("oldpath", sa.String(256)),
        sa.Column("newsize", sa.Integer()),
        sa.Column("oldsize", sa.Integer()),
        sa.Column("is_binary", sa.Boolean()),
        sa.Column("lang", sa.String(32)),
        sa.Column("langtype", sa.String(32)),
        # Extracted plain metrics
        sa.Column("loc_i", sa.Integer()),
        sa.Column("loc_d", sa.Integer()),
        sa.Column("comp_i", sa.Integer()),
        sa.Column("comp_d", sa.Integer()),
        sa.Column("loc_i_std", sa.Integer()),
        sa.Column("loc_d_std", sa.Integer()),
        sa.Column("comp_i_std", sa.Integer()),
        sa.Column("comp_d_std", sa.Integer()),
        sa.Column("nhunks", sa.Integer()),
        sa.Column("nrewrites", sa.Integer()),
        sa.Column("rewrites_loc", sa.Integer()),
    )

    # Extracted Patch Rewrites
    extracted_patch_rewrites = sa.Table(
        "extracted_patch_rewrites",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("rewritten_atime", sa.DateTime()),
        sa.Column("rewritten_aemail", sa.String(128)),
        sa.Column("rewritten_commit_id", sa.String(40)),
        sa.Column("loc_d", sa.Integer()),
    )

    # Calculated Patches

    # Calculated Commits
    return metadata
