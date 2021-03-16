import datetime as dt
import json
from typing import Optional, Tuple

import pandas as pd
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from fastapi.encoders import jsonable_encoder

from gitential2.datatypes.extraction import (
    ExtractedCommit,
    ExtractedKind,
    ExtractedPatch,
    Langtype,
    ExtractedPatchRewrite,
)
from gitential2.datatypes.subscriptions import SubscriptionInDB
from gitential2.datatypes.pull_requests import PullRequest
from gitential2.extraction.output import OutputHandler
from gitential2.datatypes.teammembers import TeamMemberInDB
from gitential2.datatypes.teams import TeamInDB

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
    AuthorInDB,
)
from gitential2.settings import GitentialSettings

from ..base import GitentialBackend
from ..base.mixins import WithRepositoriesMixin

from .tables import (
    users_table,
    user_infos_table,
    credentials_table,
    workspaces_table,
    workspace_members_table,
    metadata,
    subscriptions_table,
)

from .repositories import (
    SQLAuthorRepository,
    SQLProjectRepositoryRepository,
    SQLPullRequestRepository,
    SQLRepositoryRepository,
    SQLTeamMemberRepository,
    SQLTeamRepository,
    SQLUserRepository,
    SQLSubscriptionRepository,
    SQLUserInfoRepository,
    SQLCredentialRepository,
    SQLWorkspaceRepository,
    SQLWorkspaceMemberRepository,
    SQLProjectRepository,
    SQLExtractedCommitRepository,
    SQLExtractedPatchRepository,
    SQLExtractedPatchRewriteRepository,
)


def json_dumps(obj):
    return json.dumps(jsonable_encoder(obj))


class SQLGitentialBackend(WithRepositoriesMixin, GitentialBackend):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)

        self._engine = sa.create_engine(settings.connections.database_url, json_serializer=json_dumps)
        self._metadata = metadata
        self._metadata.create_all(self._engine)

        self._users = SQLUserRepository(table=users_table, engine=self._engine, in_db_cls=UserInDB)
        self._subscriptions = SQLSubscriptionRepository(
            table=subscriptions_table, engine=self._engine, in_db_cls=SubscriptionInDB
        )
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
            table=self._workspace_tables.tables["projects"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=ProjectInDB,
        )

        self._repositories = SQLRepositoryRepository(
            table=self._workspace_tables.tables["repositories"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=RepositoryInDB,
        )
        self._project_repositories = SQLProjectRepositoryRepository(
            table=self._workspace_tables.tables["project_repositories"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=ProjectRepositoryInDB,
        )
        self._authors = SQLAuthorRepository(
            table=self._workspace_tables.tables["authors"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=AuthorInDB,
        )

        self._teams = SQLTeamRepository(
            table=self._workspace_tables.tables["teams"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=TeamInDB,
        )
        self._team_members = SQLTeamMemberRepository(
            table=self._workspace_tables.tables["team_members"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=TeamMemberInDB,
        )
        self._extracted_commits = SQLExtractedCommitRepository(
            table=self._workspace_tables.tables["extracted_commits"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=ExtractedCommit,
        )

        self._extracted_patches = SQLExtractedPatchRepository(
            table=self._workspace_tables.tables["extracted_patches"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=ExtractedPatch,
        )

        self._extracted_patch_rewrites = SQLExtractedPatchRewriteRepository(
            table=self._workspace_tables.tables["extracted_patch_rewrites"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=ExtractedPatchRewrite,
        )

        self._pull_requests = SQLPullRequestRepository(
            table=self._workspace_tables.tables["pull_requests"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=PullRequest,
        )

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

    def output_handler(self, workspace_id: int) -> OutputHandler:
        return SQLOutputHandler(engine=self._engine, workspace_id=workspace_id)

    def get_extracted_dataframes(
        self, workspace_id: int, repository_id: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        def _filter_by_repo(df, repo_id):
            return df[df["repo_id"] == repo_id]

        schema_name = self._workspace_schema_name(workspace_id)
        extracted_commits_df = _filter_by_repo(
            pd.read_sql_table("extracted_commits", schema=schema_name, con=self._engine), repository_id
        )
        extracted_patches_df = _filter_by_repo(
            pd.read_sql_table("extracted_patches", schema=schema_name, con=self._engine), repository_id
        )
        extracted_patch_rewrites_df = _filter_by_repo(
            pd.read_sql_table("extracted_patch_rewrites", schema=schema_name, con=self._engine), repository_id
        )
        # print("megy a select", extracted_commits_df)

        return extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df

    def save_calculated_dataframes(
        self,
        workspace_id: int,
        repository_id: int,
        calculated_commits_df: pd.DataFrame,
        calculated_patches_df: pd.DataFrame,
    ):
        schema_name = self._workspace_schema_name(workspace_id)
        workspace_metadata = get_workspace_metadata(schema_name)
        calculated_commits_table = workspace_metadata.tables[f"{schema_name}.calculated_commits"]
        calculated_patches_table = workspace_metadata.tables[f"{schema_name}.calculated_patches"]
        # print(calculated_commits_table.delete().where(calculated_commits_table.c.repo_id == repository_id))
        # print("itt vagyunk")
        with self._engine.connect() as connection:
            with connection.begin():
                connection.execute(
                    calculated_commits_table.delete().where(calculated_commits_table.c.repo_id == repository_id)
                )
                connection.execute(
                    calculated_patches_table.delete().where(calculated_patches_table.c.repo_id == repository_id)
                )
                calculated_commits_df.to_sql(
                    name="calculated_commits", schema=schema_name, if_exists="append", con=connection, index=False
                )
                calculated_patches_df.to_sql(
                    name="calculated_patches", schema=schema_name, if_exists="append", con=connection, index=False
                )


class SQLOutputHandler(OutputHandler):
    def __init__(self, engine, workspace_id):
        self.workspace_id = workspace_id
        self.engine = engine
        self.schema_name = f"ws_{workspace_id}"

        _workspace_metadata = get_workspace_metadata(self.schema_name)
        _workspace_metadata.create_all(self.engine)

        self.metadata = get_workspace_metadata()

    def write(self, kind, value):
        table = self._get_table_for_kind(kind)

        try:
            query = table.insert().values(**value.dict())
            self._execute_query(query)
        except IntegrityError as e:
            print(e)

    def _execute_query(self, query):
        with self.engine.connect().execution_options(schema_translate_map={None: self.schema_name}) as connection:
            result = connection.execute(query)
            return result

    def _get_table_for_kind(self, kind):
        if kind == ExtractedKind.PULL_REQUEST:
            return self.metadata.tables["pull_requests"]
        elif kind == ExtractedKind.EXTRACTED_COMMIT:
            return self.metadata.tables["extracted_commits"]
        elif kind == ExtractedKind.EXTRACTED_PATCH:
            return self.metadata.tables["extracted_patches"]
        elif kind == ExtractedKind.EXTRACTED_PATCH_REWRITE:
            return self.metadata.tables["extracted_patch_rewrites"]
        else:
            raise ValueError("invalid kind")


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
        sa.PrimaryKeyConstraint("repo_id", "commit_id"),
    )

    calculated_commits = sa.Table(
        "calculated_commits",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("date", sa.DateTime()),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aid", sa.Integer()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("aname", sa.String(128)),
        sa.Column("ctime", sa.DateTime()),
        sa.Column("cid", sa.Integer()),
        sa.Column("cemail", sa.String(128)),
        sa.Column("cname", sa.String(128)),
        sa.Column("message", sa.Text()),
        sa.Column("nparents", sa.Integer()),
        sa.Column("tree_id", sa.String(40)),
        sa.Column("ismerge", sa.Boolean),
        sa.Column("loc_i", sa.Integer(), nullable=True),
        sa.Column("loc_i_inlier", sa.Integer(), nullable=True),
        sa.Column("loc_i_outlier", sa.Integer(), nullable=True),
        sa.Column("loc_d", sa.Integer(), nullable=True),
        sa.Column("loc_d_inlier", sa.Integer(), nullable=True),
        sa.Column("loc_d_outlier", sa.Integer(), nullable=True),
        sa.Column("comp_i", sa.Integer(), nullable=True),
        sa.Column("comp_i_inlier", sa.Integer(), nullable=True),
        sa.Column("comp_i_outlier", sa.Integer(), nullable=True),
        sa.Column("comp_d", sa.Integer(), nullable=True),
        sa.Column("comp_d_inlier", sa.Integer(), nullable=True),
        sa.Column("comp_d_outlier", sa.Integer(), nullable=True),
        sa.Column("nfiles", sa.Integer(), nullable=True),
        sa.Column("loc_effort", sa.Integer(), nullable=True),
        sa.Column("hours_measured", sa.Float(), nullable=True),
        sa.Column("hours_estimated", sa.Float(), nullable=True),
        sa.Column("hours", sa.Float(), nullable=True),
        sa.Column("velocity", sa.Float(), nullable=True),
        sa.Column("loc_churn", sa.Integer(), default=0),
        sa.PrimaryKeyConstraint("repo_id", "commit_id"),
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
        sa.Column("langtype", sa.Enum(Langtype)),
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
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "parent_commit_id", "newpath"),
    )

    calculated_patches = sa.Table(
        "calculated_patches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("date", sa.DateTime()),
        sa.Column("parent_commit_id", sa.String(40)),
        sa.Column("status", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("oldpath", sa.String(256)),
        sa.Column("newsize", sa.Integer()),
        sa.Column("oldsize", sa.Integer()),
        sa.Column("is_binary", sa.Boolean()),
        sa.Column("lang", sa.String(32)),
        sa.Column("langtype", sa.Enum(Langtype)),
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
        sa.Column("ismerge", sa.Boolean()),
        sa.Column("istest", sa.Boolean()),
        sa.Column("churn_loc_d", sa.Boolean()),
        sa.Column("outlier", sa.Boolean),
        sa.Column("anomaly", sa.Boolean),
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "parent_commit_id", "newpath"),
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
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "rewritten_commit_id", "newpath"),
    )

    authors = sa.Table(
        "authors",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("active", sa.Boolean),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("aliases", sa.JSON, nullable=True),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    # author_aliases = sa.Table(
    #     "author_aliases",
    #     metadata,
    #     sa.Column("id", sa.Integer, primary_key=True),
    #     sa.Column("name", sa.String(256), nullable=True),
    #     sa.Column("email", sa.String(256), nullable=True),
    #     sa.Column("author_id", sa.Integer, sa.ForeignKey("authors.id"), nullable=True),
    # )

    teams = sa.Table(
        "teams",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("sprints_enabled", sa.Boolean, default=False),
        sa.Column("sprint", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    team_members = sa.Table(
        "team_members",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("team_id", sa.Integer, sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("author_id", sa.Integer, sa.ForeignKey("authors.id"), nullable=False),
    )

    sprints = sa.Table(
        "sprints",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("team_id", sa.Integer, sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("date", sa.DateTime, nullable=False),
        sa.Column("weeks", sa.Integer, default=1),
        sa.Column("pattern", sa.String(64)),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    # Pull Requests
    pull_requests = sa.Table(
        "pull_requests",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("number", sa.Integer()),
        sa.Column("title", sa.String(256)),
        sa.Column("platform", sa.String(32)),
        sa.Column("id_platform", sa.Integer()),
        sa.Column("api_resource_uri", sa.String(256)),
        sa.Column("state_platform", sa.String(16)),
        sa.Column("state", sa.String(16)),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("closed_at", sa.DateTime, nullable=True),
        sa.Column("updated_at", sa.DateTime, nullable=True),
        sa.Column("merged_at", sa.DateTime, nullable=True),
        sa.Column("additions", sa.Integer(), nullable=True),
        sa.Column("deletions", sa.Integer(), nullable=True),
        sa.Column("changed_files", sa.Integer(), nullable=True),
        sa.Column("draft", sa.Boolean, default=False, nullable=False),
        sa.Column("user", sa.String(64)),
        sa.Column("commits", sa.Integer(), nullable=True),
        sa.Column("merged_by", sa.String(64), nullable=True),
        sa.Column("first_reaction_at", sa.DateTime, nullable=True),
        sa.Column("first_commit_authored_at", sa.DateTime, nullable=True),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.UniqueConstraint("repo_id", "number", name="uix__repo_id__number"),
    )

    return metadata
