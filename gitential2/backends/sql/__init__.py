from datetime import datetime

import json
from typing import Any, Tuple, Set
from threading import Lock
import pandas as pd
import sqlalchemy as sa
from ibis.backends.postgres import connect
from sqlalchemy.sql import and_, select

from fastapi.encoders import jsonable_encoder

from gitential2.datatypes.extraction import (
    ExtractedCommit,
    ExtractedKind,
    ExtractedPatch,
    ExtractedPatchRewrite,
)
from gitential2.datatypes.subscriptions import SubscriptionInDB
from gitential2.datatypes.pull_requests import PullRequest
from gitential2.extraction.output import OutputHandler
from gitential2.datatypes.teammembers import TeamMemberInDB
from gitential2.datatypes.teams import TeamInDB
from gitential2.datatypes.stats import IbisTables
from gitential2.datatypes import (
    UserInDB,
    UserInfoInDB,
    CredentialInDB,
    WorkspaceInDB,
    ProjectInDB,
    RepositoryInDB,
    ProjectRepositoryInDB,
    WorkspaceMemberInDB,
    AuthorInDB,
)
from gitential2.datatypes.email_log import EmailLogInDB

from gitential2.datatypes.calculated import CalculatedCommit, CalculatedPatch
from gitential2.settings import GitentialSettings

from ..base import GitentialBackend
from ..base.mixins import WithRepositoriesMixin

from .tables import (
    access_log_table,
    email_log_table,
    users_table,
    user_infos_table,
    credentials_table,
    workspaces_table,
    workspace_members_table,
    metadata,
    subscriptions_table,
    get_workspace_metadata,
)

from .repositories import (
    SQLAccessLogRepository,
    SQLAuthorRepository,
    SQLEmailLogRepository,
    SQLCalculatedPatchRepository,
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
    SQLCalculatedCommitRepository,
)


def json_dumps(obj):
    return json.dumps(jsonable_encoder(obj))


class SQLGitentialBackend(WithRepositoriesMixin, GitentialBackend):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        self._ibis_conn = None
        self._ibis_lock = Lock()
        self._engine = sa.create_engine(
            settings.connections.database_url, json_serializer=json_dumps, pool_pre_ping=True
        )
        self._metadata = metadata
        self._metadata.create_all(self._engine)

        self._users = SQLUserRepository(table=users_table, engine=self._engine, in_db_cls=UserInDB)
        self._access_logs = SQLAccessLogRepository(table=access_log_table, engine=self._engine)
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

        self._workspace_tables, _ = get_workspace_metadata(schema=None)

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

        self._calculated_commits = SQLCalculatedCommitRepository(
            table=self._workspace_tables.tables["calculated_commits"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=CalculatedCommit,
        )

        self._calculated_patches = SQLCalculatedPatchRepository(
            table=self._workspace_tables.tables["calculated_patches"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=CalculatedPatch,
        )

        self._pull_requests = SQLPullRequestRepository(
            table=self._workspace_tables.tables["pull_requests"],
            engine=self._engine,
            metadata=self._workspace_tables,
            in_db_cls=PullRequest,
        )

        self._email_log = SQLEmailLogRepository(table=email_log_table, engine=self._engine, in_db_cls=EmailLogInDB)

    def _execute_query(self, query):
        with self._engine.connect() as connection:
            result = connection.execute(query)
            return result

    def _workspace_schema_name(self, workspace_id: int) -> str:
        return f"ws_{workspace_id}"

    def initialize_workspace(self, workspace_id: int):
        schema_name = self._workspace_schema_name(workspace_id)
        self._engine.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        workspace_metadata, _ = get_workspace_metadata(schema_name)
        workspace_metadata.create_all(self._engine)

    def output_handler(self, workspace_id: int) -> OutputHandler:
        return SQLOutputHandler(workspace_id=workspace_id, backend=self)

    def get_commit_ids_for_repository(self, workspace_id: int, repository_id: int) -> Set[str]:
        schema_name = self._workspace_schema_name(workspace_id)
        workspace_metadata, _ = get_workspace_metadata(schema_name)
        extracted_commits_table = workspace_metadata.tables[f"{schema_name}.extracted_commits"]
        query = select([extracted_commits_table.c.commit_id]).where(extracted_commits_table.c.repo_id == repository_id)
        with self._engine.connect() as connection:
            with connection.begin():
                result = connection.execute(query)
                return set(row["commit_id"] for row in result.fetchall())

    def get_extracted_dataframes(
        self, workspace_id: int, repository_id: int, from_: datetime, to_: datetime
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        # def _filter_by_repo(df, repo_id):
        #     return df[df["repo_id"] == repo_id]

        schema_name = self._workspace_schema_name(workspace_id)
        workspace_metadata, _ = get_workspace_metadata(schema_name)
        extracted_commits_table = workspace_metadata.tables[f"{schema_name}.extracted_commits"]
        extracted_patches_table = workspace_metadata.tables[f"{schema_name}.extracted_patches"]
        extracted_patch_rewrites_table = workspace_metadata.tables[f"{schema_name}.extracted_patch_rewrites"]

        schema_name = self._workspace_schema_name(workspace_id)
        extracted_commits_df = pd.read_sql_query(
            extracted_commits_table.select().where(
                and_(
                    extracted_commits_table.c.atime >= from_,
                    extracted_commits_table.c.atime < to_,
                    extracted_commits_table.c.repo_id == repository_id,
                )
            ),
            con=self._engine,
        )

        extracted_patches_join_ = extracted_patches_table.join(
            extracted_commits_table,
            and_(
                extracted_commits_table.c.repo_id == extracted_patches_table.c.repo_id,
                extracted_commits_table.c.commit_id == extracted_patches_table.c.commit_id,
            ),
        )
        extracted_patches_query_ = (
            select([extracted_patches_table])
            .select_from(extracted_patches_join_)
            .where(
                and_(
                    extracted_commits_table.c.atime >= from_,
                    extracted_commits_table.c.atime < to_,
                    extracted_patches_table.c.repo_id == repository_id,
                )
            )
        )

        extracted_patches_df = pd.read_sql_query(
            extracted_patches_query_,
            con=self._engine,
        )

        extracted_patch_rewrites_df = pd.read_sql_query(
            extracted_patch_rewrites_table.select().where(
                and_(
                    extracted_patch_rewrites_table.c.rewritten_atime >= from_,
                    extracted_patch_rewrites_table.c.rewritten_atime < to_,
                    extracted_patch_rewrites_table.c.repo_id == repository_id,
                )
            ),
            #                schema=schema_name,
            con=self._engine,
        )
        # print("megy a select", extracted_commits_df)

        return extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df

    def get_ibis_tables(self, workspace_id: int) -> Any:
        with self._ibis_lock:
            ibis_conn = self._get_ibis_conn()
            ibis_schema = ibis_conn.schema(self._workspace_schema_name(workspace_id))
            ret = IbisTables()
            ret.conn = ibis_conn
            ret.pull_requests = ibis_schema.pull_requests
            ret.commits = ibis_schema.calculated_commits
            ret.patches = ibis_schema.calculated_patches
            ret.authors = ibis_schema.authors
            return ret

    def _get_ibis_conn(self):
        if not self._ibis_conn:
            self._ibis_conn = connect(url=self.settings.connections.database_url)
        return self._ibis_conn

    def save_calculated_dataframes(
        self,
        workspace_id: int,
        repository_id: int,
        calculated_commits_df: pd.DataFrame,
        calculated_patches_df: pd.DataFrame,
        from_: datetime,
        to_: datetime,
    ):
        schema_name = self._workspace_schema_name(workspace_id)
        workspace_metadata, _ = get_workspace_metadata(schema_name)
        calculated_commits_table = workspace_metadata.tables[f"{schema_name}.calculated_commits"]
        calculated_patches_table = workspace_metadata.tables[f"{schema_name}.calculated_patches"]
        # print(calculated_commits_table.delete().where(calculated_commits_table.c.repo_id == repository_id))
        # print("itt vagyunk")
        calculated_commits_df = calculated_commits_df.reset_index().drop(["median_velocity_measured"], axis=1)
        calculated_patches_df = calculated_patches_df.reset_index()
        with self._engine.connect() as connection:
            with connection.begin():
                connection.execute(
                    calculated_commits_table.delete().where(
                        and_(
                            calculated_commits_table.c.repo_id == repository_id,
                            calculated_commits_table.c.date >= from_,
                            calculated_commits_table.c.date < to_,
                        )
                    )
                )
                connection.execute(
                    calculated_patches_table.delete().where(
                        and_(
                            calculated_patches_table.c.repo_id == repository_id,
                            calculated_patches_table.c.date >= from_,
                            calculated_patches_table.c.date < to_,
                        )
                    )
                )
                calculated_commits_df.to_sql(
                    name="calculated_commits", schema=schema_name, if_exists="append", con=connection, index=False
                )
                calculated_patches_df.to_sql(
                    name="calculated_patches", schema=schema_name, if_exists="append", con=connection, index=False
                )


class SQLOutputHandler(OutputHandler):
    def __init__(self, workspace_id: int, backend: GitentialBackend):
        self.workspace_id = workspace_id
        self.backend = backend

    def write(self, kind, value):
        repository = self._get_repository(kind)
        return repository.create_or_update(self.workspace_id, value)

    def _get_repository(self, kind):
        if kind == ExtractedKind.PULL_REQUEST:
            return self.backend.pull_requests
        elif kind == ExtractedKind.EXTRACTED_COMMIT:
            return self.backend.extracted_commits
        elif kind == ExtractedKind.EXTRACTED_PATCH:
            return self.backend.extracted_patches
        elif kind == ExtractedKind.EXTRACTED_PATCH_REWRITE:
            return self.backend.extracted_patch_rewrites
        else:
            raise ValueError("invalid kind")
