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
    get_workspace_metadata,
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

        self._engine = sa.create_engine(
            settings.connections.database_url, json_serializer=json_dumps, pool_pre_ping=True
        )
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
        workspace_metadata, _ = get_workspace_metadata(schema_name)
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
        workspace_metadata, _ = get_workspace_metadata(schema_name)
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

        _workspace_metadata, _ = get_workspace_metadata(self.schema_name)
        _workspace_metadata.create_all(self.engine)

        self.metadata, _ = get_workspace_metadata()

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
