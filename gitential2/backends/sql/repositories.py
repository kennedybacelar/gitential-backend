from typing import Iterable, Optional, Callable, List, Dict, Union, cast
import datetime as dt
import typing
import sqlalchemy as sa
import pandas as pd
from sqlalchemy.sql import and_, select, desc
from sqlalchemy.dialects.postgresql import insert
from gitential2.exceptions import NotFoundException

from gitential2.datatypes import (
    UserCreate,
    UserUpdate,
    UserInDB,
    UserInfoCreate,
    UserInfoUpdate,
    UserInfoInDB,
    CredentialCreate,
    CredentialUpdate,
    CredentialInDB,
    WorkspaceCreate,
    WorkspaceUpdate,
    WorkspaceInDB,
)
from gitential2.datatypes.extraction import (
    ExtractedCommit,
    ExtractedCommitId,
    ExtractedPatch,
    ExtractedPatchId,
    ExtractedPatchRewriteId,
    ExtractedPatchRewrite,
)
from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestComment,
    PullRequestCommentId,
    PullRequestCommit,
    PullRequestId,
    PullRequestCommitId,
    PullRequestLabel,
    PullRequestLabelId,
)
from gitential2.datatypes.access_log import AccessLog
from gitential2.datatypes.authors import AuthorCreate, AuthorInDB, AuthorUpdate
from gitential2.datatypes.teams import TeamCreate, TeamInDB, TeamUpdate

from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate, ProjectInDB
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate, RepositoryInDB
from gitential2.datatypes.project_repositories import (
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectRepositoryInDB,
)
from gitential2.datatypes.teammembers import TeamMemberCreate, TeamMemberInDB, TeamMemberUpdate

from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB
from gitential2.datatypes.calculated import CalculatedCommit, CalculatedCommitId, CalculatedPatch, CalculatedPatchId
from gitential2.backends.base.repositories import (
    AccessLogRepository,
    AuthorRepository,
    CalculatedCommitRepository,
    CalculatedPatchRepository,
    ExtractedCommitRepository,
    ExtractedPatchRepository,
    ExtractedPatchRewriteRepository,
    PullRequestRepository,
    PullRequestCommitRepository,
    PullRequestCommentRepository,
    PullRequestLabelRepository,
    TeamMemberRepository,
)

from gitential2.datatypes.email_log import (
    EmailLogCreate,
    EmailLogUpdate,
    EmailLogInDB,
)

from ..base import (
    IdType,
    CreateType,
    UpdateType,
    InDBType,
    BaseRepository,
    BaseWorkspaceScopedRepository,
    UserRepository,
    SubscriptionRepository,
    UserInfoRepository,
    CredentialRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    ProjectRepository,
    RepositoryRepository,
    ProjectRepositoryRepository,
    TeamRepository,
    EmailLogRepository,
)

fetchone_ = lambda result: result.fetchone()
fetchall_ = lambda result: result.fetchall()
inserted_primary_key_ = lambda result: result.inserted_primary_key[0]
rowcount_ = lambda result: result.rowcount


class SQLAccessLogRepository(AccessLogRepository):
    def __init__(self, table: sa.Table, engine: sa.engine.Engine):
        self.table = table
        self.engine = engine

    def create(self, log: AccessLog) -> AccessLog:
        query = self.table.insert().values(**log.dict())
        self._execute_query(query)
        return log

    def last_interaction(self, user_id: int) -> Optional[AccessLog]:
        query = (
            self.table.select()
            .where(self.table.c.user_id == user_id)
            .order_by(
                desc(self.table.c.log_time),
            )
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return AccessLog(**row) if row else None

    def _execute_query(self, query, callback_fn=lambda result: result):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return callback_fn(result)


class SQLRepository(BaseRepository[IdType, CreateType, UpdateType, InDBType]):  # pylint: disable=unsubscriptable-object
    def __init__(self, table: sa.Table, engine: sa.engine.Engine, in_db_cls: Callable[..., InDBType]):
        self.table = table
        self.engine = engine
        self.in_db_cls = in_db_cls

    def identity(self, id_: IdType):
        return self.table.c.id == id_

    def get(self, id_: IdType) -> Optional[InDBType]:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, callback_fn=fetchone_)
        return self.in_db_cls(**row) if row else None

    def get_or_error(self, id_: IdType) -> InDBType:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, callback_fn=fetchone_)
        return self.in_db_cls(**row)

    def create(self, obj: CreateType) -> InDBType:
        query = self.table.insert().values(**obj.dict())
        id_ = self._execute_query(query, callback_fn=inserted_primary_key_)
        return self.get_or_error(id_)

    def create_or_update(self, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        id_ = getattr(obj, "id_", None)
        if not id_:
            return self.create(cast(CreateType, obj))
        else:
            values_dict = obj.dict(exclude_unset=True)
            if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
                values_dict["updated_at"] = dt.datetime.utcnow()
            query = (
                insert(self.table)
                .values(**values_dict)
                .on_conflict_do_update(constraint=f"{self.table.name}_pkey", set_=values_dict)
            )
            self._execute_query(query)
            return self.get_or_error(id_)

    def insert(self, id_: IdType, obj: InDBType) -> InDBType:
        values_dict = obj.dict(exclude_unset=True)
        values_dict["id"] = id_
        if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
            values_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.insert().values(**values_dict)
        self._execute_query(query)
        return self.get_or_error(id_)

    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = obj.dict(exclude_unset=True)
        if "updated_at" in self.table.columns.keys() and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.update().where(self.identity(id_)).values(**update_dict)
        self._execute_query(query)
        return self.get_or_error(id_)

    def delete(self, id_: IdType) -> int:
        query = self.table.delete().where(self.identity(id_))
        return self._execute_query(query, callback_fn=rowcount_)

    def all(self) -> Iterable[InDBType]:
        query = self.table.select()
        rows = self._execute_query(query, callback_fn=fetchall_)
        return (self.in_db_cls(**row) for row in rows)

    def _execute_query(self, query, callback_fn=lambda result: result):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return callback_fn(result)

    def truncate(self):
        query = f"TRUNCATE TABLE {self.table.name} CASCADE;"
        self._execute_query(query)

    def reset_primary_key_id(self):
        query = (
            f"SELECT pg_catalog.setval(pg_get_serial_sequence('{self.table.name}', 'id'), "
            f"(SELECT coalesce(max(id), 1) FROM {self.table.name}));"
        )
        self._execute_query(query)


class SQLWorkspaceScopedRepository(
    BaseWorkspaceScopedRepository[IdType, CreateType, UpdateType, InDBType]
):  # pylint: disable=unsubscriptable-object
    def __init__(
        self, table: sa.Table, metadata: sa.MetaData, engine: sa.engine.Engine, in_db_cls: Callable[..., InDBType]
    ):
        self.table = table
        self.engine = engine
        self.metadata = metadata
        self.in_db_cls = in_db_cls

    def identity(self, id_: IdType):
        return self.table.c.id == id_

    def get(self, workspace_id: int, id_: IdType) -> Optional[InDBType]:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return self.in_db_cls(**row) if row else None

    def get_or_error(self, workspace_id: int, id_: IdType) -> InDBType:
        query = self.table.select().where(self.identity(id_)).limit(1)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        if row:
            return self.in_db_cls(**row)
        else:
            raise NotFoundException("Object not found")

    def create(self, workspace_id: int, obj: CreateType) -> InDBType:
        query = self.table.insert().values(**obj.dict())
        id_ = self._execute_query(query, workspace_id=workspace_id, callback_fn=inserted_primary_key_)
        return self.get_or_error(workspace_id, id_)

    def create_or_update(self, workspace_id: int, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        id_ = getattr(obj, "id_", None)
        if not id_:
            return self.create(workspace_id, cast(CreateType, obj))
        else:
            values_dict = obj.dict(exclude_unset=True)
            if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
                values_dict["updated_at"] = dt.datetime.utcnow()
            query = (
                insert(self.table)
                .values(**values_dict)
                .on_conflict_do_update(constraint=f"{self.table.name}_pkey", set_=values_dict)
            )
            self._execute_query(query, workspace_id=workspace_id)
            return self.get_or_error(workspace_id, id_)

    def insert(self, workspace_id: int, id_: IdType, obj: InDBType) -> InDBType:
        values_dict = obj.dict(exclude_unset=True)
        values_dict["id"] = id_
        if "updated_at" in self.table.columns.keys() and "updated_at" not in values_dict:
            values_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.insert().values(**values_dict)
        self._execute_query(query, workspace_id=workspace_id)
        return self.get_or_error(workspace_id, id_)

    def update(self, workspace_id: int, id_: IdType, obj: UpdateType) -> InDBType:
        update_dict = obj.dict(exclude_unset=True)
        if "updated_at" in self.table.columns.keys() and "updated_at" not in update_dict:
            update_dict["updated_at"] = dt.datetime.utcnow()

        query = self.table.update().where(self.identity(id_)).values(**update_dict)
        self._execute_query(query, workspace_id=workspace_id)
        return self.get_or_error(workspace_id, id_)

    def delete(self, workspace_id: int, id_: IdType) -> int:
        query = self.table.delete().where(self.identity(id_))
        return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)

    def all(self, workspace_id: int) -> Iterable[InDBType]:
        query = self.table.select()
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return (self.in_db_cls(**row) for row in rows)

    def iterate_all(self, workspace_id: int) -> Iterable[InDBType]:
        query = self.table.select()
        with self._connection_with_schema(workspace_id) as connection:
            proxy = connection.execution_options(stream_results=True).execute(query)
            while True:
                batch = proxy.fetchmany(10000)
                if not batch:
                    break
                for row in batch:
                    yield self.in_db_cls(**row)
            proxy.close()

    def truncate(self, workspace_id: int):
        schema_name = self._schema_name(workspace_id)
        query = f"TRUNCATE TABLE `{schema_name}`.`{self.table.name}`;"
        self._execute_query(query, workspace_id=workspace_id)

    def reset_primary_key_id(self, workspace_id: int):
        schema_name = self._schema_name(workspace_id)
        # query = f"ALTER SEQUENCE {schema_name}.{self.table.name}_id_seq RESTART WITH (SELECT max(id)+1 FROM {schema_name}.{self.table.name});"
        query = (
            f"SELECT pg_catalog.setval(pg_get_serial_sequence('{schema_name}.{self.table.name}', 'id'), "
            f"(SELECT coalesce(max(id), 1) FROM {schema_name}.{self.table.name}));"
        )
        self._execute_query(query, workspace_id)

    def _execute_query(
        self, query, workspace_id, values: Optional[List[dict]] = None, callback_fn=lambda result: result
    ):
        with self._connection_with_schema(workspace_id) as connection:
            if values:
                result = connection.execute(query, values)
            else:
                result = connection.execute(query)
            return callback_fn(result)

    def _schema_name(self, workspace_id):
        return f"ws_{workspace_id}"

    def _connection_with_schema(self, workspace_id):
        return self.engine.connect().execution_options(schema_translate_map={None: self._schema_name(workspace_id)})


class SQLUserRepository(UserRepository, SQLRepository[int, UserCreate, UserUpdate, UserInDB]):
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        query = self.table.select().where(self.table.c.email == email)
        result = self._execute_query(query)
        row = result.fetchone()
        return UserInDB(**row) if row else None


class SQLSubscriptionRepository(
    SubscriptionRepository, SQLRepository[int, SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB]
):
    def get_subscriptions_for_user(self, user_id: int) -> List[SubscriptionInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [SubscriptionInDB(**row) for row in rows]


class SQLUserInfoRepository(UserInfoRepository, SQLRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        query = self.table.select().where(
            and_(self.table.c.sub == sub, self.table.c.integration_name == integration_name)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return UserInfoInDB(**row) if row else None

    def get_for_user(self, user_id: int) -> List[UserInfoInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [UserInfoInDB(**row) for row in rows]

    def get_by_email(self, email: str) -> Optional[UserInfoInDB]:
        query = self.table.select().where(self.table.c.email == email)
        row = self._execute_query(query, callback_fn=fetchone_)
        return UserInfoInDB(**row) if row else None


class SQLCredentialRepository(
    CredentialRepository, SQLRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]
):
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        query = self.table.select().where(
            and_(self.table.c.owner_id == owner_id, self.table.c.integration_name == integration_name)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return CredentialInDB(**row) if row else None

    def get_for_user(self, owner_id) -> List[CredentialInDB]:
        query = self.table.select().where(self.table.c.owner_id == owner_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [CredentialInDB(**row) for row in rows]


class SQLWorkspaceRepository(WorkspaceRepository, SQLRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    def get_worskpaces_by_ids(self, workspace_ids: List[int]) -> List[WorkspaceInDB]:
        query = self.table.select().where(self.table.c.id.in_(workspace_ids))
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceInDB(**row) for row in rows]


class SQLWorkspaceMemberRepository(
    WorkspaceMemberRepository,
    SQLRepository[int, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB],
):
    def get_for_user(self, user_id: int) -> List[WorkspaceMemberInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceMemberInDB(**row) for row in rows]

    def get_for_workspace(self, workspace_id: int) -> List[WorkspaceMemberInDB]:
        query = self.table.select().where(self.table.c.workspace_id == workspace_id)
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [WorkspaceMemberInDB(**row) for row in rows]

    def get_for_workspace_and_user(self, workspace_id: int, user_id: int) -> Optional[WorkspaceMemberInDB]:
        query = self.table.select().where(
            and_(self.table.c.workspace_id == workspace_id, self.table.c.user_id == user_id)
        )
        row = self._execute_query(query, callback_fn=fetchone_)
        return WorkspaceMemberInDB(**row) if row else None


class SQLProjectRepository(
    ProjectRepository, SQLWorkspaceScopedRepository[int, ProjectCreate, ProjectUpdate, ProjectInDB]
):
    pass


class SQLRepositoryRepository(
    RepositoryRepository, SQLWorkspaceScopedRepository[int, RepositoryCreate, RepositoryUpdate, RepositoryInDB]
):
    def get_by_clone_url(self, workspace_id: int, clone_url: str) -> Optional[RepositoryInDB]:
        query = self.table.select().where(self.table.c.clone_url == clone_url)
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return RepositoryInDB(**row) if row else None


class SQLProjectRepositoryRepository(
    ProjectRepositoryRepository,
    SQLWorkspaceScopedRepository[int, ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB],
):
    def get_repo_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        query = select([self.table.c.repo_id]).where(self.table.c.project_id == project_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [r["repo_id"] for r in rows]

    def add_repo_ids_to_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        query = self.table.insert()
        self._execute_query(
            query,
            workspace_id=workspace_id,
            values=[{"project_id": project_id, "repo_id": repo_id} for repo_id in repo_ids],
        )

    def remove_repo_ids_from_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        query = self.table.delete().where(self.table.c.repo_id.in_(repo_ids))
        self._execute_query(query, workspace_id=workspace_id)


class SQLAuthorRepository(AuthorRepository, SQLWorkspaceScopedRepository[int, AuthorCreate, AuthorUpdate, AuthorInDB]):
    pass


class SQLTeamRepository(TeamRepository, SQLWorkspaceScopedRepository[int, TeamCreate, TeamUpdate, TeamInDB]):
    pass


class SQLTeamMemberRepository(
    TeamMemberRepository, SQLWorkspaceScopedRepository[int, TeamMemberCreate, TeamMemberUpdate, TeamMemberInDB]
):
    def add_members_to_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> List[TeamMemberInDB]:
        query = self.table.insert([{"team_id": team_id, "author_id": author_id} for author_id in author_ids]).returning(
            self.table.c.id, self.table.c.team_id, self.table.c.author_id
        )
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [TeamMemberInDB(**row) for row in rows]

    def remove_members_from_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> int:
        query = self.table.delete().where(and_(self.table.c.team_id == team_id, self.table.c.author_id.in_(author_ids)))
        return self._execute_query(query, workspace_id=workspace_id, callback_fn=rowcount_)

    def get_author_team_ids(self, workspace_id: int, author_id: int) -> List[int]:
        query = select([self.table.c.team_id]).where(self.table.c.author_id == author_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["team_id"] for row in rows]

    def get_team_member_author_ids(self, workspace_id: int, team_id: int) -> List[int]:
        query = select([self.table.c.author_id]).where(self.table.c.team_id == team_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return [row["author_id"] for row in rows]


class SQLRepoDFMixin:

    if typing.TYPE_CHECKING:
        _connection_with_schema: Callable
        table: sa.Table

    def get_repo_df(self, workspace_id: int, repo_id: int) -> pd.DataFrame:
        with self._connection_with_schema(workspace_id) as connection:
            df = pd.read_sql_query(
                sql=self.table.select().where(self.table.c.repo_id == repo_id),
                con=connection,
            )
            return df


class SQLExtractedCommitRepository(
    SQLRepoDFMixin,
    ExtractedCommitRepository,
    SQLWorkspaceScopedRepository[ExtractedCommitId, ExtractedCommit, ExtractedCommit, ExtractedCommit],
):
    def identity(self, id_: ExtractedCommitId):
        return and_(self.table.c.commit_id == id_.commit_id, self.table.c.repo_id == id_.repo_id)


class SQLExtractedPatchRepository(
    SQLRepoDFMixin,
    ExtractedPatchRepository,
    SQLWorkspaceScopedRepository[ExtractedPatchId, ExtractedPatch, ExtractedPatch, ExtractedPatch],
):
    def identity(self, id_: ExtractedPatchId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.parent_commit_id == id_.parent_commit_id,
            self.table.c.newpath == id_.newpath,
        )


class SQLExtractedPatchRewriteRepository(
    SQLRepoDFMixin,
    ExtractedPatchRewriteRepository,
    SQLWorkspaceScopedRepository[
        ExtractedPatchRewriteId, ExtractedPatchRewrite, ExtractedPatchRewrite, ExtractedPatchRewrite
    ],
):
    def identity(self, id_: ExtractedPatchRewriteId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.newpath == id_.newpath,
            self.table.c.rewritten_commit_id == id_.rewritten_commit_id,
        )


class SQLCalculatedCommitRepository(
    SQLRepoDFMixin,
    CalculatedCommitRepository,
    SQLWorkspaceScopedRepository[CalculatedCommitId, CalculatedCommit, CalculatedCommit, CalculatedCommit],
):
    def identity(self, id_: CalculatedCommitId):
        return and_(self.table.c.commit_id == id_.commit_id, self.table.c.repo_id == id_.repo_id)


class SQLCalculatedPatchRepository(
    SQLRepoDFMixin,
    CalculatedPatchRepository,
    SQLWorkspaceScopedRepository[CalculatedPatchId, CalculatedPatch, CalculatedPatch, CalculatedPatch],
):
    def identity(self, id_: CalculatedPatchId):
        return and_(
            self.table.c.commit_id == id_.commit_id,
            self.table.c.repo_id == id_.repo_id,
            self.table.c.parent_commit_id == id_.parent_commit_id,
            self.table.c.newpath == id_.newpath,
        )


class SQLPullRequestRepository(
    SQLRepoDFMixin,
    PullRequestRepository,
    SQLWorkspaceScopedRepository[PullRequestId, PullRequest, PullRequest, PullRequest],
):
    def identity(self, id_: PullRequestId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.number == id_.number,
        )

    def get_prs_updated_at(self, workspace_id: int, repository_id: int) -> Dict[int, dt.datetime]:
        def _add_utc_timezone(d: dt.datetime):
            return d.replace(tzinfo=dt.timezone.utc)

        query = select([self.table.c.number, self.table.c.updated_at]).where(self.table.c.repo_id == repository_id)
        rows = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchall_)
        return {row["number"]: _add_utc_timezone(row["updated_at"]) for row in rows}


class SQLPullRequestCommitRepository(
    SQLRepoDFMixin,
    PullRequestCommitRepository,
    SQLWorkspaceScopedRepository[PullRequestCommitId, PullRequestCommit, PullRequestCommit, PullRequestCommit],
):
    def identity(self, id_: PullRequestCommitId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.pr_number == id_.pr_number,
            self.table.c.commit_id == id_.commit_id,
        )


class SQLPullRequestCommentRepository(
    SQLRepoDFMixin,
    PullRequestCommentRepository,
    SQLWorkspaceScopedRepository[PullRequestCommentId, PullRequestComment, PullRequestComment, PullRequestComment],
):
    def identity(self, id_: PullRequestCommentId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.pr_number == id_.pr_number,
            self.table.c.comment_type == id_.comment_type,
            self.table.c.comment_id == id_.comment_id,
        )


class SQLPullRequestLabelRepository(
    SQLRepoDFMixin,
    PullRequestLabelRepository,
    SQLWorkspaceScopedRepository[
        PullRequestLabelId,
        PullRequestLabel,
        PullRequestLabel,
        PullRequestLabel,
    ],
):
    def identity(self, id_: PullRequestLabelId):
        return and_(
            self.table.c.repo_id == id_.repo_id,
            self.table.c.pr_number == id_.pr_number,
            self.table.c.name == id_.name,
        )


class SQLEmailLogRepository(EmailLogRepository, SQLRepository[int, EmailLogCreate, EmailLogUpdate, EmailLogInDB]):
    def get_emails_to_send(self) -> List[EmailLogInDB]:
        query = self.table.select().where(
            and_(self.table.c.status == "scheduled", self.table.c.scheduled_at <= dt.datetime.utcnow())
        )
        rows = self._execute_query(query, callback_fn=fetchall_)
        return [EmailLogInDB(**row) for row in rows]

    def email_log_status_update(self, row_id: int, status: str) -> Optional[EmailLogInDB]:
        # query = self.table.update(self.table.c.status).where(self.table.c.id == row_id).values(status)
        # query = self.table.update().where(self.table.c.id == row_id).values(status = "sent")
        #query = self.table.update().values(status="sent")
        query = sa.update('email_log').values({status: "sent"})
        self._execute_query(query, callback_fn=fetchall_)
        return self.get_or_error(row_id)

    def cancel_email(self, user_id: int, template: str) -> Optional[List[EmailLogInDB]]:
        query = (
            self.table.update()
            .where(
                and_(
                    self.table.c.user_id == user_id,
                    self.table.c.template_name == template,
                    self.table.c.status == "scheduled",
                )
            )
            .values(status="canceled")
        )
        self._execute_query(query)
        #return [EmailLogInDB(**row) for row in rows]
        return self.get_or_error(user_id)
