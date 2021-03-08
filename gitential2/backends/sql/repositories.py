from typing import Iterable, Optional, Callable, List, cast
import datetime as dt
import sqlalchemy as sa
from sqlalchemy.sql import and_, select
from sqlalchemy.sql.selectable import Alias

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
from gitential2.datatypes import authors
from gitential2.datatypes.authors import AuthorCreate, AuthorInDB, AuthorUpdate, AuthorAlias
from gitential2.backends.base.repositories import AuthorRepository, NotFoundException
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate, ProjectInDB
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate, RepositoryInDB
from gitential2.datatypes.project_repositories import (
    ProjectRepositoryCreate,
    ProjectRepositoryUpdate,
    ProjectRepositoryInDB,
)
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB
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
)


class SQLRepository(BaseRepository[IdType, CreateType, UpdateType, InDBType]):  # pylint: disable=unsubscriptable-object
    def __init__(self, table: sa.Table, engine: sa.engine.Engine, in_db_cls: Callable[..., InDBType]):
        self.table = table
        self.engine = engine
        self.in_db_cls = in_db_cls
        self.identity = lambda id_: (self.table.c.id == id_)

    def get(self, id_: IdType) -> Optional[InDBType]:
        query = self.table.select().where(self.identity(id_)).limit(1)
        result = self._execute_query(query)
        row = result.fetchone()
        return self.in_db_cls(**row) if row else None

    def get_or_error(self, id_: IdType) -> InDBType:
        query = self.table.select().where(self.identity(id_)).limit(1)
        result = self._execute_query(query)
        row = result.fetchone()
        return self.in_db_cls(**row)

    def create(self, obj: CreateType) -> InDBType:
        query = self.table.insert().values(**obj.dict())
        result = self._execute_query(query)
        id_ = result.inserted_primary_key[0]
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
        result = self._execute_query(query)
        return result.rowcount

    def all(self) -> Iterable[InDBType]:
        query = self.table.select()
        result = self._execute_query(query)
        return (self.in_db_cls(**row) for row in result.fetchall())

    def _execute_query(self, query):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return result


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
        self.identity = lambda id_: (self.table.c.id == id_)

    def get(self, workspace_id: int, id_: IdType) -> Optional[InDBType]:
        query = self.table.select().where(self.identity(id_)).limit(1)
        result = self._execute_query(query, workspace_id=workspace_id)
        row = result.fetchone()
        return self.in_db_cls(**row) if row else None

    def get_or_error(self, workspace_id: int, id_: IdType) -> InDBType:
        query = self.table.select().where(self.identity(id_)).limit(1)
        result = self._execute_query(query, workspace_id=workspace_id)
        row = result.fetchone()
        return self.in_db_cls(**row)

    def create(self, workspace_id: int, obj: CreateType) -> InDBType:
        query = self.table.insert().values(**obj.dict())
        result = self._execute_query(query, workspace_id=workspace_id)
        id_ = result.inserted_primary_key[0]
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
        result = self._execute_query(query, workspace_id=workspace_id)
        return result.rowcount

    def all(self, workspace_id: int) -> Iterable[InDBType]:
        query = self.table.select()
        result = self._execute_query(query, workspace_id=workspace_id)
        return (self.in_db_cls(**row) for row in result.fetchall())

    def _execute_query(self, query, workspace_id, values: Optional[List[dict]] = None):
        with self.engine.connect().execution_options(
            schema_translate_map={None: self._schema_name(workspace_id)}
        ) as connection:
            if values:
                result = connection.execute(query, values)
            else:
                result = connection.execute(query)
            return result

    def _schema_name(self, workspace_id):
        return f"ws_{workspace_id}"


class SQLUserRepository(UserRepository, SQLRepository[int, UserCreate, UserUpdate, UserInDB]):
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        query = self.table.select().where(self.table.c.email == email)
        result = self._execute_query(query)
        row = result.fetchone()
        return UserInDB(**row) if row else None


class SQLSubscriptionRepository(SubscriptionRepository, SQLRepository[int, UserCreate, UserUpdate, UserInDB]):
    pass


class SQLUserInfoRepository(UserInfoRepository, SQLRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        query = self.table.select().where(
            and_(self.table.c.sub == sub, self.table.c.integration_name == integration_name)
        )
        result = self._execute_query(query)
        row = result.fetchone()
        return UserInfoInDB(**row) if row else None


class SQLCredentialRepository(
    CredentialRepository, SQLRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]
):
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        query = self.table.select().where(
            and_(self.table.c.owner_id == owner_id, self.table.c.integration_name == integration_name)
        )
        result = self._execute_query(query)
        row = result.fetchone()
        return CredentialInDB(**row) if row else None

    def get_for_user(self, owner_id) -> List[CredentialInDB]:
        query = self.table.select().where(self.table.c.owner_id == owner_id)
        result = self._execute_query(query)
        return [CredentialInDB(**row) for row in result.fetchall()]


class SQLWorkspaceRepository(WorkspaceRepository, SQLRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    def get_worskpaces_by_ids(self, workspace_ids: List[int]) -> List[WorkspaceInDB]:
        query = self.table.select().where(self.table.c.id.in_(workspace_ids))
        result = self._execute_query(query)
        return [WorkspaceInDB(**row) for row in result.fetchall()]


class SQLWorkspaceMemberRepository(
    WorkspaceMemberRepository,
    SQLRepository[int, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB],
):
    def get_for_user(self, user_id: int) -> List[WorkspaceMemberInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        result = self._execute_query(query)
        return [WorkspaceMemberInDB(**row) for row in result.fetchall()]

    def get_for_workspace(self, workspace_id: int) -> List[WorkspaceMemberInDB]:
        query = self.table.select().where(self.table.c.workspace_id == workspace_id)
        result = self._execute_query(query)
        return [WorkspaceMemberInDB(**row) for row in result.fetchall()]

    def get_for_workspace_and_user(self, workspace_id: int, user_id: int) -> Optional[WorkspaceMemberInDB]:
        query = self.table.select().where(
            and_(self.table.c.workspace_id == workspace_id, self.table.c.user_id == user_id)
        )
        result = self._execute_query(query)
        row = result.fetchone()
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
        result = self._execute_query(query, workspace_id=workspace_id)
        row = result.fetchone()
        return RepositoryInDB(**row) if row else None


class SQLProjectRepositoryRepository(
    ProjectRepositoryRepository,
    SQLWorkspaceScopedRepository[int, ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB],
):
    def get_repo_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        query = select([self.table.c.repo_id]).where(self.table.c.project_id == project_id)
        result = self._execute_query(query, workspace_id=workspace_id)
        rows = result.fetchall()
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
    def get_or_create_author_for_alias(self, workspace_id: int, alias: AuthorAlias) -> AuthorInDB:
        query = self.metadata.tables["author_aliases"].select(
            self.metadata.tables["author_aliases"].c.email == alias.email
        )
        result = self._execute_query(query, workspace_id=workspace_id)
        row = result.fetchone()
        if row:
            author_id = row.author_id
            return self.get_or_error(workspace_id=workspace_id, id_=cast(int, author_id))
        else:
            return self.create(workspace_id=workspace_id, obj=AuthorCreate(active=True, aliases=[alias]))

    def get(self, workspace_id: int, id_: IdType) -> Optional[AuthorInDB]:
        alias_table = self.metadata.tables["author_aliases"]
        query = self.table.select().where(self.identity(id_)).limit(1)
        result = self._execute_query(query, workspace_id=workspace_id)
        row = result.fetchone()
        if row["id"]:
            alias_query = alias_table.select(alias_table.c.author_id == row["id"])
            alias_result = self._execute_query(alias_query, workspace_id=workspace_id)
            alias_rows = alias_result.fetchall()
            aliases = [AuthorAlias(name=row["name"], email=row["email"]) for row in alias_rows]
            return AuthorInDB(active=row["active"], id=row["id"], aliases=aliases)
        else:
            return None

    def get_or_error(self, workspace_id: int, id_: IdType) -> AuthorInDB:
        author = self.get(workspace_id, id_)
        if not author:
            raise NotFoundException
        else:
            return author

    def create(self, workspace_id: int, obj: CreateType) -> AuthorInDB:
        alias_table = self.metadata.tables["author_aliases"]

        values = obj.dict()
        aliases = values.pop("aliases")
        print("values:", values, "aliases:", aliases, self.table)
        query = self.table.insert().values(values)
        result = self._execute_query(query, workspace_id=workspace_id)
        id_ = result.inserted_primary_key[0]
        # dealing with the aliases
        alias_values = [{"name": alias["name"], "email": alias["email"], "author_id": id_} for alias in aliases]
        alias_query = alias_table.insert(alias_values)
        self._execute_query(alias_query, workspace_id=workspace_id)
        # returning with get
        return self.get_or_error(workspace_id, id_)

    # def update(self, workspace_id: int, id_: IdType, obj: UpdateType) -> InDBType:
    #     update_dict = obj.dict(exclude_unset=True)
    #     if "updated_at" in self.table.columns.keys() and "updated_at" not in update_dict:
    #         update_dict["updated_at"] = dt.datetime.utcnow()

    #     query = self.table.update().where(self.identity(id_)).values(**update_dict)
    #     self._execute_query(query, workspace_id=workspace_id)
    #     return self.get_or_error(workspace_id, id_)
