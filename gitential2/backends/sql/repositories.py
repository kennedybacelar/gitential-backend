from typing import Optional, Callable, List
import datetime as dt
import sqlalchemy as sa
from sqlalchemy.sql import and_

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
    WorkspacePermissionCreate,
    WorkspacePermissionUpdate,
    WorkspacePermissionInDB,
)

from ..common import (
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

    def _execute_query(self, query):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return result


class SQLUserRepository(UserRepository, SQLRepository[int, UserCreate, UserUpdate, UserInDB]):
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        return None


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
    pass


class SQLWorkspaceRepository(WorkspaceRepository, SQLRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    pass


class SQLWorkspacePermissionRepository(
    WorkspacePermissionRepository,
    SQLRepository[int, WorkspacePermissionCreate, WorkspacePermissionUpdate, WorkspacePermissionInDB],
):
    def get_for_user(self, user_id: int) -> List[WorkspacePermissionInDB]:
        query = self.table.select().where(self.table.c.user_id == user_id)
        result = self._execute_query(query)
        return [WorkspacePermissionInDB(**row) for row in result.fetchall()]
