from datetime import datetime
from typing import List, Tuple

from gitential2.datatypes import CoreModel
from gitential2.datatypes.export import ExportableModel
from .common import IDModelMixin, DateTimeModelMixin


class UserRepositoriesCacheLastRefreshBase(CoreModel):
    user_id: int
    last_refresh: datetime


class UserRepositoriesCacheLastRefreshCreate(UserRepositoriesCacheLastRefreshBase):
    pass


class UserRepositoriesCacheLastRefreshUpdate(UserRepositoriesCacheLastRefreshBase):
    pass


class UserRepositoriesCacheLastRefreshInDB(
    IDModelMixin, DateTimeModelMixin, UserRepositoriesCacheLastRefreshBase, ExportableModel
):
    def export_fields(self) -> List[str]:
        return [
            "id",
            "user_id",
            "last_refresh",
        ]

    def export_names(self) -> Tuple[str, str]:
        return "user_repositories_cache_last_refresh", "user_repositories_cache_last_refreshes"
