from typing import List, Tuple

from gitential2.datatypes.export import ExportableModel
from gitential2.datatypes.repositories import RepositoryBase
from .common import IDModelMixin, DateTimeModelMixin


class UserRepositoriesCacheBase(RepositoryBase):
    user_id: int


class UserRepositoriesCacheCreate(UserRepositoriesCacheBase):
    pass


class UserRepositoriesCacheUpdate(UserRepositoriesCacheBase):
    pass


class UserRepositoriesCacheInDB(IDModelMixin, DateTimeModelMixin, UserRepositoriesCacheBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "id",
            "user_id" "created_at",
            "updated_at",
            "clone_url",
            "protocol",
            "name",
            "namespace",
            "private",
            "integration_type",
            "integration_name",
            "credential_id",
            "extra",
        ]

    def export_names(self) -> Tuple[str, str]:
        return "user_repositories_cache", "user_repositories_cache"
