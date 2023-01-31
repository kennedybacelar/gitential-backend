from typing import List, Tuple, Optional

from gitential2.datatypes.export import ExportableModel
from gitential2.datatypes.repositories import GitProtocol
from .common import IDModelMixin, DateTimeModelMixin, ExtraFieldMixin, CoreModel


class UserRepositoryCacheBase(ExtraFieldMixin, CoreModel):
    user_id: int
    clone_url: str
    protocol: GitProtocol
    name: str = ""
    namespace: str = ""
    private: bool = False
    integration_type: Optional[str] = None
    integration_name: Optional[str] = None
    credential_id: Optional[int] = None


class UserRepositoryCacheCreate(UserRepositoryCacheBase):
    pass


class UserRepositoryCacheUpdate(UserRepositoryCacheBase):
    pass


class UserRepositoryCacheInDB(IDModelMixin, DateTimeModelMixin, UserRepositoryCacheBase, ExportableModel):
    def export_fields(self) -> List[str]:
        return [
            "id",
            "user_id",
            "created_at",
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
        return "user_repositories_cache", "user_repositories_caches"
