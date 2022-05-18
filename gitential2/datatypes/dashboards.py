from typing import Optional, List, Tuple

from pydantic import Field

from .common import IDModelMixin, CoreModel, DateTimeModelMixin, ExtraFieldMixin
from .export import ExportableModel


class DashboardBase(ExtraFieldMixin, CoreModel):
    title: Optional[str]
    config: str
    charts: str


class DashboardPublic(IDModelMixin, DateTimeModelMixin, DashboardBase):
    pass


class DashboardCreate(DashboardBase):
    title: str = Field(..., min_length=2, max_length=128)


class DashboardUpdate(DashboardBase):
    pass


class DashboardInDB(IDModelMixin, DateTimeModelMixin, DashboardBase, ExportableModel):
    def export_names(self) -> Tuple[str, str]:
        return "dashboard", "dashboards"

    def export_fields(self) -> List[str]:
        return [
            "id",
            "title",
            "created_at",
            "updated_at",
            "config",
            "charts",
            "extra",
        ]
