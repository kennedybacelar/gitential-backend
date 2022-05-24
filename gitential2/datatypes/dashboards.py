from typing import Optional, List, Dict, Any, Tuple

from pydantic import Field

from gitential2.datatypes.stats import FilterName, QueryType
from .charts import ChartPublic
from .common import IDModelMixin, CoreModel, DateTimeModelMixin, ExtraFieldMixin
from .export import ExportableModel


class DashboardConfig(CoreModel):
    type: QueryType
    filters: Dict[FilterName, Any]


class DashboardBase(ExtraFieldMixin, CoreModel):
    title: Optional[str]
    config: DashboardConfig
    charts: List[ChartPublic]


class DashboardPublic(IDModelMixin, DateTimeModelMixin, DashboardBase):
    pass


class DashboardCreate(DashboardBase):
    title: str = Field(..., min_length=2, max_length=128)


class DashboardCreateRequest(CoreModel):
    title: Optional[str]
    config: DashboardConfig
    charts: List[int]


class DashboardUpdate(DashboardBase):
    pass


class DashboardUpdateRequest(DashboardCreateRequest):
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
