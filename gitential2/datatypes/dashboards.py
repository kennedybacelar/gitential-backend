from enum import Enum
from typing import Optional, List, Dict, Any, Tuple

from pydantic import Field

from gitential2.datatypes.stats import MetricName, DimensionName, FilterName, QueryType
from .common import IDModelMixin, CoreModel, DateTimeModelMixin, ExtraFieldMixin
from .export import ExportableModel


class ChartVisualizationTypes(str, Enum):
    chart_line_chart_bar = "chart-line_chart-bar"
    chart_bubble = "chart-bubble"
    chart_pie = "chart-pie"
    chart_stacked_bar = "chart-stacked_bar"
    table = "table"


class DashboardChartLayout(CoreModel):
    x: int
    y: int
    w: int
    h: int


class DashboardConfig(CoreModel):
    type: QueryType
    filters: Dict[FilterName, Any]


class DashboardChartDetails(CoreModel):
    layout: DashboardChartLayout
    title: str
    chart_type: ChartVisualizationTypes
    metrics: List[MetricName]
    dimensions: List[DimensionName]


class DashboardBase(ExtraFieldMixin, CoreModel):
    title: Optional[str]
    config: DashboardConfig
    charts: List[DashboardChartDetails]


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
