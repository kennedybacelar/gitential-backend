from typing import List

from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes.charts import ChartInDB, ChartCreate, ChartUpdate, ChartPublic
from gitential2.datatypes.dashboards import (
    DashboardInDB,
    DashboardUpdate,
    DashboardCreate,
)
from gitential2.exceptions import SettingsException

logger = get_logger(__name__)


def list_dashboards(g: GitentialContext, workspace_id: int) -> List[DashboardInDB]:
    return list(g.backend.dashboards.all(workspace_id=workspace_id))


def get_dashboard(g: GitentialContext, workspace_id: int, dashboard_id: int) -> DashboardInDB:
    return g.backend.dashboards.get_or_error(workspace_id=workspace_id, id_=dashboard_id)


def get_chart_public_from_chart_in_db(chart_in_db: ChartInDB) -> ChartPublic:
    return ChartPublic(
        id=chart_in_db.id,
        created_at=chart_in_db.created_at,
        updated_at=chart_in_db.updated_at,
        extra=chart_in_db.extra,
        is_custom=True,
        title=chart_in_db.title,
        chart_type=chart_in_db.chart_type,
        layout=chart_in_db.layout,
        metrics=chart_in_db.metrics,
        dimensions=chart_in_db.dimensions,
    )


def create_dashboard(g: GitentialContext, workspace_id: int, dashboard_create: DashboardCreate) -> DashboardInDB:
    if not dashboard_create.charts:
        raise SettingsException("Can not create dashboard with no charts!")
    logger.info("creating dashboard", workspace_id=workspace_id, title=dashboard_create.title)
    charts = [get_chart(g, workspace_id, chart.id) for chart in dashboard_create.charts]
    d = DashboardCreate(
        title=dashboard_create.title,
        filters=dashboard_create.filters,
        charts=[get_chart_public_from_chart_in_db(c_in_db) for c_in_db in charts],
    )
    return g.backend.dashboards.create(workspace_id, d)


def update_dashboard(
    g: GitentialContext, workspace_id: int, dashboard_id: int, dashboard_update: DashboardUpdate
) -> DashboardInDB:
    if not dashboard_update.charts:
        raise SettingsException("Can not update dashboard with no charts!")
    charts = [get_chart(g, workspace_id, chart.id) for chart in dashboard_update.charts]
    d = DashboardUpdate(
        title=dashboard_update.title,
        filters=dashboard_update.filters,
        charts=[get_chart_public_from_chart_in_db(c_in_db) for c_in_db in charts],
    )
    return g.backend.dashboards.update(workspace_id, dashboard_id, d)


def delete_dashboard(g: GitentialContext, workspace_id: int, dashboard_id: int) -> bool:
    g.backend.dashboards.delete(workspace_id=workspace_id, id_=dashboard_id)
    return True


# Chart


def list_charts(g: GitentialContext, workspace_id: int) -> List[ChartInDB]:
    return list(g.backend.charts.all(workspace_id=workspace_id))


def get_chart(g: GitentialContext, workspace_id: int, chart_id: int) -> ChartInDB:
    return g.backend.charts.get_or_error(workspace_id=workspace_id, id_=chart_id)


def create_chart(g: GitentialContext, workspace_id: int, chart_create: ChartCreate) -> ChartInDB:
    logger.info("creating chart", workspace_id=workspace_id, title=chart_create.title)
    chart_create.is_custom = True
    return g.backend.charts.create(workspace_id, chart_create)


def update_chart(g: GitentialContext, workspace_id: int, chart_id: int, chart_update: ChartUpdate) -> ChartInDB:
    if chart_id < 0:
        raise SettingsException("Can not update not custom chart!")
    logger.info("updating chart", workspace_id=workspace_id, title=chart_update.title)
    chart_update.is_custom = True
    chart_updated = g.backend.charts.update(workspace_id, chart_id, chart_update)
    for d in list(g.backend.dashboards.all(workspace_id=workspace_id)):
        dashboard_chart_ids = [c.id for c in d.charts]
        is_dashboard_need_to_be_updated = any(cid == chart_id for cid in dashboard_chart_ids)
        if is_dashboard_need_to_be_updated:
            dashboard_update = DashboardUpdate(title=d.title, filters=d.filters, charts=d.charts)
            update_dashboard(g, workspace_id=workspace_id, dashboard_id=d.id, dashboard_update=dashboard_update)
    return chart_updated


def delete_chart(g: GitentialContext, workspace_id: int, chart_id: int) -> bool:
    dashboards = list(g.backend.dashboards.all(workspace_id=workspace_id))
    is_chart_exists_in_dashboards = all(all(c.id != chart_id for c in d.charts) for d in dashboards)
    if not dashboards or is_chart_exists_in_dashboards:
        delete_result = g.backend.charts.delete(workspace_id=workspace_id, id_=chart_id)
        if not delete_result:
            logger.info(f"Chart delete failed! Not able to find chart with id: {chart_id}")
        return bool(delete_result)
    logger.info(f"Chart delete failed! Chart is already in one of the dashboards! chart_id={chart_id}")
    return False
