from typing import List

from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes.dashboards import DashboardInDB, DashboardCreate, DashboardUpdate

logger = get_logger(__name__)


def list_dashboards(g: GitentialContext, workspace_id: int) -> List[DashboardInDB]:
    return list(g.backend.dashboards.all(workspace_id=workspace_id))


def get_dashboard(g: GitentialContext, workspace_id: int, dashboard_id: int) -> DashboardInDB:
    return g.backend.dashboards.get_or_error(workspace_id=workspace_id, id_=dashboard_id)


def create_dashboard(g: GitentialContext, workspace_id: int, dashboard_create: DashboardCreate) -> DashboardInDB:
    logger.info("creating dashboard", workspace_id=workspace_id, title=dashboard_create.title)
    return g.backend.dashboards.create(workspace_id, dashboard_create)


def update_project(
    g: GitentialContext, workspace_id: int, dashboard_id: int, dashboard_update: DashboardUpdate
) -> DashboardInDB:
    return g.backend.dashboards.update(workspace_id, dashboard_id, dashboard_update)


def delete_dashboard(g: GitentialContext, workspace_id: int, dashboard_id: int) -> bool:
    g.backend.dashboards.delete(workspace_id=workspace_id, id_=dashboard_id)
    return True
