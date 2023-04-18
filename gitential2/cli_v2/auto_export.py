from datetime import datetime
from typing import Optional, List
from structlog import get_logger
import typer
from gitential2.core.export import create_auto_export, process_auto_export_for_all_workspaces
from .common import get_context


logger = get_logger(__name__)
app = typer.Typer()


@app.command("create")
def create_auto_export_(
    workspace_id: int,
    emails: List[str],
    tempo_access_token: Optional[str] = typer.Option(None, "--tempo-access-token"),
    date_from: Optional[datetime] = typer.Option(None, "--date-from"),
):
    """ """
    g = get_context()
    workspace = g.backend.workspaces.get(id_=workspace_id)
    if workspace:
        create_auto_export(g, workspace_id, emails, tempo_access_token=tempo_access_token, date_from=date_from)
    else:
        logger.info(f"Workspace {workspace_id} not found")
        raise typer.Exit(code=1)
