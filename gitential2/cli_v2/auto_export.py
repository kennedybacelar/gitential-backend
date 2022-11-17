import typer
from structlog import get_logger
from gitential2.core.export import create_auto_export
from .common import get_context
from typing import Optional, List

logger = get_logger(__name__)
app = typer.Typer()


@app.command("create")
def create_auto_export_(
    workspace_id: int, 
    cron_schedule_time:int, 
    emails:List[str], 
    tempo_access_token: Optional[str] = None
    ):
    """
    With this command you can create a new scheduled automatic workspace export for a workspace. This schedule will only be created for a workspace that exists, and for a cron schedule time that hasn't already been created for the workspace.

    @args: 
    <workspace_id>: The id of the workspace to be added
    <cron_schedule_time>: The time of day to launch the export
    <tempo_access_token>: The tempo access token if tempo is being used by the workspace
    <emails>: The list of email recipients

    @example:
    $ g2 auto-export create <workspace_id> <cron_schedule_hour> <tempo_access_token> <emails>
    """
    g = get_context()

    # Validate CLI Inputs
    if cron_schedule_time > 23 or cron_schedule_time < 0:
        logger.error(error = f"{cron_schedule_time} is not a valid value for cron_schedule_time")
        raise typer.Exit(code=1)

    # Validate Workspace ID
    if workspace_id not in [workspace.id for workspace in g.backend.workspaces.all()]:
        logger.error(error = f"Workspace id {workspace_id} does not exist")
        raise typer.Exit(code=1)


    auto_export_data = create_auto_export(g, workspace_id, cron_schedule_time, tempo_access_token, emails)
    
    if auto_export_data == None:
        logger.error(error = f"Schedule already exists on wokspace {workspace_id}")
        raise typer.Exit(code=1)

    logger.info(msg = f"New schedule created for workspace {workspace_id}")
    logger.info(msg = auto_export_data)
