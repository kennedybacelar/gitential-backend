import tempfile
from gitential2.cli_v2.jira import lookup_tempo_worklogs
from gitential2.core.extended_email_service import ExtendedEmailService
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.core.context import GitentialContext
from datetime import datetime
from pathlib import Path
from enum import Enum
from gitential2.core.workspaces import get_workspace_owner
from structlog import get_logger
from gitential2.datatypes import (AutoExportCreate, AutoExportInDB)
from typing import Optional, List
from gitential2.core.refresh_v2 import refresh_workspace
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType

logger = get_logger(__name__)

class ExportFormat(str, Enum):
    csv = "csv"
    json = "json"
    sqlite = "sqlite"
    xlsx = "xlsx"

def create_auto_export(
    g: GitentialContext, workspace_id: int, cron_schedule_time:int, tempo_access_token: Optional[str], emails:List[str]) -> AutoExportInDB:
    """
    @desc: create a new scheduled automatic workspace export for a workspace.
    @args: workspace_id, cron_schedule_time, tempo_access_token, emails (List of email recepients)
    """
    # Data input valudation
    auto_export_data = AutoExportCreate(
        workspace_id = workspace_id,
        cron_schedule_time = cron_schedule_time,
        tempo_access_token = tempo_access_token,
        emails = emails
    )
    return g.backend.auto_export.create(auto_export_data= auto_export_data)

def auto_export_task(    
    g: GitentialContext,
    ) -> None:
    from gitential2.cli_v2.export import export_full_workspace
    """
    @desc: workspace auto refresh function to be triggered by the celery beat conf
    - Fetch all workspaces in the auto_export schedule table
    - Verify the crontab schedule time, and run if within the current run time
    - Run step by step auto export:
        1. Run the workspace refresh process (wait until it finishes; we set a one-by-one refresh strategy and force refresh)
        2. Tempo refresh process (wait until it finishes)
        3. Run the full-workspace data export process with the uploading to the s3 bucket

    @args: g: GitentialContext
    """
    for workspace in g.backend.auto_export.all():
        if workspace.cron_schedule_time == datetime.now().hour and not workspace.is_exported:
            # Refresh full workspace
            logger.info(msg = f"Starting full workspace refresh for workspace {workspace.workspace_id}....")
            refresh_workspace(g, workspace.workspace_id, RefreshStrategy.one_by_one, RefreshType.everything, True )

            # Refresh Tempo Data
            logger.info(msg = f"Starting Tempo data refresh for workspace {workspace.workspace_id}....")
            if workspace.tempo_access_token:
                lookup_tempo_worklogs(g, workspace.workspace_id, workspace.tempo_access_token, True)

            # Export full workspace
            logger.info(msg = f"Starting full workspace export for workspace {workspace.workspace_id}....")
            export_full_workspace(
                workspace_id=workspace.workspace_id,
                export_format=ExportFormat.xlsx,
                date_from = datetime.now().min,
                upload_to_aws_s3 = True,
                aws_s3_location = _generate_destination_path(g, workspace.workspace_id)
                )
            logger.info(msg = f"Storage on AWS s3 Complete!:).......")  

            # Send Exported Sheet via Email
            logger.info(msg = f"Starting Email Dispatch.......")
            _dispatch_workspace_data_via_email(workspace.workspace_id, workspace.emails)

            # Update the export schedule row to avoid double exports
            logger.info(msg = f"Updating Export Schedule for workspace {workspace.workspace_id}....")
            update_export_status(g, workspace.id, True)
            
            logger.info(msg = f"Export Completed! :)")

def _dispatch_workspace_data_via_email(workspace_id: int, recipients):
    print("Preparing to dispatch reports to admin via email")
    source_path = tempfile.gettempdir() + f"/ws_{workspace_id}_export.xlsx"
    template_name = 'export_workspace'
    email_dispatch_service = ExtendedEmailService(g, recipients, source_path, template_name)
    email_dispatch_service.email_to_many()

def _generate_destination_path(g, workspace_id):
    date = datetime.utcnow()
    return Path(f"Data Exports/production-cloud/{str(date)[:10]} {get_workspace_owner(g, workspace_id).full_name}")

def update_export_status(g: GitentialContext, row_id: int, status: bool)-> bool: 
    g.backend.auto_export.update_export_status(g,row_id, status)
    return True
