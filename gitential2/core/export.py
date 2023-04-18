from typing import Optional, List
from pathlib import Path
from datetime import datetime
from structlog import get_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
from gitential2.core.workspaces import get_workspace_owner
from gitential2.core.emails import send_email_to_address
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.core.context import GitentialContext
from gitential2.datatypes import AutoExportCreate, AutoExportInDB
from gitential2.core.refresh_v2 import refresh_workspace

# pylint: disable=import-outside-toplevel,cyclic-import
from gitential2.cli_v2.export import export_full_workspace, ExportFormat
from gitential2.cli_v2.jira import lookup_tempo_worklogs

logger = get_logger(__name__)


def create_auto_export(
    g: GitentialContext,
    workspace_id: int,
    emails: List[str],
    **kwargs,
) -> Optional[AutoExportInDB]:
    """
    @desc: create a new scheduled automatic workspace export for a workspace.
    @args: workspace_id, cron_schedule_time, tempo_access_token, emails (List of email recipients)
    """
    # Data input validation
    g.backend.auto_export.create(AutoExportCreate(workspace_id=workspace_id, emails=emails, extra=dict(kwargs)))


def auto_export_workspace(g: GitentialContext, workspace_to_export: AutoExportInDB):
    refresh_workspace(g=g, workspace_id=workspace_to_export.workspace_id, strategy=RefreshStrategy.one_by_one)


def process_auto_export_for_all_workspaces(
    g: GitentialContext,
) -> bool:
    workspaces_to_be_exported = g.backend.auto_export.all()
    with ThreadPoolExecutor() as executor:
        for workspace_to_export in workspaces_to_be_exported:
            executor.submit(auto_export_workspace, g, workspace_to_export)


def _dispatch_workspace_data_via_email(g: GitentialContext, recipient_list: list, s3_upload_url: str):
    for recipient in recipient_list:
        send_email_to_address(g, recipient, "export_workspace", s3_upload_url=s3_upload_url)
    logger.info(msg="Email dispatch complete...")


def _generate_destination_path(g, workspace_id):
    date = datetime.utcnow()
    return Path(f"Exports/production-cloud/{str(date)[:10]}-{get_workspace_owner(g, workspace_id).full_name}")


def construct_aws_object_url(bucket_name, region: str, destination_path: str, workspace_id: int):
    return (
        f"https://{bucket_name}.s3.{region}.amazonaws.com/{parse_path(destination_path)}/ws_{workspace_id}_export.xlsx"
    )


def parse_path(path_string: str):
    return str(path_string).replace(" ", "+")
