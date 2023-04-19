from typing import Optional, List
from pathlib import Path
from datetime import datetime
from structlog import get_logger
import base64
from cryptography.fernet import Fernet
from concurrent.futures import ThreadPoolExecutor
from gitential2.core.workspaces import get_workspace_owner
from gitential2.core.emails import send_email_to_address
from gitential2.datatypes.refresh import RefreshStrategy
from gitential2.core.context import GitentialContext
from gitential2.datatypes import AutoExportCreate, AutoExportInDB
from gitential2.core.refresh_v2 import refresh_workspace

# pylint: disable=import-outside-toplevel,cyclic-import
from gitential2.cli_v2.export import export_full_workspace, ExportFormat
from gitential2.cli_v2.jira import lookup_tempo_worklogs

logger = get_logger(__name__)


def encrypting_tempo_access_token(g: GitentialContext, tempo_access_token: str) -> str:
    key = g.settings.secret
    encoded_key = base64.urlsafe_b64encode(key.encode())
    f = Fernet(encoded_key)
    encoded_tempo_access_token = f.encrypt(tempo_access_token.encode())
    encoded_tempo_access_token_str = base64.urlsafe_b64encode(encoded_tempo_access_token).decode("utf-8")

    return encoded_tempo_access_token_str


def create_auto_export(
    g: GitentialContext,
    workspace_id: int,
    emails: List[str],
    **kwargs,
) -> Optional[AutoExportInDB]:

    extra = dict(kwargs)
    if extra.get("tempo_access_token"):
        extra["tempo_access_token"] = encrypting_tempo_access_token(g, extra["tempo_access_token"])
    g.backend.auto_export.create(AutoExportCreate(workspace_id=workspace_id, emails=emails, extra=extra))


def auto_export_workspace(g: GitentialContext, workspace_to_export: AutoExportInDB):
    refresh_workspace(g=g, workspace_id=workspace_to_export.workspace_id, strategy=RefreshStrategy.one_by_one)
    if workspace_to_export.extra.get("tempo_access_token"):
        export_params = workspace_to_export.extra
        lookup_tempo_worklogs(
            g=g,
            workspace_id=workspace_to_export.workspace_id,
            tempo_access_token=export_params["tempo_access_token"],
            date_from=export_params["date_from"],
        )
    export_full_workspace(
        workspace_id=workspace_to_export.workspace_id,
        export_format=ExportFormat.xlsx,
        date_from=export_params["date_from"],
    )


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
