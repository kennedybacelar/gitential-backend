from typing import Optional, List
from pathlib import Path
from datetime import datetime
from structlog import get_logger
import base64
import tempfile
from cryptography.fernet import Fernet
from concurrent.futures import ThreadPoolExecutor
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


def decrypting_tempo_access_token(g: GitentialContext, encrypted_tempo_access_token: str) -> str:
    key = g.settings.secret
    encoded_key = base64.urlsafe_b64encode(key.encode())
    f = Fernet(encoded_key)
    decoded_tempo_access_token = base64.urlsafe_b64decode(encrypted_tempo_access_token.encode())
    decrypted_tempo_access_token = f.decrypt(decoded_tempo_access_token).decode()

    return decrypted_tempo_access_token


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
    logger.info("Auto export process started for workspace", workspace_id=workspace_to_export.workspace_id)
    refresh_workspace(g=g, workspace_id=workspace_to_export.workspace_id, strategy=RefreshStrategy.one_by_one)
    export_params = workspace_to_export.extra
    if workspace_to_export.extra.get("tempo_access_token"):
        lookup_tempo_worklogs(
            g=g,
            workspace_id=workspace_to_export.workspace_id,
            tempo_access_token=decrypting_tempo_access_token(g, export_params["tempo_access_token"]),
            date_from=export_params["date_from"],
        )
    with tempfile.TemporaryDirectory() as tmp_dir:
        logger.info(f"Export file temporarily stored in {tmp_dir}", workspace_id=workspace_to_export.workspace_id)
        export_full_workspace(
            workspace_id=workspace_to_export.workspace_id,
            export_format=ExportFormat.xlsx,
            date_from=export_params["date_from"],
            destination_directory=Path(tmp_dir),
            upload_to_aws_s3=True,
            aws_s3_location=export_params.get("aws_s3_location") or _generate_aws_s3_location_path(),
        )


def process_auto_export_for_all_workspaces(
    g: GitentialContext,
) -> bool:
    workspaces_to_be_exported = g.backend.auto_export.all()
    with ThreadPoolExecutor() as executor:
        for workspace_to_export in workspaces_to_be_exported:
            if datetime.now().weekday() in workspace_to_export.extra.get("weekday_numbers", []):
                executor.submit(auto_export_workspace, g, workspace_to_export)


def _generate_aws_s3_location_path():
    return Path("Exports/production-cloud/")


def _dispatch_workspace_data_via_email(g: GitentialContext, recipient_list: list, s3_upload_url: str):
    for recipient in recipient_list:
        send_email_to_address(g, recipient, "export_workspace", s3_upload_url=s3_upload_url)
    logger.info(msg="Email dispatch complete...")
