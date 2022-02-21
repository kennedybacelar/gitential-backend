from typing import Optional, cast
import sys
import logging

from structlog import get_logger
import typer


from gitential2.datatypes.credentials import CredentialInDB
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import get_update_token_callback, get_fresh_credential, list_credentials_for_workspace
from gitential2.integrations.vsts import VSTSIntegration
from .common import get_context, print_results, OutputFormat

from gitential2.cli_v2.common import get_context
from gitential2.settings import IntegrationType

from gitential2.core.workspaces import get_own_workspaces, get_workspace

app = typer.Typer()
logger = get_logger(__name__)


def _get_vsts_credential(g: GitentialContext, workspace_id: int, integration_name="vsts") -> Optional[CredentialInDB]:
    return get_fresh_credential(g, workspace_id=workspace_id, integration_name=integration_name)


@app.command("list-available-projects")
def list_available_projects(
    workspace_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    vsts_credential: CredentialInDB = _get_vsts_credential(g, workspace_id)
    vsts_integration = g.integrations.get("vsts")

    for single_user in g.backend.user_infos.get_for_user(vsts_credential.owner_id):
        if single_user.integration_type == IntegrationType.vsts:
            userinfo: UserInfoInDB = single_user
            break

    if vsts_credential and vsts_integration:
        vsts_integration = cast(VSTSIntegration, vsts_integration)
        token = vsts_credential.to_token_dict(g.fernet)
        its_projects = vsts_integration.list_available_its_projects(
            token,
            update_token=get_update_token_callback(g, vsts_credential),
            provider_user_id=userinfo.sub if userinfo else None,
        )
        print_results(its_projects, format_=format_, fields=fields)
