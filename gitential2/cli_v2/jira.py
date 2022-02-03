from typing import Optional, cast
import sys
import logging

from structlog import get_logger
import typer


from gitential2.datatypes.credentials import CredentialInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import get_update_token_callback, get_fresh_credential
from gitential2.integrations.jira import JiraIntegration
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)

# log = logging.getLogger("authlib")
# log.addHandler(logging.StreamHandler(sys.stdout))
# log.setLevel(logging.DEBUG)


def _get_jira_credential(g: GitentialContext, workspace_id: int, integration_name="jira") -> Optional[CredentialInDB]:
    return get_fresh_credential(g, workspace_id=workspace_id, integration_name=integration_name)


@app.command("list-accessible-resources")
def list_accessible_resources(workspace_id: int):
    log = logging.getLogger("authlib")
    log.addHandler(logging.StreamHandler(sys.stdout))
    log.setLevel(logging.DEBUG)

    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    if jira_credential and jira_integration:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        # jira_integration.list_accessible_resources(token, get_update_token_callback(g, jira_credential))
        # token = jira_integration.refresh_token(token, get_update_token_callback(g, jira_credential))
        # print(token)
        sites = jira_integration.list_accessible_resources(token, get_update_token_callback(g, jira_credential))
        print(sites)


@app.command("list-available-projects")
def list_available_projects(
    workspace_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    if jira_credential and jira_integration:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        its_projects = jira_integration.list_available_its_projects(token)
        print_results(its_projects, format_=format_, fields=fields)
