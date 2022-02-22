from datetime import datetime
from typing import Optional, cast
from functools import partial
import sys
import logging

from structlog import get_logger
import typer


from gitential2.datatypes.credentials import CredentialInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import get_update_token_callback, get_fresh_credential
from gitential2.core.authors import developer_map_callback
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


# @app.command("list-boards")
# def list_boards(
#     workspace_id: int,
#     itsp_id: int,
#     # format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
#     # fields: Optional[str] = None,
# ):
#     g = get_context()
#     jira_credential = _get_jira_credential(g, workspace_id)
#     jira_integration = g.integrations.get("jira")
#     its_project = g.backend.its_projects.get(workspace_id, itsp_id)
#     print(its_project)
#     if jira_credential and jira_integration and its_project:
#         jira_integration = cast(JiraIntegration, jira_integration)
#         token = jira_credential.to_token_dict(g.fernet)
#         jira_integration.list_boards(token, its_project.api_url)


@app.command("list-all-issues")
def list_all_issues(
    workspace_id: int,
    itsp_id: int,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        issue_headers = jira_integration.list_all_issues_for_project(token, its_project)
        print_results(issue_headers, format_=format_, fields=fields)


@app.command("list-recently-updated-issues")
def list_recently_updated_issues(
    workspace_id: int,
    itsp_id: int,
    date_from: datetime,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        issue_headers = jira_integration.list_recently_updated_issues(token, its_project, date_from=date_from)
        print_results(issue_headers, format_=format_, fields=fields)


@app.command("get-issue")
def get_issue(
    workspace_id: int,
    itsp_id: int,
    issue_id_or_key: str,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    jira_credential = _get_jira_credential(g, workspace_id)
    jira_integration = g.integrations.get("jira")
    its_project = g.backend.its_projects.get(workspace_id, itsp_id)
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)

    if jira_credential and jira_integration and its_project:
        jira_integration = cast(JiraIntegration, jira_integration)
        token = jira_credential.to_token_dict(g.fernet)
        issue = jira_integration.get_all_data_for_issue(token, its_project, issue_id_or_key, dev_map_callback)
        print_results([issue], format_=format_, fields=fields)
