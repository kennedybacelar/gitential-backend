from gitential2.core.context import GitentialContext
from gitential2.core.refresh_v2 import refresh_workspace, refresh_project, refresh_repository
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.core.context import init_context_from_settings, GitentialContext
from gitential2.settings import load_settings
from gitential2.core.tasks import schedule_task, configure_celery
from gitential2.core.refresh_statuses import get_project_refresh_status
from gitential2.backends.base.repositories import BaseWorkspaceScopedRepository
import boto3
from structlog import get_logger
from gitential2.export.exporters import Exporter, CSVExporter, JSONExporter, SQLiteExporter, XlsxExporter
from datetime import datetime
from pathlib import Path
from typing import cast, List
from enum import Enum
from gitential2.datatypes.refresh_statuses import ProjectRefreshStatus
from gitential2.exceptions import InvalidStateException
from gitential2.utils import get_schema_name
from structlog import get_logger
from gitential2.datatypes import (AutoExportCreate, AutoExportInDB)
from typing import Optional, List
from gitential2.core.refresh_v2 import refresh_workspace
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.datatypes.authors import AuthorAlias, AuthorInDB
from functools import partial
import requests

logger = get_logger(__name__)
class ExportFormat(str, Enum):
    csv = "csv"
    json = "json"
    sqlite = "sqlite"
    xlsx = "xlsx"


def get_context() -> GitentialContext:
    return init_context_from_settings(load_settings())

# Get gitential settings context
g = get_context()
configure_celery(g.settings)


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
    g: GitentialContext=g,
    ) -> None:
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
        print(workspace)
        print(datetime.now().hour)
        if workspace.cron_schedule_time == datetime.now().hour:
            # Refresh full workspace
            logger.info(msg = f"Starting full workspace refresh for workspace {workspace.id}")
            refresh_workspace(g, workspace.workspace_id, RefreshStrategy.one_by_one, RefreshType.everything, True )

            # Refresh Tempo Data
            logger.info(msg = f"Starting tempo data refresh for workspace {workspace.id}")
            if workspace.tempo_access_token:
                lookup_tempo_worklogs(g, workspace.workspace_id, workspace.tempo_access_token, True)

            # Export full workspace
            logger.info(msg = f"Starting full workspace export for workspace {workspace.id}")
            export_full_workspace(g, workspace.id, date_from = datetime.now().min)
            

def export_full_workspace(
    g: GitentialContext,
    workspace_id: int,
    date_from: datetime = datetime.min,
    export_format: ExportFormat = ExportFormat.xlsx,

):
    """
    @desc: Main function to export workspace
    @args: workspace Id, date_from, export_format
    @output: Exports the workspace files to the /tmp directory
    @TODO: Upload data to s3 bucket - To be provided by Prosper
    """

    destination_directory: Path = Path("/tmp")
    data_to_export = [
        ("projects", g.backend.projects),
        ("repositories", g.backend.repositories),
        ("project_repositories", g.backend.project_repositories),
        ("authors", g.backend.authors),
        ("teams", g.backend.teams),
        ("team_members", g.backend.team_members),
        ("calculated_commits", g.backend.calculated_commits),
        ("calculated_patches", g.backend.calculated_patches),
        ("extracted_commits", g.backend.extracted_commits),
        ("extracted_patches", g.backend.extracted_patches),
        ("extracted_patch_rewrites", g.backend.extracted_patch_rewrites),
        ("pull_requests", g.backend.pull_requests),
        ("pull_request_commits", g.backend.pull_request_commits),
        ("pull_request_comments", g.backend.pull_request_comments),
        ("pull_request_labels", g.backend.pull_request_labels),
        # its related tables
        ("project_its_projects", g.backend.project_its_projects),
        ("its_projects", g.backend.its_projects),
        ("its_issues", g.backend.its_issues),
        ("its_issue_changes", g.backend.its_issue_changes),
        ("its_issue_times_in_statuses", g.backend.its_issue_times_in_statuses),
        ("its_issue_comments", g.backend.its_issue_comments),
        ("its_issue_linked_issues", g.backend.its_issue_linked_issues),
        ("its_sprints", g.backend.its_sprints),
        ("its_issue_sprints", g.backend.its_issue_sprints),
        ("its_issue_worklogs", g.backend.its_issue_worklogs),
    ]

    def _date_filter(name, obj, date_from):
        skip_date_filter = [
            "projects",
            "repositories",
            "project_repositories",
            "authors",
            "teams",
            "team_members",
            "its_issue_sprints",
            "its_issue_linked_issues",
        ]
        if name in skip_date_filter or date_from == datetime.min:
            return True
        elif hasattr(obj, "created_at") and getattr(obj, "created_at", datetime.min) >= date_from:
            return True
        elif hasattr(obj, "started_at") and (getattr(obj, "started_at", datetime.min) or datetime.min) >= date_from:
            return True
        elif hasattr(obj, "date") and getattr(obj, "date", datetime.min) >= date_from:
            return True
        else:
            return False

    # Export to the user defined format
    exporter = _get_exporter(export_format, destination_directory, workspace_id)

    for name, backend_repository in data_to_export:
        backend_repository = cast(BaseWorkspaceScopedRepository, backend_repository)
        logger.info("export_full_workspace", datatype=name, workspace_id=workspace_id, format=export_format)

        for obj in backend_repository.iterate_all(workspace_id=workspace_id):
            if _date_filter(name, obj, date_from):
                exporter.export_object(obj)

    exporter.close()

    # Upload data to s3 bucket - To be provided by Prosper
    # Pick exported file from "/tmp"
    # export_to_aws = Export(workspace_id)
    # export_to_aws.run



def _get_exporter(export_format: ExportFormat, destination_directory: Path, workspace_id: int) -> Exporter:
    schema_name = get_schema_name(workspace_id)
    prefix = f"{schema_name}_{datetime.now().date()}_"
    if export_format == ExportFormat.csv:
        return CSVExporter(destination_directory, prefix)
    elif export_format == ExportFormat.json:
        return JSONExporter(destination_directory, prefix)
    elif export_format == ExportFormat.sqlite:
        return SQLiteExporter(destination_directory, prefix)
    elif export_format == ExportFormat.xlsx:
        return XlsxExporter(destination_directory, prefix)
    raise ValueError("Invalid export format")



def lookup_tempo_worklogs(g: GitentialContext, workspace_id: int, tempo_access_token: str, force):
    worklogs_for_issue = {}
    _author_callback_partial = partial(_author_callback, g=g, workspace_id=workspace_id)

    for worklog in g.backend.its_issue_worklogs.iterate_desc(workspace_id):
        try:
            if not worklog.author_dev_id or force:
                author = None
                tempo_worklog = None

                jira_issue_id = worklog.extra.get("issueId") if worklog.extra else None
                if not jira_issue_id:
                    continue
                
                if jira_issue_id not in worklogs_for_issue:
                    worklogs_for_issue[jira_issue_id] = _get_tempo_worklogs_for_issue(tempo_access_token, jira_issue_id)

                for wl in worklogs_for_issue[jira_issue_id].get("results", []):
                    if str(wl["jiraWorklogId"]) == worklog.api_id:
                        tempo_worklog = wl
                        break

                if tempo_worklog:
                    author = _author_callback_partial(AuthorAlias(name=tempo_worklog["author"]["displayName"]))

                if author:
                    print(worklog.created_at, worklog.api_id, jira_issue_id, author.id, author.name)
                    worklog.author_dev_id = author.id
                    worklog.author_name = author.name
                    g.backend.its_issue_worklogs.update(workspace_id, worklog.id, worklog)
                else:
                    print(worklog.created_at, worklog.api_id, jira_issue_id, tempo_worklog)
                print("-------------------------------------------------------")
        except:
            pass

def _get_tempo_worklogs_for_issue(tempo_access_token: str, jira_issue_id) -> dict:
    response = requests.get(
        f"https://api.tempo.io/core/3/worklogs?issue={jira_issue_id}",
        headers={"Authorization": f"Bearer {tempo_access_token}"},
    )
    response.raise_for_status()
    return response.json()


def _author_callback(
    alias: AuthorAlias,
    g: GitentialContext,
    workspace_id: int,
) -> Optional[AuthorInDB]:
    author = get_or_create_optional_author_for_alias(g, workspace_id, alias)
    if author:
        return author
    else:
        return None
