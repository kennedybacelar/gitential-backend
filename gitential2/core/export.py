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


# TODO: Temporarily restrict the workspace id to only RAI -> This prevents from running refresh on all workspaces
# Change "1" to RAI workspace ID in prod

restricted_id = [1]

def daily_refresh_task(    
    g: GitentialContext,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    force: bool = False,
    schedule: bool = False,
    ) -> None:
    """
    @desc: workspace refresh function to be triggered once daily
    @args: strategy = parallel, refresh_type=everything, force = false, schedule = false
    @TODO: Check license status
    """

    # TODO: Check license status

    # Start a scheduled refresh for all workspaces in gitential
    # Limit to restricted workspace ID for not  -> RAI
    for workspace in g.backend.workspaces.all():
        try:
            if workspace.id in restricted_id:
                schedule_task(
                    g,
                    task_name="refresh_workspace",
                    params={
                        "workspace_id": workspace.id,
                        "strategy":strategy,
                        "refresh_type":refresh_type,
                        "force":force
                    },
                )
        except InvalidStateException:
            logger.warning("Skipping workspace, no owner?", workspace_id=workspace.id)

def export_workspace_task(
    g: GitentialContext = g,
    export_format: ExportFormat = ExportFormat.csv,    
    ) -> None:
    """
    @desc: workspace export function -> will check if the status of refresh_status.commits_refresh_scheduled, refresh_status.commits_in_progress, refresh_status.prs_refresh_scheduled, refresh_status.prs_in_progress are false, then run workspace export
    @args: workspace_id
    @TODO: Currently limiting exports to restricted_id list. This list has workspace.id = 1 in dev, and will be RAI workspace.id in prod. 
    """
    # Workspace level iteration
    for workspace in g.backend.workspaces.all():
        print(workspace)
        try:
            # Temp -> Limit to restricted workspace ID -> RAI
            if workspace.id in restricted_id:

                # Project level iteration
                for project in g.backend.projects.all(workspace.id):
                    print(project)

                    # Get the repo refresh status  
                    project_refresh_status:ProjectRefreshStatus = get_project_refresh_status(g, workspace.id, project.id)
                    
                    # Internal variable to determine if repo is ready for export
                    _run_export = False

                    for repo in project_refresh_status.repositories:
                        print(repo)
                        if not repo.commits_refresh_scheduled and not repo.commits_in_progress and not repo.prs_refresh_scheduled and not repo.prs_in_progress:
                            _run_export = True
                        else:
                            _run_export= True

                    if _run_export:    
                        logger.info("export_workspace_task",workspace_id=workspace.id, message = "Running export")
                        export_full_workspace(workspace.id, date_from = datetime.now().min, export_format = export_format)

                    else:
                        logger.info("export_workspace_task",workspace_id=workspace.id, message = "Not ready for export")

        except InvalidStateException:
            logger.warning("Skipping workspace, no owner?", workspace_id=workspace.id)


def export_full_workspace(
    workspace_id: int,
    date_from: datetime = datetime.min,
    export_format: ExportFormat = ExportFormat.csv,

):
    """
    @desc: Main function to export workspace
    @args: workspace Id, date_from, export_format
    @output: Exports the workspace files to the /tmp directory
    @TODO: Upload data to s3 bucket - To be provided by Prosper
    """

    g = get_context()
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
