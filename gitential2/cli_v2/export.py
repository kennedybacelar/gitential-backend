from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import cast
import typer
from structlog import get_logger
from gitential2.export.exporters import Exporter, CSVExporter, JSONExporter, SQLiteExporter, XlsxExporter
from gitential2.backends.base.repositories import BaseWorkspaceScopedRepository

from .common import get_context, validate_directory_exists

app = typer.Typer()

logger = get_logger(__name__)


class ExportFormat(str, Enum):
    csv = "csv"
    json = "json"
    sqlite = "sqlite"
    xlsx = "xlsx"


@app.command("full-workspace")
def export_full_workspace(
    workspace_id: int,
    destination_directory: Path = Path("/tmp"),
    export_format: ExportFormat = ExportFormat.csv,
    date_from: datetime = datetime.min,
):
    validate_directory_exists(destination_directory)

    g = get_context()
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
    ]

    def _date_filter(name, obj, date_from):
        skip_date_filter = ["projects", "repositories", "project_repositories", "authors", "teams", "team_members"]
        if name in skip_date_filter or date_from == datetime.min:
            return True
        elif hasattr(obj, "created_at") and getattr(obj, "created_at", datetime.min) >= date_from:
            return True
        elif hasattr(obj, "date") and getattr(obj, "date", datetime.min) >= date_from:
            return True
        else:
            return False

    exporter = _get_exporter(export_format, destination_directory, workspace_id)

    for name, backend_repository in data_to_export:
        backend_repository = cast(BaseWorkspaceScopedRepository, backend_repository)
        logger.info("exporting", datatype=name, workspace_id=workspace_id, format=export_format)

        for obj in backend_repository.iterate_all(workspace_id=workspace_id):
            if _date_filter(name, obj, date_from):
                exporter.export_object(obj)

    exporter.close()


@app.command("repositories")
def export_repositories(
    workspace_id: int, destination_directory: Path = Path("/tmp"), export_format: ExportFormat = ExportFormat.csv
):
    validate_directory_exists(destination_directory)
    g = get_context()
    exporter = _get_exporter(export_format, destination_directory, workspace_id)
    for repository in g.backend.repositories.all(workspace_id=workspace_id):
        exporter.export_object(repository)
    exporter.close()


def _get_exporter(export_format: ExportFormat, destination_directory: Path, workspace_id: int) -> Exporter:
    prefix = f"ws_{workspace_id}_"
    if export_format == ExportFormat.csv:
        return CSVExporter(destination_directory, prefix)
    elif export_format == ExportFormat.json:
        return JSONExporter(destination_directory, prefix)
    elif export_format == ExportFormat.sqlite:
        return SQLiteExporter(destination_directory, prefix)
    elif export_format == ExportFormat.xlsx:
        return XlsxExporter(destination_directory, prefix)
    raise ValueError("Invalid export format")
