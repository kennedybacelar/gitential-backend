from typing import Optional
import typer

from structlog import get_logger

from gitential2.core.repositories import list_project_repositories, list_repositories, list_available_repositories
from .common import get_context, print_results, OutputFormat

app = typer.Typer()
logger = get_logger(__name__)


@app.command("list")
def list_repositories_(
    workspace_id: int,
    project_id: Optional[int] = None,
    available: bool = False,
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    results: list = []
    if available:
        results = list_available_repositories(g, workspace_id)
    elif project_id:
        results = list_project_repositories(g, workspace_id, project_id)
    else:
        results = list_repositories(g, workspace_id)

    print_results(results, format_=format_, fields=fields)
