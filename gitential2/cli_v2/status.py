import json
import typer
from structlog import get_logger
from fastapi.encoders import jsonable_encoder
from tabulate import tabulate

from gitential2.core.refresh_v2 import get_repo_refresh_status

from .common import get_context

app = typer.Typer()
logger = get_logger(__name__)


@app.command("repository")
def repository_status_(workspace_id: int, repository_id: int):
    g = get_context()
    status = get_repo_refresh_status(g, workspace_id, repository_id)
    print(json.dumps(jsonable_encoder(status)))


@app.command("project")
def project_status_(workspace_id: int, project_id: int):
    g = get_context()
    statuses = [
        jsonable_encoder(get_repo_refresh_status(g, workspace_id, repo_id))
        for repo_id in g.backend.project_repositories.get_repo_ids_for_project(workspace_id, project_id)
    ]
    header = {h: h.replace("_", "\n") for h in list(statuses[0].keys())}
    print(tabulate(statuses, headers=header, tablefmt="psql"))
