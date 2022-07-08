from pathlib import Path
import typer

from scripts.gather_internal_deployment_history import (
    gathering_commits_in_master_branch,
    exporting_deploys_into_json_file,
)
from gitential2.core.deploys import get_all_deploys, recalculate_deploy_commits
from .common import get_context, print_results, OutputFormat

app = typer.Typer()


@app.command("get-all-deploys")
def get_deploys(workspace_id: int):
    g = get_context()
    deploys = get_all_deploys(g=g, workspace_id=workspace_id)
    print_results([deploys], format_=OutputFormat.json)


@app.command("recalculate-deploy-commits")
def recalculate_deploys(workspace_id: int):
    g = get_context()
    recalculate_deploy_commits(g=g, workspace_id=workspace_id)


@app.command("gather-internal-deployment")
def gather_internal_deployment_history(path: str):
    internal_deployment_history = gathering_commits_in_master_branch(path)
    print_results(internal_deployment_history, format_=OutputFormat.json)


@app.command("exporting-internal-deployment-into-json-file")
def export_data_into_json_file(repo_source_path: Path, destination_path: Path):
    exporting_deploys_into_json_file(repo_source_path, destination_path)
