import typer

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
    to_be_removed = recalculate_deploy_commits(g=g, workspace_id=workspace_id)
    print_results([to_be_removed], format_=OutputFormat.json)
