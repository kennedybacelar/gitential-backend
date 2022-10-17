import typer
from gitential2.core.projects import _recalculate_active_authors
from gitential2.core.authors import force_filling_of_author_names

from .common import get_context


app = typer.Typer()


@app.command("recalculate-active-authors")
def recalculate_active_authors_(workspace_id: int):
    g = get_context()
    _recalculate_active_authors(g, workspace_id)


@app.command("force-filling-authors-name")
def _force_filling_of_author_names(
    workspace_id: int,
):
    """This functionality is a hot fix for the workspaces which authors tables contains entries with null names or emails"""
    g = get_context()
    force_filling_of_author_names(g, workspace_id)
