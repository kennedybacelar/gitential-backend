import typer

from gitential2.core.projects import _recalculate_active_authors
from .common import get_context

app = typer.Typer()


@app.command("recalculate-active-authors")
def recalculate_active_authors_(workspace_id: int):
    g = get_context()
    _recalculate_active_authors(g, workspace_id)
