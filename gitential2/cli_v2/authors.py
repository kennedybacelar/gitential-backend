import typer
from gitential2.core.projects import _recalculate_active_authors
from gitential2.core.authors import get_author_update

from .common import get_context


app = typer.Typer()


@app.command("recalculate-active-authors")
def recalculate_active_authors_(workspace_id: int):
    g = get_context()
    _recalculate_active_authors(g, workspace_id)


@app.command("force-filling-authors-name")
def force_filling_of_author_names(
    workspace_id: int,
):
    """This functionality is a hot fix for the workspaces which authors tables contains entries with null names"""

    g = get_context()
    authors_with_null_name_or_email = g.backend.authors.get_authors_with_null_name_or_email(workspace_id)

    for author in authors_with_null_name_or_email:
        login = None
        for alias in author.aliases:
            if not author.name:
                author.name = alias.name
                if not login:
                    login = alias.login
            if not author.email:
                author.email = alias.email
            if author.name and author.email:
                g.backend.authors.update(workspace_id=workspace_id, id_=author.id, obj=get_author_update(author))
                break
        author.name = author.name or login or "unknown"
        g.backend.authors.update(workspace_id=workspace_id, id_=author.id, obj=get_author_update(author))
