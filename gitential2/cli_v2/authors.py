import typer
from gitential2.core.projects import _recalculate_active_authors
from gitential2.datatypes.authors import AuthorUpdate

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
    """This functionality is a hot fix for the workspaces whose authors tables contains entries with null names"""

    g = get_context()
    authors_with_null_names = g.backend.authors.get_authors_with_null_names(workspace_id)

    for author in authors_with_null_names:
        for alias in author.aliases:
            if alias.get("name"):
                g.backend.authors.update(
                    workspace_id=workspace_id,
                    id_=author.id,
                    obj=AuthorUpdate(
                        active=author.active,
                        name=alias.get("name"),
                        email=author.email,
                        aliases=author.aliases,
                        extra=author.extra,
                    ),
                )
                break
            if alias.get("login"):
                g.backend.authors.update(
                    workspace_id=workspace_id,
                    id_=author.id,
                    obj=AuthorUpdate(
                        active=author.active,
                        name=alias.get("login"),
                        email=author.email,
                        aliases=author.aliases,
                        extra=author.extra,
                    ),
                )
                break
            if alias.get("email"):
                g.backend.authors.update(
                    workspace_id=workspace_id,
                    id_=author.id,
                    obj=AuthorUpdate(
                        active=author.active,
                        name=alias.get("email"),
                        email=author.email,
                        aliases=author.aliases,
                        extra=author.extra,
                    ),
                )
                break
