from typing import Optional

import typer
import uvicorn
from structlog import get_logger

from gitential2.core.deduplication import deduplicate_authors
from gitential2.core.authors import fix_author_names
from gitential2.core.emails import send_email_to_user
from gitential2.core.maintenance import maintenance
from gitential2.core.tasks import configure_celery
from gitential2.core.users import get_user
from gitential2.logging import initialize_logging
from gitential2.settings import load_settings
from gitential2.core.quick_login import generate_quick_login
from .common import OutputFormat, get_context, print_results
from .emails import app as emails_app
from .export import app as export_app
from .extract import app as extraction_app
from .projects import app as projects_app
from .query import app as query_app
from .refresh import app as refresh_app
from .repositories import app as repositories_app
from .status import app as status_app
from .tasks import app as tasks_app
from .usage_stats import app as usage_stats_app
from .users import app as users_app
from .crud import app as crud_app
from .invitation import app as invitation_app
from .jira import app as jira_app
from .vsts import app as vsts_app
from .its import app as its_app

logger = get_logger(__name__)

app = typer.Typer()
app.add_typer(export_app, name="export")
app.add_typer(users_app, name="users")
app.add_typer(projects_app, name="projects")
app.add_typer(repositories_app, name="repositories")
app.add_typer(usage_stats_app, name="usage-stats")
app.add_typer(refresh_app, name="refresh")
app.add_typer(tasks_app, name="tasks")
app.add_typer(status_app, name="status")
app.add_typer(query_app, name="query")
app.add_typer(emails_app, name="emails")
app.add_typer(extraction_app, name="extract")
app.add_typer(crud_app, name="crud")
app.add_typer(invitation_app, name="invitation")
app.add_typer(jira_app, name="jira")
app.add_typer(vsts_app, name="vsts")
app.add_typer(its_app, name="its")


@app.command("public-api")
def public_api(
    host: str = typer.Option("0.0.0.0", "--host", "-h"),
    port: int = typer.Option(7999, "--port", "-p"),
    reload: bool = False,
):
    uvicorn.run(
        "gitential2.public_api.main:app",
        host=host,
        port=port,
        log_level="info",
        reload=reload,
        forwarded_allow_ips="*",
        proxy_headers=True,
    )


@app.command("initialize-database")
def initialize_database():
    g = get_context()
    g.backend.initialize()
    g.backend.migrate()
    workspaces = g.backend.workspaces.all()
    for w in workspaces:
        logger.info("Initializing workspace schema", workspace_id=w.id)
        g.backend.initialize_workspace(w.id)
        g.backend.migrate_workspace(w.id)


@app.command("send-email-to-user")
def send_email_to_user_(
    user_id: int,
    template_name: str,
):
    g = get_context()
    user = get_user(g, user_id=user_id)
    if user:
        send_email_to_user(g, user, template_name)


@app.command("maintenance")
def maintenance_():
    g = get_context()
    configure_celery(g.settings)
    maintenance(g)


@app.command("deduplicate-authors")
def deduplicate_authors_(worspace_id: int, dry_run: bool = False):
    g = get_context()
    configure_celery(g.settings)

    results = deduplicate_authors(g, worspace_id, dry_run)

    for result in results:

        print_results(result, format_=OutputFormat.tabulate, fields=["id", "name", "email", "aliases", "active"])
        print()


@app.command("fix-author-names")
def fix_author_names_(
    worspace_id: int,
):
    g = get_context()
    configure_celery(g.settings)
    fix_author_names(g, worspace_id)


@app.command("quick-login")
def quick_login(user_id):
    g = get_context()
    login_hash = generate_quick_login(g, user_id)
    print("Login hash:", login_hash)


def main(prog_name: Optional[str] = None):
    initialize_logging(load_settings())
    app(prog_name=prog_name)
