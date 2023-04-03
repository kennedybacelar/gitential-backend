from typing import Optional
import typer
from structlog import get_logger

from gitential2.core.users import list_users, set_as_admin, reset_cache_for_user
from gitential2.core.subscription import set_as_professional, enable_or_disable_jira_integration
from .common import get_context, print_results, OutputFormat
from ..datatypes.cli_v2 import CacheRefreshType

app = typer.Typer()
logger = get_logger(__name__)


@app.command("list")
def list_users_(
    format_: OutputFormat = typer.Option(OutputFormat.json, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    users = list(list_users(g))
    print_results(users, format_=format_, fields=fields)


@app.command("set-as-admin")
def set_as_admin_(user_id: int, revoke: bool = False):
    g = get_context()
    set_as_admin(g, user_id, is_admin=not revoke)


@app.command(name="set-as-professional")
def set_as_professional_(user_id: int, number_of_developers: int = 5):
    g = get_context()
    subscription = set_as_professional(g, user_id, number_of_developers)
    print(subscription)


@app.command(name="set-jira-integration")
def toggle_jira_integration(user_id: int, enable: bool = typer.Option(None, "--enable/--disable")):
    g = get_context()
    subscription = enable_or_disable_jira_integration(g, user_id, enable)
    print(subscription)


@app.command(name="deactivate")
def deactivate_user(user_id: int):
    g = get_context()

    user = g.backend.users.get(id_=user_id)
    if user:
        confirm_res = typer.confirm(f"Are you really sure you want to deactivate the user with id=[{user.id}]?")
        if confirm_res:
            g.backend.deactivate_user(user_id=user_id)
    else:
        logger.exception("Given user_id is invalid!", user_id=user_id)


@app.command(name="purge")
def purge_user_from_database(user_id: int):
    g = get_context()

    user = g.backend.users.get(id_=user_id)
    if user:
        confirm_res = typer.confirm(
            f"Are you really sure you want to purge the user with id=[{user.id}] from the database?"
        )
        if confirm_res:
            purged_user = g.backend.purge_user_from_database(user_id=user_id)
            reset_cache_for_user(g=g, reset_type=CacheRefreshType.everything, user_id=user_id)
            print_results([purged_user])
    else:
        logger.exception("Given user_id is invalid!", user_id=user_id)
