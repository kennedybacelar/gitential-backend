from typing import List

from structlog import get_logger
from gitential2.core import GitentialContext


from .users import import_legacy_users
from .secrets import import_legacy_secrets
from .workspaces import import_legacy_workspaces
from .projects_and_repos import import_project_and_repos
from .teams_and_authors import import_teams_and_authors

logger = get_logger(__name__)


def _truncate_public(g: GitentialContext):
    g.backend.workspace_members.truncate()
    g.backend.workspaces.truncate()
    g.backend.credentials.truncate()
    g.backend.user_infos.truncate()
    g.backend.subscriptions.truncate()
    g.backend.users.truncate()


def import_legacy_database(
    g: GitentialContext,
    legacy_users: List[dict],
    legacy_secrets: List[dict],
    legacy_accounts: List[dict],
    legacy_users_accounts: List[dict],
):

    _truncate_public(g)
    import_legacy_users(g, legacy_users)
    import_legacy_secrets(g, legacy_secrets)
    import_legacy_workspaces(
        g,
        legacy_accounts,
        legacy_users_accounts,
    )


def import_legacy_workspace(
    g: GitentialContext,
    workspace_id: int,
    legacy_projects_repos: List[dict],
    legacy_aliases: List[dict],
    legacy_teams_authors: List[dict],
):
    _recreate_workspace_schema(g, workspace_id)
    import_project_and_repos(g, workspace_id, legacy_projects_repos)
    import_teams_and_authors(g, workspace_id, legacy_aliases, legacy_teams_authors)


def _recreate_workspace_schema(g: GitentialContext, workspace_id: int):  # pylint: disable=unused-argument
    g.backend.initialize_workspace(workspace_id)
