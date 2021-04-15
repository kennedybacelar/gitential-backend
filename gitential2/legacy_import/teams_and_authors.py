from typing import List, Optional
from pydantic import ValidationError

from structlog import get_logger
from gitential2.core import GitentialContext

from gitential2.datatypes.authors import AuthorCreate, AuthorInDB
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.teams import TeamCreate, TeamInDB
from gitential2.datatypes.teammembers import TeamMemberCreate

logger = get_logger(__name__)


def import_teams_and_authors(
    g: GitentialContext,
    workspace_id: int,
    legacy_aliases: List[dict],
    legacy_teams_authors: List[dict],
):
    def search_old_id(items, obj_id):
        for item in items:
            if item["old_id"] == obj_id:
                return item["new_obj"]
        return False

    added_teamids: List = []
    added_authorids: List = []
    g.backend.initialize_workspace(workspace_id)
    x: int = 0
    for team_author in legacy_teams_authors:
        existing_team_obj = search_old_id(added_teamids, team_author["team"]["id"])
        print(x)
        x += 1
        if not existing_team_obj:
            existing_team_obj = _import_team(g, team_author["team"], workspace_id)
            added_teamids.append({"old_id": team_author["team"]["id"], "new_obj": existing_team_obj})
        existing_author_obj = search_old_id(added_authorids, team_author["author"]["id"])
        if not existing_author_obj:
            existing_author_obj = _import_author(g, team_author["author"], workspace_id, legacy_aliases)
            added_authorids.append({"old_id": team_author["author"]["id"], "new_obj": existing_author_obj})
        _create_team_author(
            g, team_id=existing_team_obj.id, author_id=existing_author_obj.id, workspace_id=workspace_id
        )


def _import_aliases(g: GitentialContext, email: str, workspace_id: int) -> list:  # pylint: disable=unused-argument
    return [
        AuthorAlias(
            name="asd",
            email="asdqwe",
        )
    ]


def _import_author(g: GitentialContext, author: dict, workspace_id: int, aliases) -> Optional[AuthorInDB]:
    try:
        processed_aliases = _import_aliases(g, aliases, workspace_id=workspace_id)  # pylint: disable=unused-variable
        author_create = AuthorCreate(
            active=author["active"],
            name=author["name"],
            email=author["email"],
            aliases=processed_aliases,
        )
        logger.info("Importing author", workspace_id=workspace_id)
        return g.backend.authors.create(workspace_id, author_create)
    except ValidationError as e:
        print(f"Failed to import author {author['email']}", e)
        return None


def _import_team(g: GitentialContext, team: dict, workspace_id: int) -> Optional[TeamInDB]:
    try:
        team_create = TeamCreate(
            name=team["name"],
            sprints_enabled=team["sprints_enabled"],
            created_at=team["created_at"],
        )
        logger.info("Importing team", workspace_id=workspace_id)
        return g.backend.teams.create(workspace_id, team_create)
    except ValidationError as e:
        print(f"Failed to import team {team['name']}", e)
        return None


def _create_team_author(g: GitentialContext, team_id: int, author_id: int, workspace_id: int) -> None:
    try:
        team_author_create = TeamMemberCreate(
            team_id=team_id,
            author_id=author_id,
        )
        logger.info("adding teammember", workspace_id=workspace_id)
        g.backend.team_members.create(workspace_id, team_author_create)
    except ValidationError as e:
        print(f"Failed to create teammember {e}")
