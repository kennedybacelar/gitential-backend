from typing import List
from gitential2.datatypes.teams import TeamCreate, TeamInDB, TeamUpdate, TeamPublicWithAuthors


from .context import GitentialContext


def list_teams(g: GitentialContext, workspace_id: int) -> List[TeamInDB]:
    return list(g.backend.teams.all(workspace_id))


def add_authors_to_team(g: GitentialContext, workspace_id: int, team_id: int, author_ids: List[int]):
    added_members = g.backend.team_members.add_members_to_team(workspace_id, team_id, author_ids)
    return len(added_members)


def remove_authors_from_team(g: GitentialContext, workspace_id: int, team_id: int, author_ids: List[int]):
    return g.backend.team_members.remove_members_from_team(workspace_id, team_id, author_ids)


def create_team(g: GitentialContext, workspace_id: int, team_create: TeamCreate) -> TeamInDB:
    return g.backend.teams.create(workspace_id, team_create)


def delete_team(g: GitentialContext, workspace_id: int, team_id: int) -> int:
    return g.backend.teams.delete(workspace_id, team_id)


def update_team(g: GitentialContext, workspace_id: int, team_id: int, team_update: TeamUpdate) -> TeamInDB:
    return g.backend.teams.update(workspace_id, team_id, team_update)


def get_team_with_authors(g: GitentialContext, workspace_id: int, team_id: int) -> TeamPublicWithAuthors:
    team = g.backend.teams.get_or_error(workspace_id, team_id)
    team_dict = team.dict()
    # members = g.backend.team_members.get_members_for_team(workspace_id, team_id)
    team_dict["authors"] = []
    return TeamPublicWithAuthors(**team_dict)