from typing import Optional
from datetime import datetime
from structlog import get_logger

from fastapi import APIRouter, Depends, Query

from gitential2.datatypes.permissions import Entity, Action
from gitential2.core.context import GitentialContext
from gitential2.core.commits import get_commits
from gitential2.core.permissions import check_permission

from ..dependencies import gitential_context, current_user

logger = get_logger(__name__)


router = APIRouter()


def _convert_to_datetime(s: Optional[str], eod: bool = False) -> Optional[datetime]:
    if s:
        dt = datetime.strptime(s, "%Y-%m-%d")
        if eod:
            return dt.replace(hour=23, minute=59, second=59)
        else:
            return dt
    return None


@router.get("/workspaces/{workspace_id}/repos/{repo_id}/commits")
def commits_repo_level(
    workspace_id: int,
    repo_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
    from_: Optional[str] = Query(None, alias="from"),
    to_: Optional[str] = Query(None, alias="to"),
    author_email: Optional[str] = Query(None, alias="email"),
    is_merge: Optional[bool] = Query(None),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_commits(
        g,
        workspace_id=workspace_id,
        repo_ids=[repo_id],
        from_=_convert_to_datetime(from_),
        to_=_convert_to_datetime(to_, eod=True),
        author_email=author_email,
        is_merge=is_merge,
    )


@router.get("/workspaces/{workspace_id}/commits")
def commits_workspace_level(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
    from_: Optional[str] = Query(None, alias="from"),
    to_: Optional[str] = Query(None, alias="to"),
    author_email: Optional[str] = Query(None, alias="email"),
    is_merge: Optional[bool] = Query(None),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_commits(
        g,
        workspace_id=workspace_id,
        from_=_convert_to_datetime(from_),
        to_=_convert_to_datetime(to_, eod=True),
        author_email=author_email,
        is_merge=is_merge,
    )


@router.get("/workspaces/{workspace_id}/projects/{project_id}/commits")
def commits_project_level(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
    from_: Optional[str] = Query(None, alias="from"),
    to_: Optional[str] = Query(None, alias="to"),
    author_email: Optional[str] = Query(None, alias="email"),
    is_merge: Optional[bool] = Query(None),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_commits(
        g,
        workspace_id=workspace_id,
        project_id=project_id,
        from_=_convert_to_datetime(from_),
        to_=_convert_to_datetime(to_, eod=True),
        author_email=author_email,
        is_merge=is_merge,
    )


@router.get("/workspaces/{workspace_id}/teams/{team_id}/commits")
def commits_team_level(
    workspace_id: int,
    team_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
    from_: Optional[str] = Query(None, alias="from"),
    to_: Optional[str] = Query(None, alias="to"),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_commits(
        g,
        workspace_id=workspace_id,
        team_id=team_id,
        from_=_convert_to_datetime(from_),
        to_=_convert_to_datetime(to_, eod=True),
    )
