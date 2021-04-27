import asyncio
from structlog import get_logger

from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import RedirectResponse
from gitential2.datatypes.permissions import Entity, Action
from gitential2.core import (
    GitentialContext,
    handle_authorize,
    check_permission,
)
from gitential2.core.legacy import (
    get_dev_related_projects,
    get_repos_projects,
    get_repo_top_devs,
    get_dev_top_repos,
    get_developers,
)
from gitential2.exceptions import AuthenticationException

from ..dependencies import gitential_context, OAuth, current_user
from .auth import _get_token, _get_user_info

logger = get_logger(__name__)


router = APIRouter()


# pylint: disable=too-many-arguments
@router.get("/login", name="legacy_login")
async def legacy_login(
    request: Request,
    source: str = "vsts",
    id_token: str = None,
    code: str = None,
    oauth_verifier: str = None,
    g: GitentialContext = Depends(gitential_context),
    oauth: OAuth = Depends(),
    current_user=Depends(current_user),
):
    remote = oauth.create_client(source)
    integration = g.integrations.get(source)

    if remote is None or integration is None:
        raise HTTPException(404)
    try:

        token = await _get_token(request, remote, code, id_token, oauth_verifier)
        user_info = await _get_user_info(request, remote, token)

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, handle_authorize, g, integration.name, token, user_info, current_user)

        request.session["current_user_id"] = result["user"].id

        redirect_uri = request.session.get("redirect_uri")
        if redirect_uri:
            return RedirectResponse(url=redirect_uri)
        else:
            return result
    except Exception as e:  # pylint: disable=broad-except
        logger.exception("Error during authtentication")
        raise AuthenticationException("Error during authentication") from e


@router.get("/v2/workspaces/{workspace_id}/repos/projects")
def workspace_repos_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_repos_projects(g, workspace_id)


@router.get("/v2/workspaces/{workspace_id}/repo_top_devs")
def repo_top_devs(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_repo_top_devs(g, workspace_id)


@router.get("/v2/workspaces/{workspace_id}/dev_top_repos")
def dev_top_repos(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_dev_top_repos(g, workspace_id)


@router.get("/v2/workspaces/{workspace_id}/developers")
def developers(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_developers(g, workspace_id)


@router.get("/v2/workspaces/{workspace_id}/repos/{repo_id}/developers")
def developers_repo_level(
    workspace_id: int,
    repo_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_developers(g, workspace_id=workspace_id, repo_id=repo_id)


@router.get("/v2/workspaces/{workspace_id}/projects/{project_id}/developers")
async def developers_project_level(
    orient: str,
    limit: int,
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):  # pylint: disable=unused-argument
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_developers(g, workspace_id=workspace_id, project_id=project_id)


@router.get("/v2/workspaces/{workspace_id}/dev_related_projects")
def dev_related_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_dev_related_projects(g, workspace_id)
