from fastapi import APIRouter, Depends

from gitential2.datatypes.permissions import Entity, Action
from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission

from gitential2.core.its import list_available_its_projects, list_project_its_projects
from ..dependencies import current_user, gitential_context

router = APIRouter(tags=["its"])


@router.get("/workspaces/{workspace_id}/available-its-projects")
def available_its_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_available_its_projects(g, workspace_id)


@router.get("/workspaces/{workspace_id}/projects/{project_id}/its-projects")
def project_its_projects(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_project_its_projects(g, workspace_id, project_id=project_id)
