from typing import List
from fastapi import APIRouter, Depends
from gitential2.datatypes.workspaces import WorkspacePublic, WorkspaceCreate, WorkspaceUpdate
from ..dependencies import current_user, GitentialCore, workspace_ctrl

router = APIRouter(tags=["workspaces"])


@router.get("/workspaces", response_model=List[WorkspacePublic], response_model_exclude_unset=True)
def workspaces(
    include_members: bool = False,
    include_projects: bool = False,
    current_user=Depends(current_user),
    gitential: GitentialCore = Depends(),
):
    return gitential.get_accessible_workspaces(
        current_user=current_user, include_members=include_members, include_projects=include_projects
    )


@router.get("/workspaces/{workspace_id}", response_model=WorkspacePublic)
def get_workspace(
    workspace_id: int,
    include_members: bool = False,
    include_projects: bool = False,
    current_user=Depends(current_user),
    gitential: GitentialCore = Depends(),
):
    return gitential.get_workspace(
        workspace_id=workspace_id,
        current_user=current_user,
        include_members=include_members,
        include_projects=include_projects,
    )


@router.put("/workspaces/{workspace_id}", response_model=WorkspacePublic)
def update_workspace(
    workspace_id: int,
    workspace_update: WorkspaceUpdate,
    current_user=Depends(current_user),
    gitential: GitentialCore = Depends(),
):
    return gitential.update_workspace(
        workspace_id=workspace_id,
        workspace=workspace_update,
        current_user=current_user,
    )


@router.delete("/workspaces/{workspace_id}")
def delete_workspace(
    workspace_id: int,
    current_user=Depends(current_user),
    gitential: GitentialCore = Depends(),
):
    return gitential.delete_workspace(workspace_id, current_user)


@router.post("/workspaces", response_model=WorkspacePublic)
def create_workspace(
    workspace_create: WorkspaceCreate, current_user=Depends(current_user), gitential: GitentialCore = Depends()
):
    return gitential.create_workspace(workspace_create, current_user=current_user, primary=False)


@router.get("/workspaces/{workspace_id}/repository-sources")
def list_connected_repository_sources(workspace_ctrl=Depends(workspace_ctrl)):
    return workspace_ctrl.list_connected_repository_sources()
