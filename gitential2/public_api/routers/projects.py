import asyncio
from typing import List

from fastapi import APIRouter, WebSocket, Depends

from gitential2.datatypes import ProjectCreateWithRepositories, ProjectUpdateWithRepositories, ProjectStatus
from gitential2.datatypes.projects import ProjectPublic
from gitential2.datatypes.permissions import Entity, Action

from gitential2.core import (
    check_permission,
    GitentialContext,
    list_projects,
    create_project,
    update_project,
    delete_project,
    get_project,
    get_project_status,
)

from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["projects"])


@router.get("/workspaces/{workspace_id}/projects", response_model=List[ProjectPublic])
def workspace_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.read, workspace_id=workspace_id)
    return list_projects(g, workspace_id=workspace_id)


@router.post("/workspaces/{workspace_id}/projects", response_model=ProjectPublic)
def create_project_(
    project_create: ProjectCreateWithRepositories,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.create, workspace_id=workspace_id)
    return create_project(g, workspace_id, project_create)


@router.put("/workspaces/{workspace_id}/projects/{project_id}", response_model=ProjectPublic)
def update_project_(
    project_update: ProjectUpdateWithRepositories,
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.update, workspace_id=workspace_id, project_id=project_id)
    return update_project(g, workspace_id, project_id=project_id, project_update=project_update)


@router.delete("/workspaces/{workspace_id}/projects/{project_id}")
def delete_project_(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.delete, workspace_id=workspace_id, project_id=project_id)
    return delete_project(g, workspace_id, project_id=project_id)


@router.get("/workspaces/{workspace_id}/projects/{project_id}")
def get_project_(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.read, workspace_id=workspace_id, project_id=project_id)
    return get_project(g, workspace_id, project_id=project_id)


@router.get("/workspaces/{workspace_id}/projects/{project_id}/status", response_model=ProjectStatus)
def get_project_status_(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.read, workspace_id=workspace_id, project_id=project_id)
    return get_project_status(g, workspace_id, project_id=project_id)


def _get_project_status_hack(workspace_id, project_id) -> ProjectStatus:
    from gitential2.public_api.main import app

    # app.state.gitential.check_permission(
    #     current_user, Entity.project, Action.read, workspace_id=workspace_id, project_id=project_id
    # )
    return get_project_status(g=app.state.gitential, workspace_id=workspace_id, project_id=project_id)


@router.websocket("/workspaces/{workspace_id}/projects/{project_id}/progress")
async def websocket_endpoint(
    websocket: WebSocket,
    project_id: int,
    workspace_id: int,
):
    loop = asyncio.get_running_loop()
    await websocket.accept()
    project_status = await loop.run_in_executor(None, _get_project_status_hack, workspace_id, project_id)
    await websocket.send_json(project_status.dict())
    while True:
        await asyncio.sleep(0.05)
        project_status = await loop.run_in_executor(None, _get_project_status_hack, workspace_id, project_id)
        await websocket.send_json(project_status.dict())
        if project_status.done:
            break
