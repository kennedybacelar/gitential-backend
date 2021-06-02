import io
import asyncio
from typing import List

from fastapi import APIRouter, WebSocket, Depends
from fastapi.responses import StreamingResponse
from fastapi.encoders import jsonable_encoder
import pandas as pd
from structlog import get_logger
from gitential2.datatypes import ProjectCreateWithRepositories, ProjectUpdateWithRepositories
from gitential2.datatypes.projects import ProjectPublic, ProjectExportDatatype
from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.refresh_statuses import ProjectRefreshStatus

from gitential2.core.refresh_v2 import refresh_project

from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission
from gitential2.core.projects import (
    list_projects,
    create_project,
    update_project,
    delete_project,
    get_project,
)
from gitential2.core.refresh_statuses import get_project_refresh_status

from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["projects"])

logger = get_logger(__name__)


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


@router.get("/workspaces/{workspace_id}/projects/{project_id}/status", response_model=ProjectRefreshStatus)
def get_project_status_(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.read, workspace_id=workspace_id, project_id=project_id)
    return get_project_refresh_status(g, workspace_id, project_id=project_id)


def _get_project_status_hack(workspace_id, project_id) -> ProjectRefreshStatus:
    from gitential2.public_api.main import app  # pylint: disable=import-outside-toplevel,cyclic-import

    # app.state.gitential.check_permission(
    #     current_user, Entity.project, Action.read, workspace_id=workspace_id, project_id=project_id
    # )
    return get_project_refresh_status(g=app.state.gitential, workspace_id=workspace_id, project_id=project_id)


@router.post("/workspaces/{workspace_id}/projects/{project_id}/process", response_model=ProjectRefreshStatus)
def refresh_project_(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.update, workspace_id=workspace_id, project_id=project_id)
    refresh_project(g, workspace_id, project_id)
    return get_project_refresh_status(g, workspace_id, project_id=project_id)


@router.post("/workspaces/{workspace_id}/projects/{project_id}/rebuild", response_model=ProjectRefreshStatus)
def refresh_project_rebuild(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.project, Action.update, workspace_id=workspace_id, project_id=project_id)
    refresh_project(g, workspace_id, project_id)
    return get_project_refresh_status(g, workspace_id, project_id=project_id)


@router.websocket("/workspaces/{workspace_id}/projects/{project_id}/progress")
async def websocket_endpoint(
    websocket: WebSocket,
    project_id: int,
    workspace_id: int,
):
    loop = asyncio.get_running_loop()
    await websocket.accept()
    project_status = await loop.run_in_executor(None, _get_project_status_hack, workspace_id, project_id)
    await websocket.send_json(jsonable_encoder(project_status.dict()))
    while True:
        await asyncio.sleep(0.05)
        project_status = await loop.run_in_executor(None, _get_project_status_hack, workspace_id, project_id)
        await websocket.send_json(jsonable_encoder(project_status.dict()))
        if project_status.done:
            break


@router.get("/workspaces/{workspace_id}/projects/{project_id}/export/{datatype}")
def export_project_data(
    workspace_id: int,
    project_id: int,
    datatype: ProjectExportDatatype,
):
    logger.info("Export called", workspace_id=workspace_id, project_id=project_id, datatype=datatype)

    df = pd.DataFrame(
        {"num_legs": [2, 4, 8, 0], "num_wings": [2, 0, 0, 0], "num_specimen_seen": [10, 2, 1, 8]},
        index=["falcon", "dog", "spider", "fish"],
    )
    stream = io.StringIO()
    df.to_csv(stream, index=False)

    response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")

    filename = f"gitential-{workspace_id}-{project_id}-{datatype}.csv"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return response
