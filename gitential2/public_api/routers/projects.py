# pylint: skip-file
import datetime as dt
from gitential2.datatypes.projects import ProjectPublic
from uuid import UUID
from asyncio import sleep
from typing import List, Optional, Union

from fastapi import APIRouter, Query, WebSocket, Depends
from pydantic import BaseModel
from ..dependencies import workspace_ctrl
from gitential2.datatypes import ProjectCreateWithRepositories, ProjectUpdateWithRepositories

router = APIRouter(tags=["projects"])


@router.get("/workspaces/{workspace_id}/projects", response_model=List[ProjectPublic])
def workspace_projects(workspace_ctrl=Depends(workspace_ctrl)):
    return workspace_ctrl.list_projects()


@router.post("/workspaces/{workspace_id}/projects", response_model=ProjectPublic)
async def create_project(project: ProjectCreateWithRepositories, workspace_ctrl=Depends(workspace_ctrl)):
    return workspace_ctrl.create_project(project)


@router.put("/workspaces/{workspace_id}/projects/{project_id}", response_model=ProjectPublic)
async def update_project(
    project_update: ProjectUpdateWithRepositories, project_id: int, workspace_ctrl=Depends(workspace_ctrl)
):
    return workspace_ctrl.update_project(project_id=project_id, project_update=project_update)


@router.delete("/workspaces/{workspace_id}/projects/{project_id}")
async def delete_project(project_id: int, workspace_ctrl=Depends(workspace_ctrl)):
    return workspace_ctrl.delete_project(project_id=project_id)


@router.get("/workspaces/{workspace_id}/projects/{project_id}")
async def get_project(project_id: int, workspace_ctrl=Depends(workspace_ctrl)):
    return workspace_ctrl.get_project(project_id=project_id)


@router.websocket("/workspaces/{workspace_id}/projects/{project_id}/progress")
async def websocket_endpoint(websocket: WebSocket, workspace_id: Union[int, UUID], project_id: int):
    def _data_template():
        return {
            "done": False,
            "id": project_id,
            "name": "Szia Balazs",
            "status": "pending",
            "repos": [
                {
                    "clone": 0,
                    "done": False,
                    "error": None,
                    "extract": 0,
                    "id": 844,
                    "name": "react",
                    "persist": 0,
                    "phase": "pending",
                    "status": "pending",
                }
            ],
        }

    await websocket.accept()
    if project_id in [2496, 2507, 2512]:
        data = _data_template()
        data["done"] = True
        data["status"] = "finished"
        data["repos"][0]["done"] = True
        data["repos"][0]["phase"] = "done"
        data["repos"][0]["status"] = "finished"
        data["repos"][0]["pocessed_at"] = int(dt.datetime.now().timestamp())

        await websocket.send_json(data)
    else:

        for i in range(101):
            data = _data_template()
            data["repos"][0]["clone"] = i * 0.01
            data["repos"][0]["phase"] = "clone"
            await websocket.send_json(data)
            await sleep(0.01)

        for i in range(101):
            data = _data_template()
            data["repos"][0]["extract"] = i * 0.01
            data["repos"][0]["phase"] = "extract"
            await websocket.send_json(data)

            await sleep(0.01)

        for i in range(101):
            data = _data_template()
            data["repos"][0]["persist"] = i * 0.01
            data["repos"][0]["phase"] = "persist"
            await websocket.send_json(data)
            await sleep(0.01)

        data = _data_template()
        data["done"] = True
        data["status"] = "finished"
        data["repos"][0]["phase"] = "done"
        data["repos"][0]["done"] = True
        data["repos"][0]["status"] = "finished"
        data["repos"][0]["processed_at"] = int(dt.datetime.now().timestamp())
        await websocket.send_json(data)

    # while True:
    #     data = await websocket.receive_text()
    #     await websocket.send_text(f"Message text was: {data}")
