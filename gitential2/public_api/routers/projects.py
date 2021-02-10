# pylint: skip-file
import datetime as dt
from uuid import UUID
from asyncio import sleep
from typing import List, Optional, Union

from fastapi import APIRouter, Query, WebSocket, Depends
from pydantic import BaseModel
from ..dependencies import workspace_ctrl

router = APIRouter(tags=["projects"])


@router.get("/workspaces/{workspace_id}/projects")
def workspace_projects(workspace_ctrl=Depends(workspace_ctrl)):
    return workspace_ctrl.list_projects()

    # return [
    #     {
    #         "id": 2496,
    #         "name": "Project1",
    #         "account": 2155,
    #         "shareable": True,
    #         "pattern": "",
    #         "created_at": 1593682789,
    #         "updated_at": 1593682789,
    #     },
    #     {
    #         "id": 2507,
    #         "name": "Project2",
    #         "account": 2155,
    #         "shareable": True,
    #         "pattern": "",
    #         "created_at": 1593777102,
    #         "updated_at": 1593777102,
    #     },
    #     {
    #         "id": 2512,
    #         "name": "Project3",
    #         "account": 2155,
    #         "shareable": True,
    #         "pattern": "",
    #         "created_at": 1593979149,
    #         "updated_at": 1593979149,
    #     },
    # ]


class Repo(BaseModel):
    clone_url: str
    name: str
    source: str
    secret_source: str
    private: bool
    github_id: Optional[int] = None
    gitlab_id: Optional[int] = None
    vsts_id: Optional[int] = None
    bitbucket_id: Optional[int] = None


class CreateProjectRequest(BaseModel):
    name: str
    repos: List[Repo]
    pattern: str


@router.post("/workspaces/{workspace_id}/projects")
async def create_project(project: CreateProjectRequest):
    print("!!!", project)
    data = project.dict()
    data["id"] = 1234
    return data


@router.put("/workspaces/{workspace_id}/projects/{project_id}")
async def update_project(workspace_id: Union[int, UUID], project_id: int, project: CreateProjectRequest):
    data = project.dict()
    data["id"] = project_id
    return data


@router.delete("/workspaces/{workspace_id}/projects/{project_id}")
async def delete_project(workspace_id: Union[int, UUID], project_id: int):
    return {}


@router.get("/workspaces/{workspace_id}/projects/{project_id}")
async def get_project(workspace_id: Union[int, UUID], project_id: int):
    return {
        "id": project_id,
        "name": "Project1 Example",
        "account": workspace_id,
        "shareable": True,
        "pattern": "",
        "created_at": 1593682789,
        "updated_at": 1593682789,
    }


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
