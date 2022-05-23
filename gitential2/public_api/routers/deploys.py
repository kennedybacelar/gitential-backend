from typing import List
from fastapi import APIRouter, Depends, Request
from structlog import get_logger

from gitential2.datatypes.deploys import Deploy
from gitential2.core.context import GitentialContext

from gitential2.core.deploys import get_all_deploys, register_deploy

from ..dependencies import gitential_context

logger = get_logger(__name__)

router = APIRouter(tags=["deploys"])


@router.get("/workspaces/{workspace_id}/deploys", response_model=List[Deploy])
def get_deploys(
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
):
    return get_all_deploys(g, workspace_id)


@router.post("/workspaces/{workspace_id}/deploys", response_model=Deploy)
async def record_deploy(
    request: Request,
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
):

    deploy_json = await request.json()
    return register_deploy(g, workspace_id=workspace_id, deploy_json=deploy_json)
