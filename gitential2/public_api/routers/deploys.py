from typing import List
from fastapi import APIRouter, Depends
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
def record_deploy(
    deploy: Deploy,
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
):

    return register_deploy(g, workspace_id=workspace_id, deploy=deploy)
