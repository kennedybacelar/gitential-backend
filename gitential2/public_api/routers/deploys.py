from typing import List
from fastapi import APIRouter, Depends, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from gitential2.datatypes.deploys import Deploy
from gitential2.core.context import GitentialContext
from gitential2.datatypes.permissions import Entity, Action

from gitential2.core.deploys import get_all_deploys, register_deploy, delete_deploy_by_id, recalculate_deploy_commits
from gitential2.core.permissions import check_permission

from ..dependencies import gitential_context, current_user

security = HTTPBearer()
router = APIRouter(tags=["deploys"])


@router.get("/workspaces/{workspace_id}/deploys", response_model=List[Deploy])
def get_deploys(
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
    auth: HTTPAuthorizationCredentials = Security(security),
):
    return get_all_deploys(g, workspace_id, token=auth.credentials)


@router.post("/workspaces/{workspace_id}/deploys", response_model=bool)
def record_deploy(
    deploy: Deploy,
    workspace_id: int,
    g: GitentialContext = Depends(gitential_context),
    auth: HTTPAuthorizationCredentials = Security(security),
):
    return register_deploy(g, workspace_id=workspace_id, deploy=deploy, token=auth.credentials)


@router.delete("/workspaces/{workspace_id}/{deploy_id}/deploy", response_model=bool)
def delete_deploy(
    workspace_id: int,
    deploy_id: str,
    g: GitentialContext = Depends(gitential_context),
    auth: HTTPAuthorizationCredentials = Security(security),
):
    return delete_deploy_by_id(g=g, workspace_id=workspace_id, deploy_id=deploy_id, token=auth.credentials)


@router.post("/workspaces/{workspace_id}/deploys/recalculate-deploy-commits")
def recalculate_deploy_commits_manual_trigger(
    workspace_id: int, g: GitentialContext = Depends(gitential_context), current_user=Depends(current_user)
):
    check_permission(g, current_user, Entity.workspace, Action.delete, workspace_id=workspace_id)
    recalculate_deploy_commits(g=g, workspace_id=workspace_id)
    return True
