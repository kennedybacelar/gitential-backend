# pylint: skip-file
from typing import List, Optional

from fastapi import APIRouter, Depends

from gitential2.core.context import GitentialContext
from gitential2.core.permissions import check_permission
from gitential2.core.repositories import (
    list_available_repositories,
    search_public_repositories,
    get_repository,
    list_repositories,
    create_repositories,
    delete_repositories,
    list_project_repositories,
    list_available_repo_groups,
)
from gitential2.datatypes.permissions import Entity, Action
from gitential2.datatypes.repositories import RepositoryCreate
from ..dependencies import current_user, gitential_context

router = APIRouter(tags=["repositories"])


@router.post("/workspaces/{workspace_id}/available-repo-groups")
def available_repo_groups(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_available_repo_groups(g=g, workspace_id=workspace_id, user_id=current_user.id)


@router.post("/workspaces/{workspace_id}/available-repos")
def available_repos(
    workspace_id: int,
    user_organization_name_list: Optional[List[str]] = None,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_available_repositories(g, workspace_id, current_user.id, user_organization_name_list)


@router.get("/workspaces/{workspace_id}/search-public-repos")
def search_public_repos(
    workspace_id: int,
    search: str,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return search_public_repositories(g, workspace_id, search=search)


@router.get("/workspaces/{workspace_id}/repos")
def workspace_repos(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_repositories(g, workspace_id)


@router.get("/workspaces/{workspace_id}/repos/{repository_id}")
def get_repo(
    workspace_id: int,
    repository_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):

    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return get_repository(g, workspace_id, repository_id)


@router.post("/workspaces/{workspace_id}/repos")
def add_repos(
    workspace_id: int,
    repository_creates: List[RepositoryCreate],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.repository, Action.create, workspace_id=workspace_id)
    return create_repositories(g, workspace_id, repository_creates)


@router.delete("/workspaces/{workspace_id}/repos")
def delete_repos(
    workspace_id: int,
    repository_ids: List[int],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.repository, Action.delete, workspace_id=workspace_id)
    return delete_repositories(g, workspace_id, repository_ids)


@router.get("/workspaces/{workspace_id}/projects/{project_id}/repos")
def project_repos(
    workspace_id: int,
    project_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.workspace, Action.read, workspace_id=workspace_id)
    return list_project_repositories(g, workspace_id, project_id=project_id)
