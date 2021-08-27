from typing import List
from fastapi import APIRouter, Depends
from gitential2.core.context import GitentialContext
from gitential2.core.authors import (
    list_authors,
    update_author,
    delete_author,
    create_author,
    list_active_authors,
    get_author,
)
from gitential2.core.deduplication import deduplicate_authors
from gitential2.core.permissions import check_permission
from gitential2.datatypes.authors import AuthorCreate, AuthorPublic, AuthorUpdate
from gitential2.datatypes.permissions import Entity, Action
from gitential2.core.legacy import authors_in_projects


from ..dependencies import current_user, gitential_context


router = APIRouter(tags=["authors"])


@router.get("/workspaces/{workspace_id}/authors", response_model=List[AuthorPublic])
def list_authors_(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)

    return list_authors(g, workspace_id)


@router.post("/workspaces/{workspace_id}/authors", response_model=AuthorPublic)
def create_author_(
    author_create: AuthorCreate,
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)

    return create_author(g, workspace_id, author_create)


@router.put("/workspaces/{workspace_id}/authors/{author_id}", response_model=AuthorPublic)
def update_author_(
    author_update: AuthorUpdate,
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.update, workspace_id=workspace_id)

    return update_author(g, workspace_id, author_id, author_update)


@router.get("/workspaces/{workspace_id}/authors/{author_id}", response_model=AuthorPublic)
def get_author_(
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_author(g, workspace_id, author_id)


@router.delete("/workspaces/{workspace_id}/authors/{author_id}")
def delete_author_(
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.delete, workspace_id=workspace_id)
    return delete_author(g, workspace_id, author_id)


@router.post("/workspaces/{workspace_id}/authors/dedup")
def run_deduplicator(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.update, workspace_id=workspace_id)
    return deduplicate_authors(g, workspace_id)


@router.get("/workspaces/{workspace_id}/developers-with-projects")
def developers_with_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    authors_and_projects = authors_in_projects(g, workspace_id)
    authors = list_active_authors(g, workspace_id)
    ret = []
    for author in authors:
        author_dict = author.dict()
        author_dict["projects"] = authors_and_projects.get(author.id, {}).get("project_ids", [])
        ret.append(author_dict)
    return ret
