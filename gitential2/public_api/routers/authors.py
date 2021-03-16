from typing import List
from fastapi import APIRouter, Depends
from gitential2.core import (
    GitentialContext,
    list_authors,
    check_permission,
    update_author,
    deduplicate_authors,
    delete_author,
    create_author,
)
from gitential2.datatypes.authors import AuthorCreate, AuthorPublic, AuthorUpdate
from gitential2.datatypes.permissions import Entity, Action
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
    deduplicate_authors(g, workspace_id)
    return {"ok": True}


@router.get("/workspaces/{workspace_id}/developers-with-projects")
def developers_with_projects(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)

    authors = list_authors(g, workspace_id)
    ret = []
    for author in authors:
        author_dict = author.dict()
        author_dict["projects"] = []
        ret.append(author_dict)
    return ret
