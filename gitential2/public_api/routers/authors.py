from typing import List, Optional

from fastapi import APIRouter, Depends, Query

from gitential2.core.authors import (
    list_authors,
    update_author,
    delete_author,
    create_author,
    list_active_authors,
    get_author,
    get_author_names_and_emails,
    authors_count,
    move_emails_and_logins_to_author,
)
from gitential2.core.context import GitentialContext
from gitential2.core.deduplication import deduplicate_authors
from gitential2.core.legacy import authors_in_projects
from gitential2.core.permissions import check_permission
from gitential2.datatypes.authors import (
    AuthorCreate,
    AuthorPublic,
    AuthorUpdate,
    AuthorFilters,
    AuthorsPublicExtendedSearchResult,
    AuthorPublicExtended,
)
from gitential2.datatypes.permissions import Entity, Action
from ..dependencies import current_user, gitential_context
from ...core.authors_list import list_authors_extended, get_author_extended

router = APIRouter(tags=["authors"])


@router.get("/workspaces/{workspace_id}/authors-count")
def authors_count_(
    workspace_id: int,
    is_only_git_active_authors: Optional[bool] = Query(None, alias="is_only_git_active_authors"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return authors_count(g=g, workspace_id=workspace_id, is_only_git_active_authors=is_only_git_active_authors)


@router.get("/workspaces/{workspace_id}/authors-names-emails")
def list_authors_names_emails_(
    workspace_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_author_names_and_emails(g=g, workspace_id=workspace_id)


@router.get("/workspaces/{workspace_id}/authors", response_model=List[AuthorPublic])
def list_authors_(
    workspace_id: int,
    emails_and_logins: Optional[List[str]] = Query(None, alias="emails_and_logins"),
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return list_authors(g=g, workspace_id=workspace_id, emails_and_logins=emails_and_logins)


@router.post("/workspaces/{workspace_id}/authors-extended", response_model=AuthorsPublicExtendedSearchResult)
def list_authors_extended_(
    workspace_id: int,
    author_filters: Optional[AuthorFilters] = None,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return list_authors_extended(g=g, workspace_id=workspace_id, author_filters=author_filters)


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


@router.get("/workspaces/{workspace_id}/authors/{author_id}/extended", response_model=AuthorPublicExtended)
def get_author_extended_(
    workspace_id: int,
    author_id: int,
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.read, workspace_id=workspace_id)
    return get_author_extended(g, workspace_id, author_id)


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


@router.post("/workspaces/{workspace_id}/authors/{destination_author_id}/move-aliases")
def move_emails_and_logins_to_author_(
    workspace_id: int,
    destination_author_id: int,
    emails_and_logins: List[str],
    current_user=Depends(current_user),
    g: GitentialContext = Depends(gitential_context),
):
    check_permission(g, current_user, Entity.author, Action.update, workspace_id=workspace_id)
    return move_emails_and_logins_to_author(g, workspace_id, emails_and_logins, destination_author_id)
