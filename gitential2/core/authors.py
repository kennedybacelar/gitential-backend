from typing import Iterable, Dict, List

from gitential2.datatypes.authors import AuthorAlias, AuthorInDB, AuthorCreate, AuthorUpdate
from .context import GitentialContext


def list_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return list(g.backend.authors.all(workspace_id))


def deduplicate_authors(g: GitentialContext, workspace_id: int):
    pass


def update_author(g: GitentialContext, workspace_id: int, author_id: int, author_update: AuthorUpdate):
    # existing_author = g.backend.authors.get_or_error(workspace_id, author_id)
    return g.backend.authors.update(workspace_id, author_id, author_update)


def delete_author(g: GitentialContext, workspace_id: int, author_id: int) -> int:
    return g.backend.authors.delete(workspace_id, author_id)


def create_author(g: GitentialContext, workspace_id: int, author_create: AuthorCreate):
    return g.backend.authors.create(workspace_id, author_create)


def get_or_create_author_for_alias(g: GitentialContext, workspace_id: int, alias: AuthorAlias) -> AuthorInDB:
    all_authors = g.backend.authors.all(workspace_id)
    email_author_id_map = _build_email_author_id_map(all_authors)
    if alias.email in email_author_id_map:
        return g.backend.authors.get_or_error(workspace_id, email_author_id_map[alias.email])
    else:
        return g.backend.authors.create(workspace_id, _new_author_from_alias(alias))


def _build_email_author_id_map(author_list: Iterable[AuthorInDB]) -> Dict[str, int]:
    ret = {}
    for author in author_list:
        for alias in author.aliases or []:
            ret[alias.email] = author.id
        if author.email:
            ret[author.email] = author.id
    return ret


def _new_author_from_alias(alias: AuthorAlias) -> AuthorCreate:
    return AuthorCreate(active=True, name=alias.name, email=alias.email, aliases=[alias])
