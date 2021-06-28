import re
from typing import Iterable, Dict, List
from itertools import product
from unidecode import unidecode
from gitential2.datatypes.authors import AuthorAlias, AuthorInDB, AuthorCreate, AuthorUpdate
from gitential2.utils import levenshtein_ratio
from .context import GitentialContext


def list_active_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return [author for author in list_authors(g, workspace_id) if author.active]


def list_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return list(g.backend.authors.all(workspace_id))


def deduplicate_authors(g: GitentialContext, workspace_id: int):
    print(g, workspace_id)


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
            if alias.email:
                ret[alias.email] = author.id
        if author.email:
            ret[author.email] = author.id
    return ret


def _new_author_from_alias(alias: AuthorAlias) -> AuthorCreate:
    return AuthorCreate(active=True, name=alias.name, email=alias.email, aliases=[alias])


def aliases_matching(first: AuthorAlias, second: AuthorAlias) -> bool:
    for first_token, second_token in product(tokenize_alias(first), tokenize_alias(second)):
        if levenshtein_ratio(first_token, second_token) > 0.8:
            return True
    return False


def tokenize_alias(alias: AuthorAlias) -> List[str]:
    def _tokenize_str(s: str) -> str:
        _lower_ascii = unidecode(s.lower())
        _replaced_special = re.sub(r"\W+", " ", _lower_ascii)
        _splitted = _replaced_special.split()
        return " ".join(sorted(_splitted))

    def _remove_duplicates(l: List[str]) -> List[str]:
        ret = []
        for s in l:
            if s not in ret:
                ret.append(s)
        return ret

    def _remove_common_words(l: List[str]) -> List[str]:
        return [s for s in l if s not in _COMMON_WORDS]

    ret = []
    if alias.name:
        ret.append(_tokenize_str(alias.name))
    if alias.email:
        email_first_part = _tokenize_str(alias.email.split("@")[0])
        if len(email_first_part) >= 10:
            ret.append(email_first_part)
    if alias.login:
        ret.append(_tokenize_str(alias.login))
    return _remove_common_words(_remove_duplicates(ret))


_COMMON_WORDS = ["mail", "info", "noreply", "email", "user", "test"]
