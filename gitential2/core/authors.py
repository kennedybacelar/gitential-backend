import re
from typing import Iterable, Dict, List, Optional
from itertools import product
from unidecode import unidecode
from structlog import get_logger
from gitential2.datatypes.authors import AuthorAlias, AuthorInDB, AuthorCreate, AuthorUpdate
from gitential2.utils import levenshtein_ratio
from .context import GitentialContext

logger = get_logger(__name__)


def list_active_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return [author for author in list_authors(g, workspace_id) if author.active]


def list_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return list(g.backend.authors.all(workspace_id))


def update_author(g: GitentialContext, workspace_id: int, author_id: int, author_update: AuthorUpdate):
    # existing_author = g.backend.authors.get_or_error(workspace_id, author_id)
    return g.backend.authors.update(workspace_id, author_id, author_update)


def merge_authors(g: GitentialContext, workspace_id: int, authors: List[AuthorInDB]) -> AuthorInDB:
    first, *rest = authors
    author_update = AuthorUpdate(**first.dict())
    for other in rest:
        author_update.aliases += other.aliases  # pylint: disable=no-member
        delete_author(g, workspace_id, other.id)
    author_update.aliases = _remove_duplicate_aliases(author_update.aliases)
    return g.backend.authors.update(workspace_id, first.id, author_update)


def delete_author(g: GitentialContext, workspace_id: int, author_id: int) -> int:
    team_ids = g.backend.team_members.get_author_team_ids(workspace_id, author_id)
    for team_id in team_ids:
        g.backend.team_members.remove_members_from_team(workspace_id, team_id, [author_id])
    return g.backend.authors.delete(workspace_id, author_id)


def create_author(g: GitentialContext, workspace_id: int, author_create: AuthorCreate):
    return g.backend.authors.create(workspace_id, author_create)


def get_or_create_author_for_alias(g: GitentialContext, workspace_id: int, alias: AuthorAlias) -> AuthorInDB:
    all_authors = list(g.backend.authors.all(workspace_id))

    email_author_id_map = _build_email_author_id_map(all_authors)
    if alias.email and alias.email in email_author_id_map:
        author = g.backend.authors.get_or_error(workspace_id, email_author_id_map[alias.email])
        logger.debug(
            "Matching author for alias by email address", alias=alias, author=author, workspace_id=workspace_id
        )
        return author
    else:
        for author in all_authors:
            if alias_matching_author(alias, author):
                logger.debug(
                    "Matching author for alias by L-distance", alias=alias, author=author, workspace_id=workspace_id
                )
                return add_alias_to_author(g, workspace_id, author, alias)

        new_author = g.backend.authors.create(workspace_id, _new_author_from_alias(alias))
        logger.debug("Creating new author for alias", alias=alias, author=new_author)
        return new_author


def get_or_create_optional_author_for_alias(
    g: GitentialContext, workspace_id: int, alias: AuthorAlias
) -> Optional[AuthorInDB]:
    if alias.name or alias.email or alias.login:
        return get_or_create_author_for_alias(g, workspace_id, alias)
    else:
        logger.debug("Skipping author matching, empty alias")
        return None


def add_alias_to_author(g: GitentialContext, workspace_id: int, author: AuthorInDB, alias: AuthorAlias) -> AuthorInDB:
    author_update = AuthorUpdate(**author.dict())
    author_update.aliases = _remove_duplicate_aliases(author_update.aliases + [alias])
    return g.backend.authors.update(workspace_id, author.id, author_update)


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


def alias_matching_author(alias: AuthorAlias, author: AuthorInDB):
    return any(aliases_matching(author_alias, alias) for author_alias in author.aliases)


def aliases_matching(first: AuthorAlias, second: AuthorAlias) -> bool:
    if first.email and second.email and first.email == second.email:
        return True
    elif first.login and second.login and first.login == second.login:
        return True
    for first_token, second_token in product(tokenize_alias(first), tokenize_alias(second)):
        if levenshtein_ratio(first_token, second_token) > 0.8:
            return True
    return False


def authors_matching(first: AuthorInDB, second: AuthorInDB):
    for alias in second.aliases:
        if alias_matching_author(alias, first):
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
        if len(_remove_common_words(email_first_part.split())) > 1 or len(email_first_part) >= 10:
            ret.append(email_first_part)
    if alias.login:
        ret.append(_tokenize_str(alias.login))
    ret = _remove_common_words(_remove_duplicates(ret))
    # logger.debug("Tokenized alias", alias=alias, tokens=ret)
    return ret


def _remove_duplicate_aliases(aliases: List[AuthorAlias]) -> List[AuthorAlias]:
    ret: List[AuthorAlias] = []
    for alias in aliases:
        if alias.email and alias.email in [r.email for r in ret]:
            continue
        elif alias.login and alias.login in [r.login for r in ret]:
            continue
        else:
            ret.append(alias)
    return ret


_COMMON_WORDS = ["mail", "info", "noreply", "email", "user", "test", "github", "github com"]
