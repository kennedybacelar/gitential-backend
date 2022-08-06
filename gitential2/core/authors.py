import contextlib
import re
from itertools import product
from typing import Iterable, Dict, List, Optional, cast, Tuple, Union

from structlog import get_logger
from unidecode import unidecode

from gitential2.datatypes.authors import (
    AuthorAlias,
    AuthorInDB,
    AuthorCreate,
    AuthorUpdate,
)
from gitential2.utils import levenshtein_ratio
from .context import GitentialContext
from ..datatypes.teammembers import TeamMemberInDB
from ..datatypes.teams import TeamInDB

logger = get_logger(__name__)


def list_active_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return [author for author in list_authors(g, workspace_id) if author.active]


def list_active_author_ids(g: GitentialContext, workspace_id: int) -> List[int]:
    ret = cast(list, g.kvstore.get_value(f"active-authors-{workspace_id}"))
    if not ret:
        ret = [author.id for author in list_active_authors(g, workspace_id=workspace_id)]
        g.kvstore.set_value(f"active-authors-{workspace_id}", ret, 60)

    return ret


@contextlib.contextmanager
def authors_change_lock(
    g: GitentialContext,
    workspace_id: int,
    blocking_timeout_seconds=None,
    timeout_seconds=30,
):
    with g.kvstore.lock(
        f"authors-change-lock{workspace_id}", timeout=timeout_seconds, blocking_timeout=blocking_timeout_seconds
    ):
        yield


def _get_team_title(team_member: TeamMemberInDB, teams_in_workspace: List[TeamInDB]) -> Union[str, None]:
    result = None
    for team in teams_in_workspace:
        if team.id == team_member.team_id:
            result = team.name
            break
    return result


def list_authors(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    return list(g.backend.authors.all(workspace_id))


def update_author(g: GitentialContext, workspace_id: int, author_id: int, author_update: AuthorUpdate):
    # existing_author = g.backend.authors.get_or_error(workspace_id, author_id)
    with authors_change_lock(g, workspace_id):
        _reset_all_authors_from_cache(g, workspace_id)
        return g.backend.authors.update(workspace_id, author_id, author_update)


def merge_authors(g: GitentialContext, workspace_id: int, authors: List[AuthorInDB]) -> AuthorInDB:
    first, *rest = authors
    author_update = AuthorUpdate(**first.dict())
    with authors_change_lock(g, workspace_id):
        for other in rest:
            author_update.aliases += other.aliases  # pylint: disable=no-member
            delete_author(g, workspace_id, other.id)
        author_update.aliases = _remove_duplicate_aliases(author_update.aliases)
        _reset_all_authors_from_cache(g, workspace_id)
        return g.backend.authors.update(workspace_id, first.id, author_update)


def developer_map_callback(
    alias: AuthorAlias,
    g: GitentialContext,
    workspace_id: int,
) -> Optional[int]:
    author = get_or_create_optional_author_for_alias(g, workspace_id, alias)
    if author:
        return author.id
    else:
        return None


def delete_author(g: GitentialContext, workspace_id: int, author_id: int) -> int:
    with authors_change_lock(g, workspace_id):
        team_ids = g.backend.team_members.get_author_team_ids(workspace_id, author_id)
        for team_id in team_ids:
            g.backend.team_members.remove_members_from_team(workspace_id, team_id, [author_id])
        _reset_all_authors_from_cache(g, workspace_id)
        return g.backend.authors.delete(workspace_id, author_id)


def create_author(g: GitentialContext, workspace_id: int, author_create: AuthorCreate):
    _reset_all_authors_from_cache(g, workspace_id)
    return g.backend.authors.create(workspace_id, author_create)


def get_author(g: GitentialContext, workspace_id: int, author_id: int) -> Optional[AuthorInDB]:
    return g.backend.authors.get(workspace_id, author_id)


def fix_author_names(g: GitentialContext, workspace_id: int):
    with authors_change_lock(g, workspace_id):
        fixed_count = 0
        for author in g.backend.authors.all(workspace_id):
            if not author.name:
                logger.info("Fixing author name", author=author, workspace_id=workspace_id)
                possible_names = [alias.name for alias in author.aliases] + [alias.login for alias in author.aliases]
                if not possible_names:
                    logger.warning("No names for author", author=author, workspace_id=workspace_id)
                    continue
                # pylint: disable=unnecessary-lambda
                sorted_names = sorted(possible_names, key=lambda x: len(x) if x else 0, reverse=True)
                author.name = sorted_names[0]
                logger.info("Updating author name", workspace_id=workspace_id, author=author)
                g.backend.authors.update(workspace_id, author.id, cast(AuthorUpdate, author))
                fixed_count += 1
        if fixed_count > 0:
            _reset_all_authors_from_cache(g, workspace_id)


def fix_author_aliases(g: GitentialContext, workspace_id: int):
    with authors_change_lock(g, workspace_id):
        fixed_count = 0
        for author in g.backend.authors.all(workspace_id):
            aliases = _remove_duplicate_aliases(author.aliases)
            if len(aliases) != len(author.aliases):
                author.aliases = aliases
                g.backend.authors.update(workspace_id, author.id, cast(AuthorUpdate, author))
                logger.info("Fixed author aliases", workspace_id=workspace_id, author=author)
                fixed_count += 1
        if fixed_count > 0:
            _reset_all_authors_from_cache(g, workspace_id)


def _get_all_authors_from_cache(g: GitentialContext, workspace_id: int) -> List[AuthorInDB]:
    def _to_list(authors: List[AuthorInDB]) -> List[dict]:
        return [a.dict() for a in authors]

    def _from_list(l: list) -> List[AuthorInDB]:
        return [AuthorInDB(**author_dict) for author_dict in l]

    cache_key = f"authors_ws_{workspace_id}"

    from_cache = g.kvstore.get_value(cache_key)
    if from_cache:
        return _from_list(cast(list, from_cache))
    else:
        all_authors = list(g.backend.authors.all(workspace_id))
        g.kvstore.set_value(cache_key, _to_list(all_authors))
        return all_authors


def _reset_all_authors_from_cache(g: GitentialContext, workspace_id: int):
    cache_key = f"authors_ws_{workspace_id}"
    return g.kvstore.delete_value(cache_key)


def get_or_create_author_for_alias(g: GitentialContext, workspace_id: int, alias: AuthorAlias) -> AuthorInDB:
    with authors_change_lock(g, workspace_id):
        all_authors = _get_all_authors_from_cache(g, workspace_id)
        alias_to_author_map = _build_alias_author_map(all_authors)
        alias_tuple = (alias.name, alias.email, alias.login)
        if alias_tuple in alias_to_author_map:
            return alias_to_author_map[alias_tuple]
        else:
            _reset_all_authors_from_cache(g, workspace_id)
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


def _build_alias_author_map(
    author_list: Iterable[AuthorInDB],
) -> Dict[Tuple[Optional[str], Optional[str], Optional[str]], AuthorInDB]:
    ret = {}
    for author in author_list:
        for alias in author.aliases or []:
            if not alias.is_empty():
                ret[(alias.name, alias.email, alias.login)] = author
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
        if first_token and second_token:
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
        elif any(a.name == alias.name and a.login == alias.login and a.email == alias.email for a in ret):
            continue
        elif not alias.is_empty():
            ret.append(alias)
    return ret


_COMMON_WORDS = ["mail", "info", "noreply", "email", "user", "test", "github", "github com"]
