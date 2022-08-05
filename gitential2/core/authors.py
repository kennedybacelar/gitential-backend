import re
import contextlib
from collections import defaultdict
from typing import Iterable, Dict, List, Optional, cast, Tuple, Union
from itertools import product
from unidecode import unidecode
from structlog import get_logger
from gitential2.datatypes.authors import (
    AuthorAlias,
    AuthorInDB,
    AuthorCreate,
    AuthorUpdate,
    AuthorPublicExt,
    IdAndTitle,
    AuthorFilters,
)
from gitential2.utils import levenshtein_ratio
from .context import GitentialContext
from .data_queries import process_data_query
from ..datatypes import (
    DataQuery,
    DQType,
    DQSourceName,
    DQFilterExpr,
    DQFunctionName,
    DQSingleColumnExpr,
    DQFnColumnExpr, DQResult,
)
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


def _get_author_ext(
    author: AuthorInDB,
    team_members: List[TeamMemberInDB],
    authors_in_projects: dict,
    teams_in_workspace: List[TeamInDB],
) -> AuthorPublicExt:
    projects_data = authors_in_projects.get(author.id) or {}
    project_ids, project_names = projects_data.get("project_ids") or [], projects_data.get("project_names") or []
    return AuthorPublicExt(
        id=author.id,
        created_at=author.created_at,
        updated_at=author.updated_at,
        active=author.active,
        name=author.name,
        email=author.email,
        aliases=author.aliases,
        projects=[
            IdAndTitle(id=project_data[0], title=project_data[1]) for project_data in zip(project_ids, project_names)
        ],
        teams=[
            IdAndTitle(
                id=team_member.team_id,
                title=_get_team_title(team_member, teams_in_workspace),
            )
            for team_member in team_members
            if team_member.author_id == author.id
        ],
    )


def list_authors_ext(g: GitentialContext, workspace_id: int, authors_in_projects: dict) -> List[AuthorPublicExt]:
    authors_from_db: List[AuthorInDB] = list_authors(g, workspace_id)
    author_ids: List[int] = [author.id for author in authors_from_db]
    teams_in_workspace: List[TeamInDB] = list(g.backend.teams.all(workspace_id))
    team_members: List[TeamMemberInDB] = g.backend.team_members.get_team_members_by_author_ids(
        workspace_id=workspace_id, author_ids=author_ids
    )
    result: List[AuthorPublicExt] = [
        _get_author_ext(
            author=author,
            team_members=team_members,
            authors_in_projects=authors_in_projects,
            teams_in_workspace=teams_in_workspace,
        )
        for author in authors_from_db
    ]
    return result


def list_authors_extended(g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters]):
    data_query_arguments = {
        "query_type": DQType.aggregate,
        "source_name": DQSourceName.calculated_commits,
        "selections": [DQFnColumnExpr(fn=DQFunctionName.COUNT)],
        "dimensions": [DQSingleColumnExpr(col="aid"), DQSingleColumnExpr(col="repo_id")],
    }

    if author_filters is not None:
        author_ids_in_teams: List[int] = (
            g.backend.team_members.get_author_ids_by_team_ids(
                workspace_id=workspace_id, team_ids=author_filters.team_ids
            )
            if len(author_filters.team_ids) > 0
            else []
        )

        repo_ids_in_projects: List[int] = (
            g.backend.project_repositories.get_repo_ids_by_project_ids(
                workspace_id=workspace_id, project_ids=author_filters.project_ids
            )
            if len(author_filters.project_ids) > 0
            else []
        )

        if author_filters.limit is not None and author_filters.limit > 0:
            data_query_arguments["limit"] = author_filters.limit
        if author_filters.offset is not None and author_filters.offset > -1:
            data_query_arguments["offset"] = author_filters.offset

        filters: List[DQFilterExpr] = []
        if len(author_filters.developer_ids) > 0 or len(author_ids_in_teams) > 0:
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN,
                    args=[
                        DQSingleColumnExpr(col="aid"),
                        list(set(author_filters.developer_ids + author_filters.developer_ids)),
                    ],
                )
            )
        if author_filters.date_range is not None:
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.BETWEEN,
                    args=[
                        DQSingleColumnExpr(col="atime"),
                        author_filters.date_range.start,
                        author_filters.date_range.end,
                    ],
                )
            )
        if len(author_filters.developer_names) > 0:
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="aname"), author_filters.developer_names]
                )
            )
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="cname"), author_filters.developer_names]
                )
            )
        if len(author_filters.developer_emails) > 0:
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="aemail"), author_filters.developer_emails]
                )
            )
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN, args=[DQSingleColumnExpr(col="cemail"), author_filters.developer_emails]
                )
            )
        if len(author_filters.repository_ids) > 0 or len(repo_ids_in_projects) > 0:
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN,
                    args=[
                        DQSingleColumnExpr(col="repo_id"),
                        list(set(author_filters.repository_ids + repo_ids_in_projects)),
                    ],
                )
            )
        if len(filters) > 0:
            data_query_arguments["filters"] = filters

    data_query = DataQuery(**data_query_arguments)
    result: DQResult = process_data_query(g=g, workspace_id=workspace_id, query=data_query)
    author_ids_all: List[int] = result.results.aid
    repo_ids_all: List[int] = result.results.repo_id

    if len(author_ids_all) > 0:
        author_ids = list(set(author_ids_all))
        # repo_ids = list(set(repo_ids_all))

        authors: List[AuthorInDB] = g.backend.authors.get_authors_by_author_ids(
            workspace_id=workspace_id,
            author_ids=author_ids
        )

        team_members: List[TeamMemberInDB] = g.backend.team_members.get_team_members_by_author_ids(
            workspace_id=workspace_id, author_ids=author_ids
        )
        teams: List[TeamInDB] = []
        if len(team_members) > 0:
            team_ids = [t_member.id for t_member in team_members]
            teams = g.backend.teams.get_teams_by_team_ids(workspace_id=workspace_id, team_ids=team_ids)

        author_ids_with_repo_ids: dict = defaultdict(lambda: set())
        for index, aid in enumerate(author_ids_all):
            author_ids_with_repo_ids[aid].append(repo_ids_all[index])

        pass


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
