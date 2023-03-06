from collections import defaultdict
from itertools import chain
from typing import List, Dict, Optional, Any, OrderedDict, cast

from gitential2.core import GitentialContext
from gitential2.core.data_queries import process_data_query
from gitential2.datatypes import (
    ProjectInDB,
    DQType,
    DQSourceName,
    DQFnColumnExpr,
    DQSingleColumnExpr,
    DQFunctionName,
    DQFilterExpr,
    DataQuery,
    DQResult,
)
from gitential2.datatypes.authors import (
    IdAndTitle,
    AuthorFilters,
    AuthorInDB,
    AuthorsPublicExtendedSearchResult,
    AuthorPublicExtended,
    AuthorsSorting,
    AuthorsSortingType,
)
from gitential2.datatypes.teammembers import TeamMemberInDB
from gitential2.datatypes.teams import TeamInDB
from gitential2.utils import is_list_not_empty

from sqlalchemy import distinct, func, select, and_, asc


def get_author_extended(g: GitentialContext, workspace_id: int, author_id: int) -> Optional[AuthorPublicExtended]:
    return __get_extended_authors_list(
        g=g,
        workspace_id=workspace_id,
        author_ids_from_other_query=[author_id],
    )[0]


def list_authors_extended(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> AuthorsPublicExtendedSearchResult:

    # Missing getting parameters from the query to make it dynamic

    engine = g.backend.authors.engine

    authors_table = g.backend.authors.table
    calculated_commits_table = g.backend.calculated_commits.table
    team_members_table = g.backend.team_members.table
    teams_table = g.backend.teams.table
    project_repositories_table = g.backend.project_repositories.table
    projects_table = g.backend.projects.table

    param_min_date = "2022-01-01"
    param_max_date = "2023-01-01"
    param_teams = []
    param_projects = []
    param_authors = []
    param_offset = 0
    param_limit = 5

    subquery = (
        select(func.count())
        .select_from(
            authors_table.join(calculated_commits_table, authors_table.c.id == calculated_commits_table.c.aid)
            .outerjoin(team_members_table, authors_table.c.id == team_members_table.c.author_id)
            .outerjoin(teams_table, team_members_table.c.team_id == teams_table.c.id)
            .join(
                project_repositories_table, calculated_commits_table.c.repo_id == project_repositories_table.c.repo_id
            )
            .outerjoin(projects_table, project_repositories_table.c.project_id == projects_table.c.id)
        )
        .where(
            and_(
                calculated_commits_table.c.date.between(
                    func.coalesce(param_min_date, calculated_commits_table.c.date),
                    func.coalesce(param_max_date, calculated_commits_table.c.date),
                ),
                teams_table.c.id.in_(param_teams) if param_teams else True,
                projects_table.c.id.in_(param_projects) if param_projects else True,
                authors_table.c.id.in_(param_authors) if param_authors else True,
            )
        )
    )

    query = (
        select(
            authors_table.c.id,
            authors_table.c.active,
            authors_table.c.name,
            authors_table.c.email,
            authors_table.c.aliases,
            func.array_agg(distinct(teams_table.c.id)).label("teams_ids"),
            func.array_agg(distinct(projects_table.c.id)).label("projects_ids"),
            subquery.scalar_subquery().label("total_count"),
        )
        .select_from(
            authors_table.join(calculated_commits_table, authors_table.c.id == calculated_commits_table.c.aid)
            .outerjoin(team_members_table, authors_table.c.id == team_members_table.c.author_id)
            .outerjoin(teams_table, team_members_table.c.team_id == teams_table.c.id)
            .join(
                project_repositories_table, calculated_commits_table.c.repo_id == project_repositories_table.c.repo_id
            )
            .outerjoin(projects_table, project_repositories_table.c.project_id == projects_table.c.id)
        )
        .where(
            and_(
                calculated_commits_table.c.date.between(
                    func.coalesce(param_min_date, calculated_commits_table.c.date),
                    func.coalesce(param_max_date, calculated_commits_table.c.date),
                ),
                teams_table.c.id.in_(param_teams) if param_teams else True,
                projects_table.c.id.in_(param_projects) if param_projects else True,
                authors_table.c.id.in_(param_authors) if param_authors else True,
            )
        )
        .group_by(authors_table.c.id)
        .order_by(asc(authors_table.c.name))
        .offset(param_offset)
        .limit(param_limit)
    )

    with engine.connect().execution_options(
        autocommit=True,
        schema_translate_map={None: f"ws_{workspace_id}"},
    ) as conn:
        authors = conn.execute(query).fetchall()

    import pprint

    total_row_count = authors[0][-1]
    authors_ret = []
    for author in authors:
        author_ext = AuthorPublicExtended(
            id=author.id,
            name=author.name,
            active=author.active,
            aliases=author.aliases,
            teams=g.backend.teams.get_teams_ids_and_names(workspace_id, author.teams_ids),
            projects=g.backend.projects.get_projects_ids_and_names(workspace_id, author.projects_ids),
        )
        authors_ret.append(author_ext)

    # authors_extended = [AuthorPublicExtended(author) for author in authors]
    t_ids = g.backend.teams.get_teams_ids_and_names(workspace_id, author.teams_ids)

    # pprint.pprint(t_ids)
    pprint.pprint(authors_ret)
    # exit()

    """
    __sort_authors(
    authors=authors,
    sorting_details=getattr(
        author_filters, "sorting_details", AuthorsSorting(type=AuthorsSortingType.name, is_desc=False)
    ),
    )
    """

    # limit: int = getattr(author_filters, "limit", 5)
    # offset: int = getattr(author_filters, "offset", 0)
    # authors = authors[offset:][:limit]

    return AuthorsPublicExtendedSearchResult(
        total=total_row_count,
        limit=param_limit,
        offset=param_offset,
        authors_list=authors_ret,
    )


def __get_author_ids_list_with_filters_applied(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> List[int]:
    data_query_arguments: Dict[str, Any] = {}

    if author_filters is not None:
        filters: List[DQFilterExpr] = __get_filters_for_data_query(
            g=g, workspace_id=workspace_id, author_filters=author_filters
        )
        if is_list_not_empty(filters):
            data_query_arguments["filters"] = filters

    data_query = DataQuery(
        query_type=DQType.aggregate,
        source_name=DQSourceName.calculated_commits,
        selections=[DQFnColumnExpr(fn=DQFunctionName.COUNT)],
        dimensions=[DQSingleColumnExpr(col="aid")],
        **data_query_arguments,
    )
    data_query_result: DQResult = process_data_query(g=g, workspace_id=workspace_id, query=data_query)

    return (getattr(data_query_result, "results", {}) or {}).get("aid", [])


def __get_filters_for_data_query(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> List[DQFilterExpr]:
    filters: List[DQFilterExpr] = []

    if author_filters is not None:
        author_ids_in_teams: List[int] = (
            g.backend.team_members.get_author_ids_by_team_ids(
                workspace_id=workspace_id, team_ids=author_filters.team_ids
            )
            if author_filters.team_ids is not None and len(author_filters.team_ids) > 0
            else []
        )

        repo_ids_in_projects: List[int] = (
            g.backend.project_repositories.get_repo_ids_by_project_ids(
                workspace_id=workspace_id, project_ids=author_filters.project_ids
            )
            if author_filters.project_ids is not None and len(author_filters.project_ids) > 0
            else []
        )

        if is_list_not_empty(author_filters.developer_ids) or is_list_not_empty(author_ids_in_teams):
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN,
                    args=[
                        DQSingleColumnExpr(col="aid"),
                        list(set((author_filters.developer_ids or []) + (author_ids_in_teams or []))),
                    ],
                )
            )
        if author_filters.date_range is not None:
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.BETWEEN,
                    args=[
                        DQSingleColumnExpr(col="atime"),
                        # It is necessary to do this cast or the app will crash when it gets here
                        # because it is expecting a string, not a date as it is stated in the AuthorFilters class.
                        str(cast(str, author_filters.date_range.start)),
                        str(cast(str, author_filters.date_range.end)),
                    ],
                )
            )
        if is_list_not_empty(author_filters.developer_names):
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
        if is_list_not_empty(author_filters.developer_emails):
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
        if is_list_not_empty(author_filters.repository_ids) or is_list_not_empty(repo_ids_in_projects):
            filters.append(
                DQFilterExpr(
                    fn=DQFunctionName.IN,
                    args=[
                        DQSingleColumnExpr(col="repo_id"),
                        list(set((author_filters.repository_ids or []) + (repo_ids_in_projects or []))),
                    ],
                )
            )
    return filters


def __get_extended_authors_list(
    g: GitentialContext,
    workspace_id: int,
    author_ids_from_other_query: List[int],
) -> List[AuthorPublicExtended]:
    result: List[AuthorPublicExtended] = []
    if is_list_not_empty(author_ids_from_other_query):
        data_query_result: DQResult = __get_data_query_result_for_authors_repos(
            g=g,
            workspace_id=workspace_id,
            author_ids=list(set(author_ids_from_other_query)),
        )

        author_ids_all: List[int] = data_query_result.results.get("aid", [])  # type: ignore
        repo_ids_all: List[int] = data_query_result.results.get("repo_id", [])  # type: ignore

        author_ids_distinct = list(OrderedDict.fromkeys(author_ids_all))
        authors: List[AuthorInDB] = g.backend.authors.get_authors_by_author_ids(
            workspace_id=workspace_id, author_ids=author_ids_distinct
        )

        authors_sorted: List[AuthorInDB] = [__find(authors, aid) for aid in author_ids_distinct]

        author_ids_with_team_details_lists = __get_author_ids_with_teams_lists(
            g=g, workspace_id=workspace_id, author_ids_distinct=author_ids_distinct
        )
        author_ids_with_project_details_lists = __get_author_ids_with_projects_lists(
            g=g,
            workspace_id=workspace_id,
            author_ids_all=author_ids_all,
            repo_ids_all=repo_ids_all,
        )

        result = [
            AuthorPublicExtended(
                teams=author_ids_with_team_details_lists[author.id],
                projects=author_ids_with_project_details_lists[author.id],
                id=author.id,
                created_at=author.created_at,
                updated_at=author.updated_at,
                active=author.active,
                name=author.name,
                email=author.email,
                aliases=author.aliases,
            )
            for author in authors_sorted
            if author
        ]

    return result


def __get_data_query_result_for_authors_repos(
    g: GitentialContext,
    workspace_id: int,
    author_ids: List[int],
) -> DQResult:
    authors_filters: List[DQFilterExpr] = [
        DQFilterExpr(
            fn=DQFunctionName.IN,
            args=[
                DQSingleColumnExpr(col="aid"),
                author_ids,
            ],
        )
    ]

    data_query = DataQuery(
        query_type=DQType.aggregate,
        source_name=DQSourceName.calculated_commits,
        selections=[DQFnColumnExpr(fn=DQFunctionName.COUNT)],
        dimensions=[DQSingleColumnExpr(col="aid"), DQSingleColumnExpr(col="repo_id")],
        filters=authors_filters,
    )
    data_query_result: DQResult = process_data_query(g=g, workspace_id=workspace_id, query=data_query)

    return data_query_result


def __find(authors: List[AuthorInDB], aid: int):
    return next(filter(lambda a: a.id == aid, authors), None)


def __get_author_ids_with_teams_lists(
    g: GitentialContext,
    workspace_id: int,
    author_ids_distinct: List[int],
) -> Dict[int, List[IdAndTitle]]:
    team_members: List[TeamMemberInDB] = g.backend.team_members.get_team_members_by_author_ids(
        workspace_id=workspace_id, author_ids=author_ids_distinct
    )
    teams: List[TeamInDB] = []
    if is_list_not_empty(team_members):
        team_ids = [t_member.team_id for t_member in team_members]
        teams = g.backend.teams.get_teams_by_team_ids(workspace_id=workspace_id, team_ids=team_ids)

    result: Dict[int, List[IdAndTitle]] = defaultdict(lambda: [])
    for author_id in author_ids_distinct:
        team_ids_for_author: List[int] = [tm.team_id for tm in team_members if author_id == tm.author_id]
        if is_list_not_empty(team_ids_for_author):
            teams_for_author: List[TeamInDB] = [t for t in teams if t.id in team_ids_for_author]
            if is_list_not_empty(teams_for_author):
                team_ids_and_titles: List[IdAndTitle] = [IdAndTitle(id=t.id, title=t.name) for t in teams_for_author]
                result[author_id] += team_ids_and_titles

    return result


def __get_author_ids_with_projects_lists(
    g: GitentialContext,
    workspace_id: int,
    author_ids_all: List[int],
    repo_ids_all: List[int],
) -> Dict[int, List[IdAndTitle]]:
    repo_ids_distinct = list(set(repo_ids_all))

    # We need to get the project id list for each repository id
    project_ids_for_repo_id: Dict[int, List[int]] = g.backend.project_repositories.get_project_ids_for_repo_ids(
        workspace_id=workspace_id, repo_ids=repo_ids_distinct
    )

    # This is the distinct list of the project ids. We need these to get the projects from the database.
    project_ids_distinct: List[int] = list(
        set(list(chain.from_iterable([project_ids_for_repo_id[key] for key in project_ids_for_repo_id])))
    )
    # Get projects list by the project ids.
    projects: List[ProjectInDB] = g.backend.projects.get_projects_by_ids(
        workspace_id=workspace_id, project_ids=project_ids_distinct
    )

    # The final move is that we need to create a dict where the author ids will be the keys and the
    # values will be the list of IdAndTitle instances created from "projects: List[ProjectInDB]".
    result: Dict[int, List[IdAndTitle]] = defaultdict(lambda: [])
    for index, author_id in enumerate(author_ids_all):
        repo_id_for_author: int = repo_ids_all[index]
        project_id_list_for_one_repo_id: List[int] = project_ids_for_repo_id[repo_id_for_author]
        for p in projects:
            if p.id in project_id_list_for_one_repo_id and not any(x.id == p.id for x in result[author_id]):
                result[author_id].append(IdAndTitle(id=p.id, title=p.name))

    return result


def __sort_authors(authors: List[AuthorPublicExtended], sorting_details: AuthorsSorting):
    if sorting_details.type is AuthorsSortingType.name:
        authors.sort(key=lambda x: (x.name or "z").lower(), reverse=sorting_details.is_desc)
    elif sorting_details.type is AuthorsSortingType.email:
        authors.sort(key=lambda x: (getattr(x, "email", "z") or "z").lower(), reverse=sorting_details.is_desc)
    elif sorting_details.type is AuthorsSortingType.active:
        authors.sort(key=lambda x: x.active, reverse=sorting_details.is_desc)
    elif sorting_details.type is AuthorsSortingType.projects:
        authors.sort(
            key=lambda x: (
                (getattr(x.projects[0], "title", "z") if x.projects is not None and len(x.projects) > 0 else "z") or "z"
            ).lower(),
            reverse=sorting_details.is_desc,
        )
    elif sorting_details.type is AuthorsSortingType.teams:
        authors.sort(
            key=lambda x: (
                (getattr(x.teams[0], "title", "z") if x.teams is not None and len(x.teams) > 0 else "z") or "z"
            ).lower(),
            reverse=sorting_details.is_desc,
        )
