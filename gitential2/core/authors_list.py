from collections import defaultdict
from itertools import chain
from typing import List, Dict, Optional, Any, OrderedDict

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
    DQSortByExpr,
)
from gitential2.datatypes.authors import (
    IdAndTitle,
    AuthorFilters,
    AuthorInDB,
    AuthorsPublicExtendedSearchResult,
    AuthorPublicExtended,
    DateRange,
)
from gitential2.datatypes.teammembers import TeamMemberInDB
from gitential2.datatypes.teams import TeamInDB
from gitential2.utils import is_list_not_empty


def list_authors_extended(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> AuthorsPublicExtendedSearchResult:
    data_query_result: DQResult = __get_data_query_result_for_authors_filtering(
        g=g, workspace_id=workspace_id, author_filters=author_filters
    )

    authors_ext_list: List[AuthorPublicExtended] = __get_extended_authors_list(
        g=g,
        workspace_id=workspace_id,
        author_ids_from_other_query=data_query_result.results["aid"],  # type: ignore
        sort_by_name_is_desc=author_filters.sort_by_name_is_desc
        if author_filters is not None and author_filters.sort_by_name_is_desc is not None
        else False,
        date_range=author_filters.date_range if author_filters is not None else None,
    )

    result = AuthorsPublicExtendedSearchResult(
        total=data_query_result.total,
        limit=data_query_result.limit,
        offset=data_query_result.offset,
        authors_list=authors_ext_list,
    )

    return result


def __get_data_query_result_for_authors_filtering(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> DQResult:
    data_query_arguments: Dict[str, Any] = {}

    if author_filters is not None:
        if author_filters.limit is not None and author_filters.limit > 0:
            data_query_arguments["limit"] = author_filters.limit
        if author_filters.offset is not None and author_filters.offset > -1:
            data_query_arguments["offset"] = author_filters.offset

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

    return data_query_result


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
                        list(set((author_filters.developer_ids or []) + (author_filters.developer_ids or []))),
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
    sort_by_name_is_desc: bool,
    date_range: Optional[DateRange] = None,
) -> List[AuthorPublicExtended]:
    result: List[AuthorPublicExtended] = []
    if is_list_not_empty(author_ids_from_other_query):
        data_query_result: DQResult = __get_data_query_result_for_authors_repos(
            g=g,
            workspace_id=workspace_id,
            author_ids=list(set(author_ids_from_other_query)),
            sort_by_name_is_desc=sort_by_name_is_desc,
            date_range=date_range,
        )

        author_ids_all: List[int] = data_query_result.results["aid"]  # type: ignore
        repo_ids_all: List[int] = data_query_result.results["repo_id"]  # type: ignore

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
        ]

    return result


def __get_data_query_result_for_authors_repos(
    g: GitentialContext,
    workspace_id: int,
    author_ids: List[int],
    sort_by_name_is_desc: bool,
    date_range: Optional[DateRange] = None,
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

    if date_range is not None:
        authors_filters.append(
            DQFilterExpr(
                fn=DQFunctionName.BETWEEN,
                args=[
                    DQSingleColumnExpr(col="atime"),
                    date_range.start,
                    date_range.end,
                ],
            )
        )

    data_query = DataQuery(
        query_type=DQType.aggregate,
        source_name=DQSourceName.calculated_commits,
        selections=[DQFnColumnExpr(fn=DQFunctionName.COUNT)],
        dimensions=[DQSingleColumnExpr(col="aid"), DQSingleColumnExpr(col="repo_id"), DQSingleColumnExpr(col="aname")],
        sort_by=[DQSortByExpr(col="aname", desc=sort_by_name_is_desc)],
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
