from collections import defaultdict
from itertools import chain
from typing import List, Dict, Optional

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
)
from gitential2.datatypes.teammembers import TeamMemberInDB
from gitential2.datatypes.teams import TeamInDB


def list_authors_extended(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> AuthorsPublicExtendedSearchResult:
    data_query_result: DQResult = __get_data_query_result(g=g, workspace_id=workspace_id, author_filters=author_filters)

    authors_ext_list: List[AuthorPublicExtended] = __get_extended_authors_list(
        g=g, workspace_id=workspace_id, data_query_result=data_query_result
    )

    result = AuthorsPublicExtendedSearchResult(
        total=data_query_result.total,
        limit=data_query_result.limit,
        offset=data_query_result.offset,
        authors_list=authors_ext_list,
    )

    return result


def __get_data_query_result(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> DQResult:
    data_query_arguments = {
        "query_type": DQType.aggregate,
        "source_name": DQSourceName.calculated_commits,
        "selections": [DQFnColumnExpr(fn=DQFunctionName.COUNT)],
        "dimensions": [DQSingleColumnExpr(col="aid"), DQSingleColumnExpr(col="repo_id")],
    }

    if author_filters.limit is not None and author_filters.limit > 0:
        data_query_arguments["limit"] = author_filters.limit
    if author_filters.offset is not None and author_filters.offset > -1:
        data_query_arguments["offset"] = author_filters.offset

    filters = __get_filters_for_data_query(g=g, workspace_id=workspace_id, author_filters=author_filters)
    if len(filters) > 0:
        data_query_arguments["filters"] = filters

    data_query = DataQuery(**data_query_arguments)
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
    return filters


def __get_extended_authors_list(
    g: GitentialContext, workspace_id: int, data_query_result: DQResult
) -> List[AuthorPublicExtended]:
    author_ids_all: List[int] = data_query_result.results.aid

    result: List[AuthorPublicExtended] = []

    if len(author_ids_all) > 0:
        repo_ids_all: List[int] = data_query_result.results.repo_id

        author_ids_distinct = list(set(author_ids_all))
        authors: List[AuthorInDB] = g.backend.authors.get_authors_by_author_ids(
            workspace_id=workspace_id, author_ids=author_ids_distinct
        )

        author_ids_with_team_details_lists = __get_author_ids_with_teams_lists(
            g=g, workspace_id=workspace_id, author_ids_distinct=author_ids_distinct
        )
        author_ids_with_project_details_lists = __get_author_ids_with_projects_lists(
            g=g, workspace_id=workspace_id, author_ids_all=author_ids_all, repo_ids_all=repo_ids_all
        )

        result = [
            AuthorPublicExtended(
                teams=author_ids_with_team_details_lists[author.id],
                projects=author_ids_with_project_details_lists[author.id],
                **author,
            )
            for author in authors
        ]

    return result


def __get_author_ids_with_teams_lists(
    g: GitentialContext, workspace_id: int, author_ids_distinct: List[int]
) -> Dict[int, List[IdAndTitle]]:
    team_members: List[TeamMemberInDB] = g.backend.team_members.get_team_members_by_author_ids(
        workspace_id=workspace_id, author_ids=author_ids_distinct
    )
    teams: List[TeamInDB] = []
    if len(team_members) > 0:
        team_ids = [t_member.id for t_member in team_members]
        teams = g.backend.teams.get_teams_by_team_ids(workspace_id=workspace_id, team_ids=team_ids)

    result: Dict[int, List[IdAndTitle]] = defaultdict(lambda: [])
    for index, author_id in enumerate(author_ids_distinct):
        team_ids: List[int] = [tm.team_id for tm in team_members if author_id == tm.author_id]
        if len(team_ids) > 0:
            teams_for_author: List[TeamInDB] = next(filter(lambda x: x.id in team_ids, teams), [])
            if len(teams_for_author) > 0:
                team_ids_and_titles: List[IdAndTitle] = [IdAndTitle(id=t.id, title=t.name) for t in teams_for_author]
                result[author_id] += team_ids_and_titles

    return result


def __get_author_ids_with_projects_lists(
    g: GitentialContext, workspace_id: int, author_ids_all: List[int], repo_ids_all: List[int]
) -> Dict[int, List[IdAndTitle]]:
    repo_ids_distinct = list(set(repo_ids_all))
    # author_ids_distinct = list(set(author_ids_all))

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
