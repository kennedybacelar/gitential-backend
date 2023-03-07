from typing import Optional, List
from sqlalchemy import distinct, func, select, and_, asc, desc
from sqlalchemy.sql.schema import Table

from gitential2.core import GitentialContext
from gitential2.datatypes.authors import (
    AuthorFilters,
    AuthorsPublicExtendedSearchResult,
    AuthorPublicExtended,
    AuthorsSorting,
    AuthorsSortingType,
)


def get_author_extended(g: GitentialContext, workspace_id: int, author_id: int) -> Optional[AuthorPublicExtended]:
    author_filters = AuthorFilters(developer_ids=[author_id])
    author_extended_public_extended = list_authors_extended(g, workspace_id, author_filters)
    if author_extended_public_extended:
        return author_extended_public_extended.authors_list[0] if author_extended_public_extended.authors_list else None
    return None


def list_authors_extended(
    g: GitentialContext, workspace_id: int, author_filters: Optional[AuthorFilters] = None
) -> AuthorsPublicExtendedSearchResult:

    if not author_filters:
        author_filters = AuthorFilters()

    engine = __get_sqlalchemy_engine(g)

    authors_table = g.backend.authors.table
    calculated_commits_table = g.backend.calculated_commits.table
    team_members_table = g.backend.team_members.table
    teams_table = g.backend.teams.table
    project_repositories_table = g.backend.project_repositories.table
    projects_table = g.backend.projects.table

    param_min_date = author_filters.date_range.start if author_filters.date_range else None
    param_max_date = author_filters.date_range.end if author_filters.date_range else None
    sorting_direction, sorting_column = __getting_sorting_details(
        author_filters.sorting_details, g.backend.authors.table
    )

    subquery = (
        select(func.count(distinct(authors_table.c.id)))
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
                teams_table.c.id.in_(author_filters.team_ids) if author_filters.team_ids else True,
                projects_table.c.id.in_(author_filters.project_ids) if author_filters.project_ids else True,
                authors_table.c.id.in_(author_filters.developer_ids) if author_filters.developer_ids else True,
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
            func.array_agg(distinct(teams_table.c.name)).label("team_names"),
            func.array_agg(distinct(projects_table.c.name)).label("project_names"),
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
                teams_table.c.id.in_(author_filters.team_ids) if author_filters.team_ids else True,
                projects_table.c.id.in_(author_filters.project_ids) if author_filters.project_ids else True,
                authors_table.c.id.in_(author_filters.developer_ids) if author_filters.developer_ids else True,
                authors_table.c.name.in_(author_filters.developer_names) if author_filters.developer_names else True,
                authors_table.c.email.in_(author_filters.developer_emails) if author_filters.developer_emails else True,
                project_repositories_table.c.repo_id.in_(author_filters.repository_ids)
                if author_filters.repository_ids
                else True,
            )
        )
        .group_by(authors_table.c.id)
        .order_by(sorting_direction(sorting_column))
        .offset(author_filters.offset)
        .limit(author_filters.limit)
    )

    with engine.connect().execution_options(
        autocommit=True,
        schema_translate_map={None: f"ws_{workspace_id}"},
    ) as conn:
        authors = conn.execute(query).fetchall()

    authors_ret = []
    if authors:
        authors_ret = __transform_to_author_public_extended(g, workspace_id, authors)

    total_row_count = authors[0][-1] if authors else 0
    return AuthorsPublicExtendedSearchResult(
        total=total_row_count,
        limit=author_filters.limit,
        offset=author_filters.offset,
        authors_list=authors_ret,
    )


def __getting_sorting_details(author_sorting: AuthorsSorting, table: Table):

    sorting_direction = desc if author_sorting.is_desc else asc

    sorting_column = {
        AuthorsSortingType.name: table.c.name,
        AuthorsSortingType.email: table.c.email,
        AuthorsSortingType.active: table.c.active,
        AuthorsSortingType.projects: "project_names",
        AuthorsSortingType.teams: "team_names",
    }

    return sorting_direction, sorting_column.get(author_sorting.type, table.c.name)


def __get_sqlalchemy_engine(g: GitentialContext):
    return g.backend.authors.engine


def __transform_to_author_public_extended(
    g: GitentialContext, workspace_id: int, authors: List
) -> List[AuthorPublicExtended]:
    authors_ret = []
    for author in authors:
        author_extended = AuthorPublicExtended(
            id=author.id,
            name=author.name,
            active=author.active,
            aliases=author.aliases,
            teams=g.backend.teams.get_teams_ids_and_names(workspace_id, author.teams_ids),
            projects=g.backend.projects.get_projects_ids_and_names(workspace_id, author.projects_ids),
        )
        authors_ret.append(author_extended)
    return authors_ret
