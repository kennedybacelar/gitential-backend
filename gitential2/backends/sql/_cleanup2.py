from datetime import date, datetime, timedelta
from typing import Optional, List
from enum import Enum

from sqlalchemy import select, or_

from gitential2.core import GitentialContext
from gitential2.datatypes.cli_v2 import CleanupType


class Commits(str, Enum):
    calculated_commits = "calculated_commits"
    extracted_commits = "extracted_commits"
    extracted_patches = "extracted_patches"
    calculated_patches = "calculated_patches"
    extracted_patch_rewrites = "extracted_patch_rewrites"
    extracted_commit_branches = "extracted_commit_branches"


def perform_data_cleanup_(
    g: GitentialContext,
    remove_residual_data: bool = False,
    workspace_ids: Optional[List[int]] = None,
    cleanup_type: Optional[CleanupType] = CleanupType.full,
):

    for workspace_id in workspace_ids:
        date_to = __get_date_to(g.settings.extraction.repo_analysis_limit_in_days)
        repo_ids_to_delete = __get_repo_ids_to_delete(g, workspace_id)
        if date_to:
            __remove_redundant_commit_data(g, workspace_id, date_to, repo_ids_to_delete)


def get_commit_ids_table(
    g: GitentialContext,
    date_to: datetime,
    repo_ids_to_delete: List[int],
):
    # Creating common table expression and returning the commit_ids
    table_ = g.backend.calculated_commits
    return (
        select([table_.table.c.commit_id])
        .where(or_(table_.table.c.date <= date_to, table_.table.c.repo_id.in_(repo_ids_to_delete)))
        .cte()
    )


def __remove_redundant_commit_data(
    g: GitentialContext,
    workspace_id: int,
    date_to: datetime,
    repo_ids_to_delete: List[int],
):
    cte = get_commit_ids_table(g, date_to, repo_ids_to_delete)  # common table expression
    for commit_table in Commits:
        table_ = g.backend.__getattribute__(commit_table.value)
        delete_records(workspace_id, table_, cte)


def __get_date_to(number_of_days_diff: Optional[int] = None) -> Optional[datetime]:
    return (
        datetime.utcnow() - timedelta(days=number_of_days_diff)
        if number_of_days_diff and number_of_days_diff > 0
        else None
    )


def __get_repo_ids_to_delete(g: GitentialContext, workspace_id: int) -> List[int]:
    repo_ids_all: List[int] = [r.id for r in g.backend.repositories.all(workspace_id=workspace_id)]
    assigned_repos = {r.repo_id for r in g.backend.project_repositories.all(workspace_id)}

    repos_to_be_deleted = [rid for rid in repo_ids_all if rid not in assigned_repos]
    return repos_to_be_deleted


def delete_records(workspace_id, table_, cte):
    print("deletando")
    schema_name = f"ws_{workspace_id}"
    query = table_.table.delete().where(table_.table.c.commit_id == cte.c.commit_id)
    table_.engine.execution_options(schema_translate_map={None: schema_name}).execute(query)
