from datetime import date, datetime, timedelta
from typing import Optional, List
from enum import Enum

from sqlalchemy import select

from gitential2.core import GitentialContext
from gitential2.datatypes.cli_v2 import CleanupType


class Commits(str, Enum):
    calculated_commits = "calculated_commits"
    extracted_commits = "extracted_commits"


def perform_data_cleanup_(
    g: GitentialContext,
    remove_residual_data: bool = False,
    workspace_ids: Optional[List[int]] = None,
    cleanup_type: Optional[CleanupType] = CleanupType.full,
):

    date_to = __get_date_to(g.settings.extraction.repo_analysis_limit_in_days)
    if date_to:
        __remove_redundant_commit_data(g, date_to)


def get_commit_ids_table(g: GitentialContext, date_to: datetime):

    # Creating common table expression and returning the commit_ids
    table_ = g.backend.calculated_commits
    return select([table_.table.c.commit_id]).where(table_.table.c.date <= date_to).cte()


def __remove_redundant_commit_data(g: GitentialContext, date_to: datetime):
    cte = get_commit_ids_table(g, date_to)  # common table expression
    print(cte)


def __get_date_to(number_of_days_diff: Optional[int] = None) -> Optional[datetime]:
    return (
        datetime.utcnow() - timedelta(days=number_of_days_diff)
        if number_of_days_diff and number_of_days_diff > 0
        else None
    )

def delete_records(table_):
    