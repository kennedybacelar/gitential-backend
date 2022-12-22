from datetime import date, datetime, timedelta
from typing import Optional, List
from enum import Enum
from collections import namedtuple

from sqlalchemy import select, or_, and_

from gitential2.core import GitentialContext
from gitential2.datatypes.cli_v2 import CleanupType

CommitTables = namedtuple("CommitTables", ["cid_column_name", "repo_id_column_name"])
PullRequestsTables = namedtuple("PullRequestsTables", ["prid_column_name", "repo_id_column_name"])
ITSProjectsTables = namedtuple("ITSProjectsTables", ["issue_id_column_name", "itsp_id_column_name"])


class CleaningGroup(str, Enum):
    commits = "commits"
    pull_requests = "pull_requests"
    its_projects = "its_projects"


all_tables_info = {
    CleaningGroup.commits: {
        "calculated_commits": CommitTables("commit_id", "repo_id"),
        "extracted_commits": CommitTables("commit_id", "repo_id"),
        "extracted_patches": CommitTables("commit_id", "repo_id"),
        "calculated_patches": CommitTables("commit_id", "repo_id"),
        "extracted_patch_rewrites": CommitTables("commit_id", "repo_id"),
        "extracted_commit_branches": CommitTables("commit_id", "repo_id"),
    },
    CleaningGroup.pull_requests: {
        "pull_requests": PullRequestsTables("number", "repo_id"),
        "pull_request_commits": PullRequestsTables("pr_number", "repo_id"),
        "pull_request_comments": PullRequestsTables("pr_number", "repo_id"),
        "pull_request_labels": PullRequestsTables("pr_number", "repo_id"),
    },
    CleaningGroup.its_projects: {
        "its_issues": ITSProjectsTables("id", "itsp_id"),
        "its_issue_changes": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_times_in_statuses": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_comments": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_linked_issues": ITSProjectsTables("issue_id", "itsp_id"),
        "its_sprints": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_sprints": ITSProjectsTables("issue_id", "itsp_id"),
        "its_issue_worklogs": ITSProjectsTables("issue_id", "itsp_id"),
    },
}


class Commits(str, Enum):
    calculated_commits = "calculated_commits"
    extracted_commits = "extracted_commits"
    extracted_patches = "extracted_patches"
    calculated_patches = "calculated_patches"
    extracted_patch_rewrites = "extracted_patch_rewrites"
    extracted_commit_branches = "extracted_commit_branches"


class PullRequests(str, Enum):
    pull_requests = "pull_requests"
    pull_request_commits = "pull_request_commits"
    pull_request_comments = "pull_request_comments"
    pull_request_labels = "pull_request_labels"


# Remember that The key for pull request is repo_id + pr_number


def perform_data_cleanup_(
    g: GitentialContext,
    workspace_ids: Optional[List[int]] = None,
    cleanup_type: Optional[CleanupType] = CleanupType.full,
):
    date_to = __get_date_to(g.settings.extraction.repo_analysis_limit_in_days)
    its_date_to = __get_date_to(g.settings.extraction.its_project_analysis_limit_in_days)
    for workspace_id in workspace_ids:
        repo_ids_to_delete = __get_repo_ids_to_delete(g, workspace_id)
        if cleanup_type in (CleanupType.full, CleanupType.commits):
            if date_to or repo_ids_to_delete:
                __remove_redundant_data(
                    g,
                    workspace_id,
                    date_to,
                    repo_ids_to_delete,
                    CleaningGroup("commits"),
                )
        if cleanup_type in (CleanupType.full, CleanupType.pull_requests):
            if date_to or repo_ids_to_delete:
                pass


def __get_keys_to_be_deleted(
    g: GitentialContext,
    date_to: datetime,
    repo_ids_to_delete: List[int],
    cleaning_group: CleaningGroup,
):
    # Creating common table expression and returning the commit_ids
    table_ = __get_reference_table(g, cleaning_group)
    if cleaning_group == CleaningGroup.commits:
        return (
            select([table_.table.c.commit_id, table_.table.c.repo_id])
            .where(or_(table_.table.c.date <= date_to, table_.table.c.repo_id.in_(repo_ids_to_delete)))
            .cte()
        )
    if cleaning_group == CleaningGroup.pull_requests:
        return (
            select([table_.table.c.number, table_.table.c.repo_id])
            .where(or_(table_.table.c.created_at <= date_to, table_.table.c.repo_id.in_(repo_ids_to_delete)))
            .cte()
        )
    if cleaning_group == CleaningGroup.its_projects:
        return (
            select([table_.table.c.id, table_.table.c.itsp_id])
            .where(or_(table_.table.c.created_at <= date_to, table_.table.c.itsp_id.in_(repo_ids_to_delete)))
            .cte()
        )


def __remove_redundant_data(
    g: GitentialContext,
    workspace_id: int,
    date_to: datetime,
    repo_ids_to_delete: List[int],
    cleaning_group: CleaningGroup,
):
    cte = __get_keys_to_be_deleted(g, date_to, repo_ids_to_delete, cleaning_group)  # common table expression
    for table_name, table_keypair in all_tables_info.get(cleaning_group).items():
        table_ = g.backend.__getattribute__(table_name)
        delete_records(workspace_id, table_, cte)


def __remove_redundant_pull_request_data(
    g: GitentialContext,
    workspace_id: int,
    date_to: datetime,
    repo_ids_to_delete: List[int],
):
    cte = __get_pull_request_keys_to_be_deleted(g, date_to, repo_ids_to_delete)  # common table expression
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
    query = table_.table.delete().where(
        and_(table_.table.c.commit_id == cte.c.commit_id, table_.table.c.repo_id == cte.c.repo_id)
    )
    table_.engine.execution_options(schema_translate_map={None: schema_name}).execute(query)


def __get_reference_table(g: GitentialContext, cleaning_group: str):

    reference_tables = {
        CleaningGroup.commits: g.backend.calculated_commits,
        CleaningGroup.pull_requests: g.backend.pull_requests,
        CleaningGroup.its_projects: g.backend.its_issues,
    }

    return reference_tables.get(cleaning_group)
