from datetime import datetime, timedelta
from typing import Optional, List

from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes.cli_v2 import CleanupType
from gitential2.utils import is_list_not_empty

logger = get_logger(__name__)


def perform_data_cleanup(
    g: GitentialContext,
    workspace_ids: Optional[List[int]] = None,
    cleanup_type: Optional[CleanupType] = CleanupType.full,
):
    wid_list = workspace_ids if is_list_not_empty(workspace_ids) else [w.id for w in g.backend.workspaces.all()]
    logger.info("Attempting to perform data cleanup process...", workspace_id_list=wid_list)
    for wid in wid_list:
        __perform_data_cleanup_on_workspace(g=g, workspace_id=wid, cleanup_type=cleanup_type)


def __perform_data_cleanup_on_workspace(
    g: GitentialContext, workspace_id: int, cleanup_type: Optional[CleanupType] = CleanupType.full
):
    logger.info("Starting data cleanup for workspace.", workspace_id=workspace_id)

    date_from: Optional[datetime] = __get_date_from(g.settings.extraction.repo_analysis_limit_in_days)
    repo_ids_to_delete = __get_repo_ids_to_delete(g=g, workspace_id=workspace_id)

    if cleanup_type in (CleanupType.full, CleanupType.commits):
        __remove_redundant_commit_data(
            g=g, workspace_id=workspace_id, repo_ids_to_delete=repo_ids_to_delete, date_from=date_from
        )
    if cleanup_type in (CleanupType.full, CleanupType.pull_requests):
        __remove_redundant_pull_request_data(
            g=g, workspace_id=workspace_id, repo_ids_to_delete=repo_ids_to_delete, date_from=date_from
        )
    if cleanup_type in (CleanupType.full, CleanupType.its_projects):
        __remove_redundant_data_for_its_projects(g=g, workspace_id=workspace_id)
    if cleanup_type in (CleanupType.full, CleanupType.redis):
        # TODO
        pass


def __remove_redundant_commit_data(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], date_from: Optional[datetime]
):
    logger.info(
        "Attempting to remove redundant data for commits...",
        workspace_id=workspace_id,
        repo_ids_to_delete=repo_ids_to_delete,
        date_from=date_from,
    )

    commits_to_delete = g.backend.extracted_commits.select_extracted_commits(
        workspace_id=workspace_id, date_from=date_from, repo_ids=repo_ids_to_delete
    )
    deleted_commit_hashes = [c.commit_id for c in commits_to_delete]
    logger.info("commit_ids_to_be_deleted", commit_ids_to_be_deleted=deleted_commit_hashes)

    number_of_deleted_extracted_commits = g.backend.extracted_commits.delete_commits(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    logger.info("extracted_commits deleted.", number_of_deleted_extracted_commits=number_of_deleted_extracted_commits)

    number_of_deleted_calculated_commits: int = g.backend.calculated_commits.delete_commits(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    logger.info(
        "calculated_commits deleted.", number_of_deleted_calculated_commits=number_of_deleted_calculated_commits
    )

    number_of_deleted_extracted_patches: int = g.backend.extracted_patches.delete_extracted_patches(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    logger.info("extracted_patches deleted.", number_of_deleted_extracted_patches=number_of_deleted_extracted_patches)

    number_of_deleted_calculated_patches: int = g.backend.calculated_patches.delete_calculated_patches(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    logger.info(
        "calculated_patches deleted.", number_of_deleted_calculated_patches=number_of_deleted_calculated_patches
    )

    number_of_deleted_extracted_patch_rewrites: int = (
        g.backend.extracted_patch_rewrites.delete_extracted_patch_rewrites(
            workspace_id=workspace_id, commit_ids=deleted_commit_hashes
        )
    )
    logger.info(
        "extracted_patch_rewrites deleted.",
        number_of_deleted_extracted_patch_rewrites=number_of_deleted_extracted_patch_rewrites,
    )

    number_of_deleted_extracted_commit_branches: int = (
        g.backend.extracted_commit_branches.delete_extracted_commit_branches(
            workspace_id=workspace_id, commit_ids=deleted_commit_hashes
        )
    )
    logger.info(
        "extracted_commit_branches deleted.",
        number_of_deleted_extracted_commit_branches=number_of_deleted_extracted_commit_branches,
    )


def __remove_redundant_pull_request_data(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], date_from: Optional[datetime]
):
    logger.info(
        "Attempting to remove redundant data for pull requests...",
        workspace_id=workspace_id,
        repo_ids_to_delete=repo_ids_to_delete,
        date_from=date_from,
    )

    prs_to_be_deleted = g.backend.pull_requests.select_pull_requests(
        workspace_id=workspace_id, date_from=date_from, repo_ids=repo_ids_to_delete
    )
    logger.info("pull_requests to be deleted.", number_of_pull_requests_to_be_deleted=len(prs_to_be_deleted))

    deleted_pr_numbers: List[int] = [pr.number for pr in prs_to_be_deleted]
    number_of_prs_deleted = g.backend.pull_requests.delete_pull_requests(
        workspace_id=workspace_id, pr_numbers=deleted_pr_numbers
    )
    logger.info("pull_requests deleted", number_of_prs_deleted=number_of_prs_deleted)

    number_of_deleted_pull_request_commits: int = g.backend.pull_request_commits.delete_pull_request_commits(
        workspace_id=workspace_id, pull_request_numbers=deleted_pr_numbers
    )
    logger.info(
        "pull_request_commits deleted.", number_of_deleted_pull_request_commits=number_of_deleted_pull_request_commits
    )

    number_of_deleted_pull_request_comments: int = g.backend.pull_request_comments.delete_pull_request_comment(
        workspace_id=workspace_id, pull_request_numbers=deleted_pr_numbers
    )
    logger.info(
        "pull_request_comments deleted.",
        number_of_deleted_pull_request_comments=number_of_deleted_pull_request_comments,
    )

    number_of_deleted_pull_request_labels: int = g.backend.pull_request_labels.delete_pull_request_labels(
        workspace_id=workspace_id, pull_request_numbers=deleted_pr_numbers
    )
    logger.info(
        "pull_request_labels deleted.", number_of_deleted_pull_request_labels=number_of_deleted_pull_request_labels
    )


def __remove_redundant_data_for_its_projects(g: GitentialContext, workspace_id: int):
    date_from: Optional[datetime] = __get_date_from(g.settings.extraction.its_project_analysis_limit_in_days)
    itsp_ids_to_be_deleted: List[int] = __get_itsp_ids_to_be_deleted(g=g, workspace_id=workspace_id)

    logger.info(
        "Remove redundant data for repositories...",
        workspace_id=workspace_id,
        its_issues_to_be_deleted=itsp_ids_to_be_deleted,
        date_from=date_from,
    )

    deleted_its_issues = g.backend.its_issues.delete_its_issues(
        workspace_id=workspace_id, date_from=date_from, its_issue_ids=itsp_ids_to_be_deleted
    )
    logger.info("its_issues deleted", number_of_deleted_its_issues=len(deleted_its_issues))

    deleted_its_issue_ids: List[str] = [its.id for its in deleted_its_issues]

    number_of_deleted_its_issue_changes: int = g.backend.its_issue_changes.delete_its_issue_changes(
        workspace_id=workspace_id, its_ids=deleted_its_issue_ids
    )
    logger.info("its_issue_changes deleted.", number_of_deleted_its_issue_changes=number_of_deleted_its_issue_changes)

    number_of_deleted_its_issue_time_in_statuses: int = (
        g.backend.its_issue_times_in_statuses.delete_its_issue_time_in_statuses(
            workspace_id=workspace_id, its_ids=deleted_its_issue_ids
        )
    )
    logger.info(
        "its_issue_times_in_statuses deleted.",
        number_of_deleted_its_issue_time_in_statuses=number_of_deleted_its_issue_time_in_statuses,
    )

    number_of_deleted_its_issue_comments: int = g.backend.its_issue_comments.delete_its_issue_comments(
        workspace_id=workspace_id, its_ids=deleted_its_issue_ids
    )
    logger.info(
        "its_issue_comments deleted.", number_of_deleted_its_issue_comments=number_of_deleted_its_issue_comments
    )

    number_of_deleted_its_issue_linked_issues: int = g.backend.its_issue_linked_issues.delete_its_issue_linked_issues(
        workspace_id=workspace_id, its_ids=deleted_its_issue_ids
    )
    logger.info(
        "its_issue_linked_issues deleted.",
        number_of_deleted_its_issue_linked_issues=number_of_deleted_its_issue_linked_issues,
    )

    number_of_deleted_its_sprints: int = g.backend.its_sprints.delete_its_sprints(
        workspace_id=workspace_id, its_ids=deleted_its_issue_ids
    )
    logger.info("its_sprints deleted.", number_of_deleted_its_sprints=number_of_deleted_its_sprints)

    number_of_deleted_its_issue_sprints: int = g.backend.its_issue_sprints.delete_its_issue_sprints(
        workspace_id=workspace_id, its_ids=deleted_its_issue_ids
    )
    logger.info("its_issue_sprints deleted.", number_of_deleted_its_issue_sprints=number_of_deleted_its_issue_sprints)

    number_of_deleted_its_issue_worklogs: int = g.backend.its_issue_worklogs.delete_its_issue_worklogs(
        workspace_id=workspace_id, its_ids=deleted_its_issue_ids
    )
    logger.info(
        "its_issue_worklogs deleted.", number_of_deleted_its_issue_worklogs=number_of_deleted_its_issue_worklogs
    )


def __get_date_from(number_of_days_diff: Optional[int] = None) -> Optional[datetime]:
    return (
        datetime.utcnow() - timedelta(days=number_of_days_diff)
        if number_of_days_diff and number_of_days_diff > 0
        else None
    )


def __get_repo_ids_to_delete(g: GitentialContext, workspace_id: int) -> List[int]:
    repo_ids_all: List[int] = [r.id for r in g.backend.repositories.all(workspace_id=workspace_id)]
    repo_ids_in_extracted_commits: List[int] = g.backend.extracted_commits.get_list_of_repo_ids_distinct(
        workspace_id=workspace_id
    )
    return [rid for rid in repo_ids_in_extracted_commits if rid not in repo_ids_all]


def __get_itsp_ids_to_be_deleted(g: GitentialContext, workspace_id: int) -> List[int]:
    itsp_ids_all: List[int] = [itsp.id for itsp in g.backend.its_projects.all(workspace_id=workspace_id)]
    itsp_ids_in_its_issues: List[int] = g.backend.its_issues.get_list_of_itsp_ids_distinct(workspace_id=workspace_id)
    return [itsp_id for itsp_id in itsp_ids_in_its_issues if itsp_id not in itsp_ids_all]
