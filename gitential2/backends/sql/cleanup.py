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
    logger.info(
        "Starting data cleanup for workspace.",
        workspace_id=workspace_id,
        repo_analysis_limit_in_days=g.settings.extraction.repo_analysis_limit_in_days,
        its_project_analysis_limit_in_days=g.settings.extraction.its_project_analysis_limit_in_days,
    )

    date_to: Optional[datetime] = __get_date_to(g.settings.extraction.repo_analysis_limit_in_days)
    repo_ids_to_delete = __get_repo_ids_to_delete(g=g, workspace_id=workspace_id)
    itsp_ids_to_delete = __get_itsp_ids_to_be_deleted(g=g, workspace_id=workspace_id)

    if cleanup_type in (CleanupType.full, CleanupType.commits):
        __remove_redundant_commit_data(
            g=g, workspace_id=workspace_id, repo_ids_to_delete=repo_ids_to_delete, date_to=date_to
        )
    if cleanup_type in (CleanupType.full, CleanupType.pull_requests):
        __remove_redundant_pull_request_data(
            g=g, workspace_id=workspace_id, repo_ids_to_delete=repo_ids_to_delete, date_to=date_to
        )
    if cleanup_type in (CleanupType.full, CleanupType.its_projects):
        __remove_redundant_data_for_its_projects(g=g, workspace_id=workspace_id, itsp_ids_to_delete=itsp_ids_to_delete)
    if cleanup_type in (CleanupType.full, CleanupType.redis):
        __remove_redundant_data_for_redis(
            g=g, workspace_id=workspace_id, repo_ids_to_delete=repo_ids_to_delete, itsp_ids_to_delete=itsp_ids_to_delete
        )


def __remove_redundant_commit_data(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], date_to: Optional[datetime]
):
    logger.info(
        "Attempting to remove redundant data for commits...",
        workspace_id=workspace_id,
        repo_ids_to_delete=repo_ids_to_delete,
        date_to=date_to,
    )

    commits_to_delete = g.backend.extracted_commits.select_extracted_commits(
        workspace_id=workspace_id, date_to=date_to, repo_ids=repo_ids_to_delete
    )
    commit_hashes_to_be_deleted = [c.commit_id for c in commits_to_delete]
    logger.info(
        "Commits selected for cleanup.",
        number_of_commits_to_be_deleted=len(commit_hashes_to_be_deleted),
    )

    no_extracted_commits_before_clean: int = g.backend.extracted_commits.count_rows(workspace_id=workspace_id)
    number_of_deleted_extracted_commits: int = g.backend.extracted_commits.delete_commits(
        workspace_id=workspace_id, commit_ids=commit_hashes_to_be_deleted
    )
    no_extracted_commits_after_clean: int = g.backend.extracted_commits.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of extracted_commits finished.",
        number_of_deleted_extracted_commits=number_of_deleted_extracted_commits,
        no_extracted_commits_before_clean=no_extracted_commits_before_clean,
        no_extracted_commits_after_clean=no_extracted_commits_after_clean,
    )

    no_calculated_commits_before_clean: int = g.backend.calculated_commits.count_rows(workspace_id=workspace_id)
    number_of_deleted_calculated_commits: int = g.backend.calculated_commits.delete_commits(
        workspace_id=workspace_id, commit_ids=commit_hashes_to_be_deleted
    )
    no_calculated_commits_after_clean: int = g.backend.calculated_commits.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of calculated_commits finished.",
        number_of_deleted_calculated_commits=number_of_deleted_calculated_commits,
        no_calculated_commits_before_clean=no_calculated_commits_before_clean,
        no_calculated_commits_after_clean=no_calculated_commits_after_clean,
    )

    no_extracted_patches_before_clean: int = g.backend.extracted_patches.count_rows(workspace_id=workspace_id)
    number_of_deleted_extracted_patches: int = g.backend.extracted_patches.delete_extracted_patches(
        workspace_id=workspace_id, commit_ids=commit_hashes_to_be_deleted
    )
    no_extracted_patches_after_clean: int = g.backend.extracted_patches.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of extracted_patches finished.",
        number_of_deleted_extracted_patches=number_of_deleted_extracted_patches,
        no_extracted_patches_before_clean=no_extracted_patches_before_clean,
        no_extracted_patches_after_clean=no_extracted_patches_after_clean,
    )

    no_calculated_patches_before_clean: int = g.backend.calculated_patches.count_rows(workspace_id=workspace_id)
    number_of_deleted_calculated_patches: int = g.backend.calculated_patches.delete_calculated_patches(
        workspace_id=workspace_id, commit_ids=commit_hashes_to_be_deleted
    )
    no_calculated_patches_after_clean: int = g.backend.calculated_patches.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of calculated_patches finished.",
        number_of_deleted_calculated_patches=number_of_deleted_calculated_patches,
        no_calculated_patches_before_clean=no_calculated_patches_before_clean,
        no_calculated_patches_after_clean=no_calculated_patches_after_clean,
    )

    no_extracted_patch_rewrites_before_clean: int = g.backend.extracted_patch_rewrites.count_rows(
        workspace_id=workspace_id
    )
    number_of_deleted_extracted_patch_rewrites: int = (
        g.backend.extracted_patch_rewrites.delete_extracted_patch_rewrites(
            workspace_id=workspace_id, commit_ids=commit_hashes_to_be_deleted
        )
    )
    no_extracted_patch_rewrites_after_clean: int = g.backend.extracted_patch_rewrites.count_rows(
        workspace_id=workspace_id
    )
    logger.info(
        "Cleanup of extracted_patch_rewrites finished.",
        number_of_deleted_extracted_patch_rewrites=number_of_deleted_extracted_patch_rewrites,
        no_extracted_patch_rewrites_before_clean=no_extracted_patch_rewrites_before_clean,
        no_extracted_patch_rewrites_after_clean=no_extracted_patch_rewrites_after_clean,
    )

    no_extracted_commit_branches_before_clean: int = g.backend.extracted_commit_branches.count_rows(
        workspace_id=workspace_id
    )
    number_of_deleted_extracted_commit_branches: int = (
        g.backend.extracted_commit_branches.delete_extracted_commit_branches(
            workspace_id=workspace_id, commit_ids=commit_hashes_to_be_deleted
        )
    )
    no_extracted_commit_branches_after_clean: int = g.backend.extracted_commit_branches.count_rows(
        workspace_id=workspace_id
    )
    logger.info(
        "extracted_commit_branches deleted.",
        number_of_deleted_extracted_commit_branches=number_of_deleted_extracted_commit_branches,
        no_extracted_commit_branches_before_clean=no_extracted_commit_branches_before_clean,
        no_extracted_commit_branches_after_clean=no_extracted_commit_branches_after_clean,
    )


def __remove_redundant_pull_request_data(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], date_to: Optional[datetime]
):
    logger.info(
        "Attempting to remove redundant data for pull requests...",
        workspace_id=workspace_id,
        repo_ids_to_delete=repo_ids_to_delete,
        date_to=date_to,
    )

    prs_to_be_deleted = g.backend.pull_requests.select_pull_requests(
        workspace_id=workspace_id, date_to=date_to, repo_ids=repo_ids_to_delete
    )
    pr_numbers_to_be_deleted: List[int] = [pr.number for pr in prs_to_be_deleted]
    logger.info("Pull requests selected for cleanup.", number_of_pull_requests_to_be_deleted=len(prs_to_be_deleted))

    no_pull_requests_before_clean = g.backend.pull_requests.count_rows(workspace_id=workspace_id)
    number_of_prs_deleted: int = g.backend.pull_requests.delete_pull_requests(
        workspace_id=workspace_id, pr_numbers=pr_numbers_to_be_deleted
    )
    no_pull_requests_after_clean = g.backend.pull_requests.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of pull_requests finished.",
        number_of_prs_deleted=number_of_prs_deleted,
        no_pull_requests_before_clean=no_pull_requests_before_clean,
        no_pull_requests_after_clean=no_pull_requests_after_clean,
    )

    no_pull_request_commits_before_clean = g.backend.pull_request_commits.count_rows(workspace_id=workspace_id)
    number_of_deleted_pull_request_commits: int = g.backend.pull_request_commits.delete_pull_request_commits(
        workspace_id=workspace_id, pull_request_numbers=pr_numbers_to_be_deleted
    )
    no_pull_request_commits_after_clean = g.backend.pull_request_commits.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of pull_request_commits finished.",
        number_of_deleted_pull_request_commits=number_of_deleted_pull_request_commits,
        no_pull_request_commits_before_clean=no_pull_request_commits_before_clean,
        no_pull_request_commits_after_clean=no_pull_request_commits_after_clean,
    )

    no_pull_request_comments_before_clean = g.backend.pull_request_comments.count_rows(workspace_id=workspace_id)
    number_of_deleted_pull_request_comments: int = g.backend.pull_request_comments.delete_pull_request_comment(
        workspace_id=workspace_id, pull_request_numbers=pr_numbers_to_be_deleted
    )
    no_pull_request_comments_after_clean = g.backend.pull_request_comments.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of pull_request_comments finished.",
        number_of_deleted_pull_request_comments=number_of_deleted_pull_request_comments,
        no_pull_request_comments_before_clean=no_pull_request_comments_before_clean,
        no_pull_request_comments_after_clean=no_pull_request_comments_after_clean,
    )

    no_pull_request_labels_before_clean = g.backend.pull_request_labels.count_rows(workspace_id=workspace_id)
    number_of_deleted_pull_request_labels: int = g.backend.pull_request_labels.delete_pull_request_labels(
        workspace_id=workspace_id, pull_request_numbers=pr_numbers_to_be_deleted
    )
    no_pull_request_labels_after_clean = g.backend.pull_request_labels.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of pull_request_labels finished.",
        number_of_deleted_pull_request_labels=number_of_deleted_pull_request_labels,
        no_pull_request_labels_before_clean=no_pull_request_labels_before_clean,
        no_pull_request_labels_after_clean=no_pull_request_labels_after_clean,
    )


def __remove_redundant_data_for_its_projects(g: GitentialContext, workspace_id: int, itsp_ids_to_delete: List[int]):
    date_to: Optional[datetime] = __get_date_to(g.settings.extraction.its_project_analysis_limit_in_days)

    logger.info(
        "Attempting to remove redundant data for repositories...",
        workspace_id=workspace_id,
        its_issues_to_be_deleted=itsp_ids_to_delete,
        date_to=date_to,
    )

    its_issues_to_delete = g.backend.its_issues.select_its_issues(
        workspace_id=workspace_id, date_to=date_to, itsp_ids=itsp_ids_to_delete
    )
    its_issue_ids_to_be_deleted: List[str] = [its.id for its in its_issues_to_delete]
    logger.info("ITS Issues selected for cleanup.", number_of_deleted_its_issues=len(its_issues_to_delete))

    no_its_issue_changes_before_clean: int = g.backend.its_issue_changes.count_rows(workspace_id=workspace_id)
    number_of_deleted_its_issue_changes: int = g.backend.its_issue_changes.delete_its_issue_changes(
        workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
    )
    no_its_issue_changes_after_clean: int = g.backend.its_issue_changes.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of its_issue_changes finished.",
        number_of_deleted_its_issue_changes=number_of_deleted_its_issue_changes,
        no_its_issue_changes_before_clean=no_its_issue_changes_before_clean,
        no_its_issue_changes_after_clean=no_its_issue_changes_after_clean,
    )

    no_its_issue_times_in_statuses_before_clean: int = g.backend.its_issue_times_in_statuses.count_rows(
        workspace_id=workspace_id
    )
    number_of_deleted_its_issue_time_in_statuses: int = (
        g.backend.its_issue_times_in_statuses.delete_its_issue_time_in_statuses(
            workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
        )
    )
    no_its_issue_times_in_statuses_after_clean: int = g.backend.its_issue_times_in_statuses.count_rows(
        workspace_id=workspace_id
    )
    logger.info(
        "Cleanup of its_issue_times_in_statuses finished.",
        number_of_deleted_its_issue_time_in_statuses=number_of_deleted_its_issue_time_in_statuses,
        no_its_issue_times_in_statuses_before_clean=no_its_issue_times_in_statuses_before_clean,
        no_its_issue_times_in_statuses_after_clean=no_its_issue_times_in_statuses_after_clean,
    )

    no_its_issue_comments_before_clean: int = g.backend.its_issue_comments.count_rows(workspace_id=workspace_id)
    number_of_deleted_its_issue_comments: int = g.backend.its_issue_comments.delete_its_issue_comments(
        workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
    )
    no_its_issue_comments_after_clean: int = g.backend.its_issue_comments.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of its_issue_comments finished.",
        number_of_deleted_its_issue_comments=number_of_deleted_its_issue_comments,
        no_its_issue_comments_before_clean=no_its_issue_comments_before_clean,
        no_its_issue_comments_after_clean=no_its_issue_comments_after_clean,
    )

    no_its_issue_linked_issues_before_clean: int = g.backend.its_issue_linked_issues.count_rows(
        workspace_id=workspace_id
    )
    number_of_deleted_its_issue_linked_issues: int = g.backend.its_issue_linked_issues.delete_its_issue_linked_issues(
        workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
    )
    no_its_issue_linked_issues_after_clean: int = g.backend.its_issue_linked_issues.count_rows(
        workspace_id=workspace_id
    )
    logger.info(
        "Cleanup of its_issue_linked_issues finished.",
        number_of_deleted_its_issue_linked_issues=number_of_deleted_its_issue_linked_issues,
        no_its_issue_linked_issues_before_clean=no_its_issue_linked_issues_before_clean,
        no_its_issue_linked_issues_after_clean=no_its_issue_linked_issues_after_clean,
    )

    no_its_sprints_before_clean: int = g.backend.its_sprints.count_rows(workspace_id=workspace_id)
    number_of_deleted_its_sprints: int = g.backend.its_sprints.delete_its_sprints(
        workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
    )
    no_its_sprints_after_clean: int = g.backend.its_sprints.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of its_sprints finished.",
        number_of_deleted_its_sprints=number_of_deleted_its_sprints,
        no_its_sprints_before_clean=no_its_sprints_before_clean,
        no_its_sprints_after_clean=no_its_sprints_after_clean,
    )

    no_its_issue_sprints_before_clean: int = g.backend.its_issue_sprints.count_rows(workspace_id=workspace_id)
    number_of_deleted_its_issue_sprints: int = g.backend.its_issue_sprints.delete_its_issue_sprints(
        workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
    )
    no_its_issue_sprints_after_clean: int = g.backend.its_issue_sprints.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of its_issue_sprints finished.",
        number_of_deleted_its_issue_sprints=number_of_deleted_its_issue_sprints,
        no_its_issue_sprints_before_clean=no_its_issue_sprints_before_clean,
        no_its_issue_sprints_after_clean=no_its_issue_sprints_after_clean,
    )

    no_its_issue_worklogs_before_clean: int = g.backend.its_issue_worklogs.count_rows(workspace_id=workspace_id)
    number_of_deleted_its_issue_worklogs: int = g.backend.its_issue_worklogs.delete_its_issue_worklogs(
        workspace_id=workspace_id, its_ids=its_issue_ids_to_be_deleted
    )
    no_its_issue_worklogs_after_clean: int = g.backend.its_issue_worklogs.count_rows(workspace_id=workspace_id)
    logger.info(
        "Cleanup of its_issue_worklogs finished.",
        number_of_deleted_its_issue_worklogs=number_of_deleted_its_issue_worklogs,
        no_its_issue_worklogs_before_clean=no_its_issue_worklogs_before_clean,
        no_its_issue_worklogs_after_clean=no_its_issue_worklogs_after_clean,
    )


def __remove_redundant_data_for_redis(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], itsp_ids_to_delete: List[int]
):
    logger.info("Attempting to clean redis data.", workspace_id=workspace_id)

    if is_list_not_empty(repo_ids_to_delete):
        for rid in repo_ids_to_delete:
            redis_key_1 = f"ws-{workspace_id}:repository-refresh-{rid}"
            g.kvstore.delete_value(name=redis_key_1)
            redis_key_2 = f"ws-{workspace_id}:r-{rid}:extraction"
            g.kvstore.delete_value(name=redis_key_2)
            redis_key_3 = f"ws-{workspace_id}:repository-status-{rid}"
            g.kvstore.delete_value(name=redis_key_3)

            logger.info("Keys deleted from redis.", keys=[redis_key_1, redis_key_2, redis_key_3])

    if is_list_not_empty(itsp_ids_to_delete):
        for itsp_id in itsp_ids_to_delete:
            redis_key = f"ws-{workspace_id}:itsp-{itsp_id}"
            g.kvstore.delete_value(name=redis_key)
            logger.info("Keys deleted from redis.", keys=[redis_key])


def __get_date_to(number_of_days_diff: Optional[int] = None) -> Optional[datetime]:
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
