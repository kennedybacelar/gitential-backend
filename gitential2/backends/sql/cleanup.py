from datetime import datetime, timedelta
from typing import Optional, List

from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.utils import is_list_not_empty

logger = get_logger(__name__)


def perform_data_cleanup(g: GitentialContext, workspace_ids: Optional[List[int]] = None):
    wid_list = workspace_ids if is_list_not_empty(workspace_ids) else [w.id for w in g.backend.workspaces.all()]
    logger.info("Attempting to perform data cleanup process...", workspace_id_list=wid_list)
    for wid in wid_list:
        __perform_data_cleanup_on_workspace(g=g, workspace_id=wid)


def __perform_data_cleanup_on_workspace(g: GitentialContext, workspace_id: int):
    logger.info("Starting data cleanup for workspace.", workspace_id=workspace_id)

    date_from_for_repo = __get_date_from(g.settings.extraction.repo_analysis_limit_in_days)
    date_from_for_its = __get_date_from(g.settings.extraction.its_project_analysis_limit_in_days)
    repo_ids_to_delete = __get_repo_ids_to_delete(g=g, workspace_id=workspace_id)

    __remove_redundant_data_for_repositories(
        g=g, workspace_id=workspace_id, date_from=date_from_for_repo, repo_ids_to_delete=repo_ids_to_delete
    )
    __remove_redundant_data_for_its_projects(workspace_id=workspace_id, date_from=date_from_for_its)


def __remove_redundant_data_for_repositories(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], date_from: Optional[datetime] = None
):
    deleted_extracted_commits = g.backend.extracted_commits.delete_commits(
        workspace_id=workspace_id, date_from=date_from, repo_ids=repo_ids_to_delete
    )
    deleted_commit_hashes: List[str] = [c.commit_id for c in deleted_extracted_commits]
    number_of_deleted_calculated_commits: int = g.backend.calculated_commits.delete_commits(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    number_of_deleted_extracted_patches: int = g.backend.extracted_patches.delete_extracted_patches(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    number_of_deleted_calculated_patches: int = g.backend.calculated_patches.delete_calculated_patches(
        workspace_id=workspace_id, commit_ids=deleted_commit_hashes
    )
    number_of_deleted_extracted_patch_rewrites: int = (
        g.backend.extracted_patch_rewrites.delete_extracted_patch_rewrites(
            workspace_id=workspace_id, commit_ids=deleted_commit_hashes
        )
    )
    number_of_deleted_extracted_commit_branches: int = (
        g.backend.extracted_commit_branches.delete_extracted_commit_branches(
            workspace_id=workspace_id, commit_ids=deleted_commit_hashes
        )
    )


def __remove_redundant_data_for_its_projects(workspace_id: int, date_from: Optional[datetime] = None):
    pass


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
