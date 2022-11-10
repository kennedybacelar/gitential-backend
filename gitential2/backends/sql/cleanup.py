from datetime import datetime, timedelta
from functools import partial
from typing import Optional, List, Iterable, Set, Callable, Any

from pydantic import BaseModel
from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes import ProjectRepositoryInDB
from gitential2.datatypes.cli_v2 import CleanupType
from gitential2.datatypes.extraction import ExtractedCommit
from gitential2.datatypes.project_its_projects import ProjectITSProjectInDB
from gitential2.datatypes.pull_requests import PullRequest, PullRequestId
from gitential2.utils import is_list_not_empty

logger = get_logger(__name__)

DELETE_ID_CHUNK_SIZE: int = 1000


class DeleteRowsResult(BaseModel):
    no_deleted_rows: int
    no_rows_before_clean: int
    no_rows_after_clean: int


class DeleteSettings(BaseModel):
    items_title: str
    delete_fn: Callable
    count_rows_fn: Callable
    delete_ids_key: str
    item_ids_to_delete: List[Any]


class ITSCleanupState(BaseModel):
    itsp_ids_to_delete: List[int]
    its_issue_ids_to_be_deleted: List[str]


def perform_data_cleanup(
    g: GitentialContext,
    workspace_ids: Optional[List[int]] = None,
    cleanup_type: Optional[CleanupType] = CleanupType.full,
):
    wid_list = workspace_ids if is_list_not_empty(workspace_ids) else [w.id for w in g.backend.workspaces.all()]
    logger.info("Attempting to perform data cleanup process...", workspace_id_list=wid_list)
    for wid in wid_list:
        try:
            cleanup_types = __perform_data_cleanup_on_workspace(g=g, workspace_id=wid, cleanup_type=cleanup_type)
            logger.info("Data cleanup finished for workspace.", workspace_id=wid, cleanup_types=cleanup_types)
        except Exception as e:  # pylint: disable=broad-except
            logger.exception("Cleanup of workspace failed.", workspace_id=wid, exception=e)


def __perform_data_cleanup_on_workspace(
    g: GitentialContext, workspace_id: int, cleanup_type: Optional[CleanupType] = CleanupType.full
) -> List[CleanupType]:
    date_to: Optional[datetime] = __get_date_to(g.settings.extraction.repo_analysis_limit_in_days)
    repo_ids_to_delete = __get_repo_ids_to_delete(g=g, workspace_id=workspace_id)
    itsp_ids_to_delete = __get_itsp_ids_to_be_deleted(g=g, workspace_id=workspace_id)

    logger.info(
        "Starting data cleanup for workspace.",
        workspace_id=workspace_id,
        repo_analysis_limit_in_days=g.settings.extraction.repo_analysis_limit_in_days,
        its_project_analysis_limit_in_days=g.settings.extraction.its_project_analysis_limit_in_days,
        date_to=date_to,
        repo_ids_to_delete=repo_ids_to_delete,
        itsp_ids_to_delete=itsp_ids_to_delete,
    )

    cleanup_types: List[CleanupType] = []
    if cleanup_type in (CleanupType.full, CleanupType.commits):
        res_c = __remove_redundant_commit_data(
            g=g, wid=workspace_id, repo_ids_to_delete=repo_ids_to_delete, date_to=date_to
        )
        cleanup_types.append(res_c)
    if cleanup_type in (CleanupType.full, CleanupType.pull_requests):
        res_pr = __remove_redundant_pull_request_data(
            g=g, wid=workspace_id, repo_ids_to_delete=repo_ids_to_delete, date_to=date_to
        )
        cleanup_types.append(res_pr)
    if cleanup_type in (CleanupType.full, CleanupType.its_projects):
        res_its = __remove_redundant_data_for_its_projects(g=g, wid=workspace_id, itsp_ids_to_delete=itsp_ids_to_delete)
        cleanup_types.append(res_its)
    if cleanup_type in (CleanupType.full, CleanupType.redis):
        res_redis = __remove_redundant_data_for_redis(
            g=g, workspace_id=workspace_id, repo_ids_to_delete=repo_ids_to_delete, itsp_ids_to_delete=itsp_ids_to_delete
        )
        if res_redis:
            cleanup_types.append(res_redis)

    return cleanup_types


def __remove_redundant_commit_data(
    g: GitentialContext, wid: int, repo_ids_to_delete: List[int], date_to: Optional[datetime] = None
) -> CleanupType:
    logger.info(
        "Attempting to remove redundant data for commits...",
        workspace_id=wid,
        repo_ids_to_delete=repo_ids_to_delete,
        date_to=date_to,
    )

    redis_key: str = __get_redis_key_for_cleanup(wid=wid, c_type=CleanupType.commits)
    cleanup_state: List[str] = g.kvstore.get_value(redis_key) or []  # type: ignore

    commits_to_delete: List[ExtractedCommit] = g.backend.extracted_commits.select_extracted_commits(
        workspace_id=wid, date_to=date_to, repo_ids=repo_ids_to_delete
    )

    commit_hashes_to_be_deleted: List[str] = list(set([c.commit_id for c in commits_to_delete] + cleanup_state))
    logger.info(number_of_commit_hashes_selected_for_cleanup=len(commit_hashes_to_be_deleted))

    if not is_list_not_empty(commit_hashes_to_be_deleted):
        logger.info("Nothing to delete from commits.", workspace_id=wid)
        return CleanupType.commits

    g.kvstore.set_value(redis_key, commit_hashes_to_be_deleted)

    items_key: str = "commit_ids"

    delete_settings: List[DeleteSettings] = [
        DeleteSettings(
            items_title="extracted_commits",
            delete_fn=g.backend.extracted_commits.delete_commits,
            count_rows_fn=g.backend.extracted_commits.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=commit_hashes_to_be_deleted,
        ),
        DeleteSettings(
            items_title="calculated_commits",
            delete_fn=g.backend.calculated_commits.delete_commits,
            count_rows_fn=g.backend.calculated_commits.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=commit_hashes_to_be_deleted,
        ),
        DeleteSettings(
            items_title="extracted_patches",
            delete_fn=g.backend.extracted_patches.delete_extracted_patches,
            count_rows_fn=g.backend.extracted_patches.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=commit_hashes_to_be_deleted,
        ),
        DeleteSettings(
            items_title="calculated_patches",
            delete_fn=g.backend.calculated_patches.delete_calculated_patches,
            count_rows_fn=g.backend.calculated_patches.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=commit_hashes_to_be_deleted,
        ),
        DeleteSettings(
            items_title="extracted_patch_rewrites",
            delete_fn=g.backend.extracted_patch_rewrites.delete_extracted_patch_rewrites,
            count_rows_fn=g.backend.extracted_patch_rewrites.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=commit_hashes_to_be_deleted,
        ),
        DeleteSettings(
            items_title="extracted_commit_branches",
            delete_fn=g.backend.extracted_commit_branches.delete_extracted_commit_branches,
            count_rows_fn=g.backend.extracted_commit_branches.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=commit_hashes_to_be_deleted,
        ),
    ]

    cleanup_result = __apply_delete_settings_list(wid=wid, delete_settings=delete_settings, c_type=CleanupType.commits)

    g.kvstore.delete_value(redis_key)

    return cleanup_result


def __remove_redundant_pull_request_data(
    g: GitentialContext, wid: int, repo_ids_to_delete: List[int], date_to: Optional[datetime]
) -> CleanupType:
    logger.info(
        "Attempting to remove redundant data for pull requests...",
        workspace_id=wid,
        repo_ids_to_delete=repo_ids_to_delete,
        date_to=date_to,
    )

    redis_key: str = __get_redis_key_for_cleanup(wid=wid, c_type=CleanupType.pull_requests)
    cleanup_state: List[PullRequestId] = g.kvstore.get_value(redis_key) or []  # type: ignore

    prs_to_be_deleted: List[PullRequest] = g.backend.pull_requests.select_pull_requests(
        workspace_id=wid, date_to=date_to, repo_ids=repo_ids_to_delete
    )
    pr_ids_to_be_deleted: List[PullRequestId] = [pr.id_ for pr in prs_to_be_deleted] + cleanup_state
    logger.info("Pull requests selected for cleanup.", number_of_pull_requests_to_be_deleted=len(pr_ids_to_be_deleted))

    if not is_list_not_empty(pr_ids_to_be_deleted):
        logger.info("Nothing to delete from pull requests.", workspace_id=wid)
        return CleanupType.commits

    g.kvstore.set_value(redis_key, pr_ids_to_be_deleted)

    items_key: str = "pr_ids"

    delete_settings: List[DeleteSettings] = [
        DeleteSettings(
            items_title="pull_requests",
            delete_fn=g.backend.pull_requests.delete_pull_requests,
            count_rows_fn=g.backend.pull_requests.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=pr_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="pull_request_commits",
            delete_fn=g.backend.pull_request_commits.delete_pull_request_commits,
            count_rows_fn=g.backend.pull_request_commits.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=pr_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="pull_request_comments",
            delete_fn=g.backend.pull_request_comments.delete_pull_request_comment,
            count_rows_fn=g.backend.pull_request_comments.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=pr_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="pull_request_labels",
            delete_fn=g.backend.pull_request_labels.delete_pull_request_labels,
            count_rows_fn=g.backend.pull_request_labels.count_rows,
            delete_ids_key=items_key,
            item_ids_to_delete=pr_ids_to_be_deleted,
        ),
    ]

    cleanup_result = __apply_delete_settings_list(
        wid=wid, delete_settings=delete_settings, c_type=CleanupType.pull_requests
    )

    g.kvstore.delete_value(redis_key)

    return cleanup_result


def __remove_redundant_data_for_its_projects(
    g: GitentialContext, wid: int, itsp_ids_to_delete: List[int]
) -> CleanupType:
    date_to: Optional[datetime] = __get_date_to(g.settings.extraction.its_project_analysis_limit_in_days)

    logger.info(
        "Attempting to remove redundant data for ITS projects...",
        workspace_id=wid,
        its_issues_to_be_deleted=itsp_ids_to_delete,
        date_to=date_to,
    )

    redis_key: str = __get_redis_key_for_cleanup(wid=wid, c_type=CleanupType.its_projects)
    cleanup_state: ITSCleanupState = g.kvstore.get_value(redis_key) or ITSCleanupState(  # type: ignore
        itsp_ids_to_delete=[], its_issue_ids_to_be_deleted=[]
    )

    its_issues_to_delete = g.backend.its_issues.select_its_issues(
        workspace_id=wid, date_to=date_to, itsp_ids=itsp_ids_to_delete
    )
    its_issue_ids_to_be_deleted: List[str] = [
        its.id for its in its_issues_to_delete
    ] + cleanup_state.its_issue_ids_to_be_deleted
    itsp_ids_to_delete_corrected: List[int] = itsp_ids_to_delete + cleanup_state.itsp_ids_to_delete
    logger.info("ITS Issues selected for cleanup.", no_its_issue_ids_to_be_deleted=len(its_issue_ids_to_be_deleted))

    if not is_list_not_empty(its_issue_ids_to_be_deleted) and not is_list_not_empty(itsp_ids_to_delete_corrected):
        logger.info("Nothing to delete from its projects.", workspace_id=wid)
        return CleanupType.its_projects

    its_cleanup_state = ITSCleanupState(
        itsp_ids_to_delete=itsp_ids_to_delete_corrected, its_issue_ids_to_be_deleted=its_issue_ids_to_be_deleted
    )
    g.kvstore.set_value(redis_key, its_cleanup_state.dict())

    delete_settings: List[DeleteSettings] = [
        DeleteSettings(
            items_title="its_issues",
            delete_fn=g.backend.its_issues.delete_its_issues,
            count_rows_fn=g.backend.its_issues.count_rows,
            delete_ids_key="its_issue_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="its_issue_changes",
            delete_fn=g.backend.its_issue_changes.delete_its_issue_changes,
            count_rows_fn=g.backend.its_issue_changes.count_rows,
            delete_ids_key="its_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="its_issue_times_in_statuses",
            delete_fn=g.backend.its_issue_times_in_statuses.delete_its_issue_time_in_statuses,
            count_rows_fn=g.backend.its_issue_times_in_statuses.count_rows,
            delete_ids_key="its_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="its_issue_comments",
            delete_fn=g.backend.its_issue_comments.delete_its_issue_comments,
            count_rows_fn=g.backend.its_issue_comments.count_rows,
            delete_ids_key="its_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="its_issue_linked_issues",
            delete_fn=g.backend.its_issue_linked_issues.delete_its_issue_linked_issues,
            count_rows_fn=g.backend.its_issue_linked_issues.count_rows,
            delete_ids_key="its_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="its_sprints",
            delete_fn=g.backend.its_sprints.delete_its_sprints,
            count_rows_fn=g.backend.its_sprints.count_rows,
            delete_ids_key="itsp_ids",
            item_ids_to_delete=itsp_ids_to_delete_corrected,
        ),
        DeleteSettings(
            items_title="its_issue_sprints",
            delete_fn=g.backend.its_issue_sprints.delete_its_issue_sprints,
            count_rows_fn=g.backend.its_issue_sprints.count_rows,
            delete_ids_key="its_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
        DeleteSettings(
            items_title="its_issue_worklogs",
            delete_fn=g.backend.its_issue_worklogs.delete_its_issue_worklogs,
            count_rows_fn=g.backend.its_issue_worklogs.count_rows,
            delete_ids_key="its_ids",
            item_ids_to_delete=its_issue_ids_to_be_deleted,
        ),
    ]

    cleanup_result = __apply_delete_settings_list(
        wid=wid, delete_settings=delete_settings, c_type=CleanupType.its_projects
    )

    g.kvstore.delete_value(redis_key)

    return cleanup_result


def __remove_redundant_data_for_redis(
    g: GitentialContext, workspace_id: int, repo_ids_to_delete: List[int], itsp_ids_to_delete: List[int]
) -> Optional[CleanupType]:
    is_rids = is_list_not_empty(repo_ids_to_delete)
    is_itsp_ids = is_list_not_empty(itsp_ids_to_delete)

    result = None

    if is_rids or is_itsp_ids:
        logger.info("Attempting to clean redis data.", workspace_id=workspace_id)
        keys = []

        if is_rids:
            for rid in repo_ids_to_delete:
                redis_key_1 = f"ws-{workspace_id}:repository-refresh-{rid}"
                g.kvstore.delete_value(name=redis_key_1)
                redis_key_2 = f"ws-{workspace_id}:r-{rid}:extraction"
                g.kvstore.delete_value(name=redis_key_2)
                redis_key_3 = f"ws-{workspace_id}:repository-status-{rid}"
                g.kvstore.delete_value(name=redis_key_3)
                keys.append(redis_key_1)
                keys.append(redis_key_2)
                keys.append(redis_key_3)

        if is_itsp_ids:
            for itsp_id in itsp_ids_to_delete:
                redis_key = f"ws-{workspace_id}:itsp-{itsp_id}"
                g.kvstore.delete_value(name=redis_key)
                keys.append(redis_key)

        logger.info("Keys deleted from redis.", keys=keys)

        result = CleanupType.redis
    else:
        logger.info("Can not perform redis cleanup. Both repo_ids_to_delete and itsp_ids_to_delete were empty.")

    return result


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

    project_repos: Iterable[ProjectRepositoryInDB] = g.backend.project_repositories.all(workspace_id=workspace_id)
    rids: Set[int] = {item.repo_id for item in project_repos}

    return [rid for rid in repo_ids_in_extracted_commits if rid not in repo_ids_all or rid not in rids]


def __get_itsp_ids_to_be_deleted(g: GitentialContext, workspace_id: int) -> List[int]:
    itsp_ids_all: List[int] = [itsp.id for itsp in g.backend.its_projects.all(workspace_id=workspace_id)]
    itsp_ids_in_its_issues: List[int] = g.backend.its_issues.get_list_of_itsp_ids_distinct(workspace_id=workspace_id)

    project_its_projects: Iterable[ProjectITSProjectInDB] = g.backend.project_its_projects.all(
        workspace_id=workspace_id
    )
    itsp_ids: Set[int] = {item.itsp_id for item in project_its_projects}

    return [itsp_id for itsp_id in itsp_ids_in_its_issues if itsp_id not in itsp_ids_all or itsp_id not in itsp_ids]


def __delete_rows(
    delete_rows_partial_fn,
    check_no_rows_partial_fn,
    items_key: str,
    item_ids_to_delete: List[Any],
    table_name: str,
    wid: int,
) -> DeleteRowsResult:
    number_of_rows_before_clean: int = check_no_rows_partial_fn()

    def __log_delete_attempt(chuck_size: int):
        logger.info(
            f"Attempting to delete rows from {table_name} table.",
            workspace_id=wid,
            number_of_rows_to_be_deleted=chuck_size,
        )

    number_of_deleted_rows: int = 0
    if is_list_not_empty(item_ids_to_delete):
        if len(item_ids_to_delete) <= DELETE_ID_CHUNK_SIZE:
            __log_delete_attempt(chuck_size=len(item_ids_to_delete))
            number_of_deleted_rows = delete_rows_partial_fn(**{items_key: item_ids_to_delete})
        else:
            delete_chunks = [
                item_ids_to_delete[x : x + DELETE_ID_CHUNK_SIZE]
                for x in range(0, len(item_ids_to_delete), DELETE_ID_CHUNK_SIZE)
            ]
            for chunk in delete_chunks:
                __log_delete_attempt(chuck_size=len(chunk))
                delete_result: int = delete_rows_partial_fn(**{items_key: chunk})
                logger.info(
                    "Rows deleted from table.",
                    table_name=table_name,
                    workspace_id=wid,
                    number_of_deleted_rows=delete_result,
                )
                number_of_deleted_rows += delete_result

    number_of_rows_after_clean: int = check_no_rows_partial_fn()
    return DeleteRowsResult(
        no_deleted_rows=number_of_deleted_rows,
        no_rows_before_clean=number_of_rows_before_clean,
        no_rows_after_clean=number_of_rows_after_clean,
    )


def __log_delete_results(delete_result: DeleteRowsResult, items_title: str):
    logger.info(
        f"Cleanup of '{items_title}' finished.",
        number_of_deleted_rows=delete_result.no_deleted_rows,
        number_of_rows_before_clean=delete_result.no_rows_before_clean,
        number_of_rows_after_clean=delete_result.no_rows_after_clean,
    )


def __apply_delete_settings_list(wid: int, delete_settings: List[DeleteSettings], c_type: CleanupType) -> CleanupType:
    for ds in delete_settings:
        logger.info(f"Cleanup of {ds.items_title} started.")
        delete_result: DeleteRowsResult = __delete_rows(
            delete_rows_partial_fn=partial(ds.delete_fn, workspace_id=wid),
            check_no_rows_partial_fn=partial(ds.count_rows_fn, workspace_id=wid),
            items_key=ds.delete_ids_key,
            item_ids_to_delete=ds.item_ids_to_delete,
            table_name=ds.items_title,
            wid=wid,
        )
        __log_delete_results(delete_result=delete_result, items_title=ds.items_title)

    return c_type


def __get_redis_key_for_cleanup(wid: int, c_type: CleanupType) -> str:
    return f"cleanup_started_for_workspace_{wid}__cleanup_type:{c_type}"
