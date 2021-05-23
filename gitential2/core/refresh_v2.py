import traceback
from datetime import timedelta
from functools import partial
from typing import Callable, Optional

from structlog import get_logger
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.datatypes.refresh_statuses import RefreshCommitsPhase
from gitential2.datatypes.repositories import GitRepositoryState, RepositoryInDB
from gitential2.datatypes.extraction import LocalGitRepository

from gitential2.utils.tempdir import TemporaryDirectory
from gitential2.extraction.repository import extract_incremental_local, clone_repository
from gitential2.exceptions import LockError

from .calculations import recalculate_repository_values
from .context import GitentialContext
from .tasks import schedule_task
from .credentials import acquire_credential, get_update_token_callback
from .refresh_statuses import get_repo_refresh_status, update_repo_refresh_status

logger = get_logger(__name__)


def refresh_workspace(
    g: GitentialContext,
    workspace_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
):
    projects = g.backend.projects.all(workspace_id)
    if strategy == RefreshStrategy.parallel:
        for p in projects:
            schedule_task(
                g,
                task_name="refresh_project",
                params={
                    "workspace_id": workspace_id,
                    "project_id": p.id,
                    "strategy": strategy,
                    "refresh_type": refresh_type,
                },
            )
    else:
        for p in projects:
            refresh_project(g, workspace_id=workspace_id, project_id=p.id, strategy=strategy, refresh_type=refresh_type)


def refresh_project(
    g: GitentialContext,
    workspace_id: int,
    project_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
):
    for repo_id in g.backend.project_repositories.get_repo_ids_for_project(workspace_id, project_id):
        commits_refresh_scheduled = refresh_type in [RefreshType.everything, RefreshType.commits_only]
        prs_refresh_scheduled = refresh_type in [RefreshType.everything, RefreshType.prs_only]
        update_repo_refresh_status(
            g,
            workspace_id,
            repo_id,
            commits_refresh_scheduled=commits_refresh_scheduled,
            prs_refresh_scheduled=prs_refresh_scheduled,
        )
        if strategy == RefreshStrategy.parallel:
            schedule_task(
                g,
                task_name="refresh_repository",
                params={
                    "workspace_id": workspace_id,
                    "repository_id": repo_id,
                    "strategy": strategy,
                    "refresh_type": refresh_type,
                },
            )
        else:
            refresh_repository(g, workspace_id, repo_id, strategy, refresh_type)


def refresh_repository(
    g: GitentialContext,
    workspace_id: int,
    repository_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    logger.info(
        "Repository refresh started",
        workspace_id=workspace_id,
        repository_id=repository_id,
        repository_name=repository.name,
        strategy=strategy,
        refresh_type=refresh_type,
    )
    # Delegating tasks
    if strategy == RefreshStrategy.parallel and refresh_type == RefreshType.everything:
        schedule_task(
            g,
            task_name="refresh_repository",
            params={
                "workspace_id": workspace_id,
                "repository_id": repository_id,
                "strategy": strategy,
                "refresh_type": RefreshType.commits_only,
            },
        )
        schedule_task(
            g,
            task_name="refresh_repository",
            params={
                "workspace_id": workspace_id,
                "repository_id": repository_id,
                "strategy": strategy,
                "refresh_type": RefreshType.prs_only,
            },
        )
        logger.info("Delegated tasks, finishing")
        return

    if refresh_type in [RefreshType.commits_only, RefreshType.everything]:
        refresh_repository_commits(g, workspace_id, repository_id)
    if refresh_type in [RefreshType.prs_only, RefreshType.everything]:
        refresh_repository_pull_requests(g, workspace_id, repository_id)
    if refresh_type == RefreshType.commit_calculations_only:
        recalculate_repository_values(g, workspace_id, repository_id)


def refresh_repository_commits(g: GitentialContext, workspace_id: int, repository_id: int):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    refresh_status = get_repo_refresh_status(g, workspace_id, repository_id)
    _update_state = partial(update_repo_refresh_status, g=g, workspace_id=workspace_id, repository_id=repository_id)
    if refresh_status.commits_in_progress:
        logger.info(
            "Skipping commits refresh, another job is already in progress",
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
        _update_state(
            commits_refresh_scheduled=False,
        )
        return
    if (
        refresh_status.commits_last_successful_run
        and g.current_time() - refresh_status.commits_last_successful_run < timedelta(minutes=30)
    ):
        logger.info(
            "Skipping commits refresh, last successful refresh was not at least 30 minute ago",
            workspace_id=workspace_id,
            repository_id=repository_id,
        )
        _update_state(
            commits_refresh_scheduled=False,
        )
        return

    with TemporaryDirectory() as workdir:
        try:
            local_repo = _refresh_repository_commits_clone_phase(g, workspace_id, repository, workdir, _update_state)
            _refresh_repository_commits_extract_phase(g, workspace_id, repository, local_repo, _update_state)
            _refresh_repository_commits_persist_phase(g, workspace_id, repository_id, _update_state)

            _update_state(
                commits_phase=RefreshCommitsPhase.done,
                commits_in_progress=False,
                commits_error=False,
                commits_error_msg="",
                commits_last_successful_run=g.current_time(),
                commits_last_run=g.current_time(),
            )
        except LockError:
            logger.warning("Failed to acquire lock, maybe rescheduling")
            raise
        except:  # pylint: disable=bare-except
            logger.exception(
                "Unexpected error with commits extraction.",
                workspace_id=workspace_id,
                repository_id=repository_id,
                repository_name=repository.name,
            )
            _update_state(
                commits_phase=RefreshCommitsPhase.done,
                commits_in_progress=False,
                commits_error=True,
                commits_error_msg=traceback.format_exc(limit=1),
                commits_last_run=g.current_time(),
            )


def _refresh_repository_commits_clone_phase(
    g: GitentialContext,
    workspace_id: int,
    repository: RepositoryInDB,
    workdir: TemporaryDirectory,
    _update_state: Callable,
):
    logger.info(
        "Cloning repository",
        workspace_id=workspace_id,
        repository_id=repository.id,
        repository_name=repository.name,
    )
    with acquire_credential(
        g,
        credential_id=repository.credential_id,
        workspace_id=workspace_id,
        integration_name=repository.integration_name,
        blocking_timeout_seconds=30,
    ) as credential:

        _update_state(
            commits_in_progress=True,
            commits_refresh_scheduled=False,
            commits_error=False,
            commits_started=g.current_time(),
            commits_phase=RefreshCommitsPhase.cloning,
        )

        local_repo = clone_repository(
            repository,
            destination_path=workdir.path,
            credentials=credential.to_repository_credential(g.fernet) if credential else None,
        )
        return local_repo


def _refresh_repository_commits_extract_phase(
    g: GitentialContext,
    workspace_id: int,
    repository: RepositoryInDB,
    local_repo: LocalGitRepository,
    _update_state: Callable,
):
    _update_state(
        commits_phase=RefreshCommitsPhase.extract,
    )

    commits_we_already_have = g.backend.get_commit_ids_for_repository(workspace_id, repository.id)
    previous_state = get_previous_extraction_state(g, workspace_id, repository.id)

    logger.info(
        "Extracting commits from",
        workspace_id=workspace_id,
        repository_id=repository.id,
        repository_name=repository.name,
        commits_we_already_have=len(commits_we_already_have),
    )
    output = g.backend.output_handler(workspace_id)

    extraction_state = extract_incremental_local(
        local_repo,
        output=output,
        settings=g.settings,
        previous_state=previous_state,
        commits_we_already_have=commits_we_already_have,
    )
    set_extraction_state(g, workspace_id, repository.id, extraction_state)


def _refresh_repository_commits_persist_phase(
    g: GitentialContext, workspace_id: int, repository_id: int, _update_state: Callable
):
    _update_state(
        commits_phase=RefreshCommitsPhase.persist,
    )
    recalculate_repository_values(g, workspace_id, repository_id)


def _extraction_state_key(workspace_id: int, repository_id: int) -> str:
    return f"ws-{workspace_id}:r-{repository_id}:extraction"


def get_previous_extraction_state(
    g: GitentialContext, workspace_id: int, repository_id: int
) -> Optional[GitRepositoryState]:
    previous_state = g.kvstore.get_value(_extraction_state_key(workspace_id, repository_id))
    return GitRepositoryState(**previous_state) if previous_state and isinstance(previous_state, dict) else None


def set_extraction_state(g: GitentialContext, workspace_id: int, repository_id: int, state: GitRepositoryState):
    g.kvstore.set_value(_extraction_state_key(workspace_id, repository_id), state.dict())


def refresh_repository_pull_requests(g: GitentialContext, workspace_id: int, repository_id: int):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    prs_we_already_have = g.backend.pull_requests.get_prs_updated_at(workspace_id, repository_id)
    _update_state = partial(update_repo_refresh_status, g=g, workspace_id=workspace_id, repository_id=repository_id)

    def _end_processing_no_error():
        current_time = g.current_time()
        _update_state(
            prs_last_successful_run=current_time,
            prs_last_run=current_time,
            prs_error=False,
            prs_in_progress=False,
            prs_refresh_scheduled=False,
            prs_error_msg="",
        )

    try:
        current_state = get_repo_refresh_status(g, workspace_id, repository_id)
        if current_state.prs_in_progress:
            logger.info(
                "Skipping PR refresh, another process in progress",
                workspace_id=workspace_id,
                repository_id=repository_id,
                repository_name=repository.name,
            )
            return
        with acquire_credential(
            g,
            credential_id=repository.credential_id,
            workspace_id=workspace_id,
            integration_name=repository.integration_name,
            blocking_timeout_seconds=30,
        ) as credential:
            _update_state(prs_in_progress=True, prs_refresh_scheduled=False, prs_started=g.current_time())

            if not credential:
                logger.info(
                    "Skipping PR refresh: no credential", workspace_id=workspace_id, repository_id=repository_id
                )
                _end_processing_no_error()
                return

            integration = g.integrations.get(repository.integration_name)
            if not integration:
                logger.info(
                    "Skipping PR refresh: no integration", workspace_id=workspace_id, repository_id=repository_id
                )
                _end_processing_no_error()
                return

            output = g.backend.output_handler(workspace_id)

            if hasattr(integration, "collect_pull_requests"):
                token = credential.to_token_dict(g.fernet)

                integration.collect_pull_requests(
                    repository=repository,
                    token=token,
                    update_token=get_update_token_callback(g, credential),
                    output=output,
                    prs_we_already_have=prs_we_already_have,
                    limit=200,
                )
                _end_processing_no_error()
            else:
                logger.info(
                    "Skipping PR refresh: collect_pull_requests not implemented",
                    workspace_id=workspace_id,
                    repository_id=repository_id,
                    integration=repository.integration_name,
                )
                _end_processing_no_error()
    except LockError:
        logger.warning("Failed to acquire lock, maybe rescheduling")
        raise
    except:  # pylint: disable=bare-except
        logger.exception(
            "Unexpected error with PR extraction.",
            workspace_id=workspace_id,
            repository_id=repository_id,
            repository_name=repository.name,
        )

        _update_state(
            prs_in_progress=False,
            prs_error=True,
            prs_error_msg=traceback.format_exc(limit=1),
            prs_last_run=g.current_time(),
        )
