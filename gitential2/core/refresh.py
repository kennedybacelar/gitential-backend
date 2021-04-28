import traceback
from typing import Optional
from functools import partial
from structlog import get_logger
from gitential2.datatypes.repositories import GitRepositoryState, RepositoryInDB, RepositoryStatusStatus

from gitential2.extraction.repository import extract_incremental_local, clone_repository
from gitential2.utils.tempdir import TemporaryDirectory

from .context import GitentialContext
from .credentials import get_update_token_callback, acquire_credential
from .statuses import get_repository_status, persist_repository_status, delete_repository_status
from .calculations import recalculate_repository_values


logger = get_logger(__name__)


def refresh_repository(g: GitentialContext, workspace_id: int, repository_id: int, force_rebuild: bool):
    if force_rebuild:
        delete_repository_status(g, workspace_id, repository_id)
        logger.warning("force-rebuild should remove from database too")
    repo_status = get_repository_status(g, workspace_id, repository_id)

    if repo_status.status == RepositoryStatusStatus.pending or force_rebuild:
        # Start a new refresh
        persist_repository_status(g, workspace_id, repository_id, repo_status.reset())

        try:
            refresh_repository_commits(g, workspace_id, repository_id)
            recalculate_repository_values(g, workspace_id, repository_id)
            repo_status = get_repository_status(g, workspace_id, repository_id)
            persist_repository_status(g, workspace_id, repository_id, repo_status.persist_started())

            # Temporary disabled pull requests refresh for prod deploy
            # refresh_repository_pull_requests(g, workspace_id, repository_id)

            persist_repository_status(g, workspace_id, repository_id, repo_status.persist_finished())
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to refresh repository", workspace_id=workspace_id, repository_id=repository_id)
            repo_status = get_repository_status(g, workspace_id, repository_id)
            persist_repository_status(
                g, workspace_id, repository_id, repo_status.finished_with_error(traceback.format_exc(limit=1))
            )
    else:
        logger.info(
            "Repository refresh already in-progress. Skipping.", workspace_id=workspace_id, repository_id=repository_id
        )


def refresh_repository_commits(g: GitentialContext, workspace_id: int, repository_id: int):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    # credential = get_credential_for_repository(g, workspace_id, repository)

    _extract_commits_patches(g, workspace_id, repository)


def refresh_repository_pull_requests(g: GitentialContext, workspace_id: int, repository_id: int):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    prs_we_already_have = g.backend.pull_requests.get_prs_updated_at(workspace_id, repository_id)

    with acquire_credential(
        g,
        credential_id=repository.credential_id,
        workspace_id=workspace_id,
        integration_name=repository.integration_name,
    ) as credential:

        # print(prs_we_already_have)
        if not credential:
            logger.info("Skipping PR refresh: no credential", workspace_id=workspace_id, repository_id=repository_id)
            return

        integration = g.integrations.get(repository.integration_name)
        if not integration:
            logger.info("Skipping PR refresh: no integration", workspace_id=workspace_id, repository_id=repository_id)
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
            )
        else:
            logger.info(
                "Skipping PR refresh: collect_pull_requests not implemented",
                workspace_id=workspace_id,
                repository_id=repository_id,
                integration=repository.integration_name,
            )


def _extract_commits_patches(
    g: GitentialContext,
    workspace_id: int,
    repository: RepositoryInDB,
    force_restart=False,
):
    _persist_status = partial(persist_repository_status, g, workspace_id, repository.id)
    output = g.backend.output_handler(workspace_id)
    previous_state = get_previous_extraction_state(g, workspace_id, repository.id) if not force_restart else None
    repo_status = get_repository_status(g, workspace_id, repository.id)

    repo_status = _persist_status(repo_status.cloning_started())

    with TemporaryDirectory() as workdir:
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
        ) as credential:
            local_repo = clone_repository(
                repository,
                destination_path=workdir.path,
                credentials=credential.to_repository_credential(g.fernet) if credential else None,
            )
            repo_status = _persist_status(repo_status.cloning_finished())
        repo_status = _persist_status(repo_status.extract_started())
        extraction_state = extract_incremental_local(
            local_repo, output=output, settings=g.settings, previous_state=previous_state
        )
        repo_status = _persist_status(repo_status.extract_finished())

    set_extraction_state(g, workspace_id, repository.id, extraction_state)


def _extraction_state_key(workspace_id: int, repository_id: int) -> str:
    return f"ws-{workspace_id}:r-{repository_id}:extraction"


def get_previous_extraction_state(
    g: GitentialContext, workspace_id: int, repository_id: int
) -> Optional[GitRepositoryState]:
    previous_state = g.kvstore.get_value(_extraction_state_key(workspace_id, repository_id))
    return GitRepositoryState(**previous_state) if previous_state and isinstance(previous_state, dict) else None


def set_extraction_state(g: GitentialContext, workspace_id: int, repository_id: int, state: GitRepositoryState):
    g.kvstore.set_value(_extraction_state_key(workspace_id, repository_id), state.dict())
