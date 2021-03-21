import traceback
from typing import Optional
from functools import partial
from structlog import get_logger
from gitential2.datatypes.credentials import CredentialInDB
from gitential2.datatypes.repositories import GitRepositoryState, RepositoryInDB, RepositoryStatusStatus

from gitential2.extraction.repository import extract_incremental_local, clone_repository
from gitential2.utils.tempdir import TemporaryDirectory

from .context import GitentialContext
from .credentials import get_credential_for_repository
from .statuses import get_repository_status, persist_repository_status, delete_repository_status
from .calculations import recalculate_repository_values


logger = get_logger(__name__)


def refresh_repository(g: GitentialContext, workspace_id: int, repository_id: int, force_rebuild: bool):
    if force_rebuild:
        delete_repository_status(g, workspace_id, repository_id)
        print("force-rebuild should remove from database too")
    repo_status = get_repository_status(g, workspace_id, repository_id)

    if repo_status.done or repo_status.status == RepositoryStatusStatus.pending or force_rebuild:
        # Start a new refresh
        persist_repository_status(g, workspace_id, repository_id, repo_status.reset())

        try:
            refresh_repository_commits(g, workspace_id, repository_id)
            refresh_repository_pull_requests(g, workspace_id, repository_id)

            repo_status = get_repository_status(g, workspace_id, repository_id)
            persist_repository_status(g, workspace_id, repository_id, repo_status.persist_started())
            recalculate_repository_values(g, workspace_id, repository_id)

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
    credential = get_credential_for_repository(g, workspace_id, repository)
    _extract_commits_patches(g, workspace_id, repository, credential)


def refresh_repository_pull_requests(g: GitentialContext, workspace_id: int, repository_id: int):
    repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
    credential = get_credential_for_repository(g, workspace_id, repository)
    prs_we_already_have = g.backend.pull_requests.get_prs_updated_at(workspace_id, repository_id)
    # print(prs_we_already_have)
    if not credential:
        # log ...
        return

    integration = g.integrations.get(repository.integration_name)
    if not integration:
        # log...
        return
    output = g.backend.output_handler(workspace_id)
    if hasattr(integration, "collect_pull_requests"):
        token = credential.to_token_dict(g.fernet)

        def _update_token(*args, **kwargs):
            print("****** UPDATE TOKEN *****")
            print(args, kwargs)
            print("****** UPDATE TOKEN *****")

        integration.collect_pull_requests(
            repository=repository,
            token=token,
            update_token=_update_token,
            output=output,
            prs_we_already_have=prs_we_already_have,
        )
    else:
        # log ...
        pass


def _extract_commits_patches(
    g: GitentialContext,
    workspace_id: int,
    repository: RepositoryInDB,
    credential: Optional[CredentialInDB] = None,
    force_restart=False,
):
    _persist_status = partial(persist_repository_status, g, workspace_id, repository.id)

    output = g.backend.output_handler(workspace_id)
    previous_state = get_previous_extraction_state(g, workspace_id, repository.id) if not force_restart else None
    repo_status = get_repository_status(g, workspace_id, repository.id)

    repo_status = _persist_status(repo_status.cloning_started())

    with TemporaryDirectory() as workdir:
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
