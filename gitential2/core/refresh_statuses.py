from typing import cast

from gitential2.datatypes.refresh_statuses import RepositoryRefreshStatus
from .context import GitentialContext


def _repo_refresh_status_key(workspace_id: int, repository_id: int) -> str:
    return f"ws-{workspace_id}:repository-refresh-{repository_id}"


def get_repo_refresh_status(g: GitentialContext, workspace_id: int, repository_id: int) -> RepositoryRefreshStatus:
    current_dict = cast(dict, g.kvstore.get_value(_repo_refresh_status_key(workspace_id, repository_id)))

    def _get_repository_name():
        repository = g.backend.repositories.get_or_error(workspace_id, repository_id)
        return repository.name

    if current_dict:
        if "workspace_id" not in current_dict:
            current_dict["workspace_id"] = workspace_id
        if "repository_id" not in current_dict:
            current_dict["repository_id"] = repository_id
        if "repository_name" not in current_dict:
            current_dict["repository_name"] = _get_repository_name()
        return RepositoryRefreshStatus(**cast(dict, current_dict))
    else:
        return RepositoryRefreshStatus(
            workspace_id=workspace_id, repository_id=repository_id, repository_name=_get_repository_name()
        )


def persist_repo_refresh_status(
    g: GitentialContext, workspace_id: int, repository_id: int, status: RepositoryRefreshStatus
) -> RepositoryRefreshStatus:
    status_dict = status.dict()
    g.kvstore.set_value(_repo_refresh_status_key(workspace_id, repository_id), status_dict)
    return status


def update_repo_refresh_status(g: GitentialContext, workspace_id: int, repository_id: int, **kwargs):
    current_status = get_repo_refresh_status(g, workspace_id, repository_id)
    current_status_dict = current_status.dict()
    for k, v in kwargs.items():
        current_status_dict[k] = v
    new_status = RepositoryRefreshStatus(**current_status_dict)
    return persist_repo_refresh_status(g, workspace_id, repository_id, new_status)
