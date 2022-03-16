from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from structlog import get_logger
from gitential2.datatypes.credentials import CredentialInDB
from gitential2.integrations import REPOSITORY_SOURCES
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryInDB, GitProtocol
from gitential2.datatypes.userinfos import UserInfoInDB

from gitential2.utils import levenshtein, find_first
from .context import GitentialContext
from .credentials import (
    get_fresh_credential,
    list_credentials_for_workspace,
    get_update_token_callback,
)


logger = get_logger(__name__)


def get_repository(g: GitentialContext, workspace_id: int, repository_id: int) -> Optional[RepositoryInDB]:
    return g.backend.repositories.get(workspace_id, repository_id)


def list_available_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryCreate]:
    def _merge_repo_lists(first: List[RepositoryCreate], second: List[RepositoryCreate]):
        existing_clone_urls = [r.clone_url for r in first]
        new_repos = [r for r in second if r.clone_url not in existing_clone_urls]
        return first + new_repos

    all_already_used_repositories = [RepositoryCreate(**r.dict()) for r in list_repositories(g, workspace_id)]

    results: List[RepositoryCreate] = all_already_used_repositories

    available_repos_for_credential = partial(list_available_repositories_for_credential, g, workspace_id)
    with ThreadPoolExecutor() as executor:
        collected_results = executor.map(
            available_repos_for_credential, list_credentials_for_workspace(g, workspace_id)
        )

    for collected_repositories in collected_results:
        results = _merge_repo_lists(collected_repositories, results)

    results = _merge_repo_lists(list_ssh_repositories(g, workspace_id), results)
    return results


def list_available_repositories_for_credential(
    g: GitentialContext, workspace_id: int, credential: CredentialInDB
) -> List[RepositoryCreate]:
    results = []

    if credential.integration_type in REPOSITORY_SOURCES and credential.integration_name in g.integrations:
        try:
            credential_ = get_fresh_credential(g, credential_id=credential.id)
            if credential_:
                # with acquire_credential(g, credential_id=credential_.id) as credential:
                integration = g.integrations[credential_.integration_name]
                token = credential_.to_token_dict(fernet=g.fernet)
                userinfo: UserInfoInDB = (
                    find_first(
                        lambda ui: ui.integration_name
                        == credential.integration_name,  # pylint: disable=cell-var-from-loop
                        g.backend.user_infos.get_for_user(credential.owner_id),
                    )
                    if credential.owner_id
                    else None
                )
                collected_repositories = integration.list_available_private_repositories(
                    token=token,
                    update_token=get_update_token_callback(g, credential),
                    provider_user_id=userinfo.sub if userinfo else None,
                )
                results = collected_repositories
            else:
                logger.error(
                    "Cannot get fresh credential",
                    credential_id=credential.id,
                    owner_id=credential.owner_id,
                    integration_name=credential.integration_name,
                )
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                "Error during collecting repositories",
                integration_name=credential.integration_name,
                credential_id=credential.id,
                workspace_id=workspace_id,
            )
    return results


def list_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryInDB]:
    all_projects = g.backend.projects.all(workspace_id)
    project_ids = [project.id for project in all_projects]

    repos = {}

    # HACK: Needed for ssh repositories
    for repo in g.backend.repositories.all(workspace_id=workspace_id):
        if repo.protocol == GitProtocol.ssh:
            repos[repo.id] = repo

    for project_id in project_ids:
        for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=workspace_id, project_id=project_id
        ):
            if repo_id not in repos:
                repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repo_id)
                if repository:
                    repos[repo_id] = repository
    return list(repos.values())


def list_ssh_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryCreate]:
    all_repositories = list_repositories(g, workspace_id)
    return [
        RepositoryCreate(**repo.dict())
        for repo in all_repositories
        if repo.credential_id is not None and repo.protocol == GitProtocol.ssh
    ]


def list_project_repositories(g: GitentialContext, workspace_id: int, project_id: int) -> List[RepositoryInDB]:
    ret = []
    for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repo_id)
        if repository:
            ret.append(repository)
    return ret


def search_public_repositories(g: GitentialContext, workspace_id: int, search: str) -> List[RepositoryCreate]:
    results: List[RepositoryCreate] = []

    for credential_ in list_credentials_for_workspace(g, workspace_id):
        if credential_.integration_type in REPOSITORY_SOURCES and credential_.integration_name in g.integrations:
            userinfo: UserInfoInDB = find_first(
                lambda ui: ui.integration_name == credential_.integration_name,  # pylint: disable=cell-var-from-loop
                g.backend.user_infos.get_for_user(credential_.owner_id),
            )
            try:
                # with acquire_credential(g, credential_id=credential_.id) as credential:
                credential = get_fresh_credential(g, credential_id=credential_.id)
                if credential:
                    integration = g.integrations[credential.integration_name]
                    token = credential.to_token_dict(fernet=g.fernet)
                    results += integration.search_public_repositories(
                        query=search,
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
                    )
                else:
                    logger.error(
                        "Cannot get fresh credential",
                        credential_id=credential_.id,
                        owner_id=credential_.owner_id,
                        integration_name=credential_.integration_name,
                    )
            except:  # pylint: disable=bare-except
                logger.exception("Error during public repo search")

    return sorted(results, key=lambda i: levenshtein(search, i.name))


def create_repositories(
    g: GitentialContext, workspace_id: int, repository_creates: List[RepositoryCreate]
) -> List[RepositoryInDB]:
    return [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id, repository_create)
        for repository_create in repository_creates
    ]


def delete_repositories(g: GitentialContext, workspace_id: int, repository_ids: List[int]):
    for project in g.backend.projects.all(workspace_id):
        g.backend.project_repositories.remove_repo_ids_from_project(workspace_id, project.id, repository_ids)

    for repo_id in repository_ids:
        g.backend.repositories.delete(workspace_id, repo_id)

    return True
