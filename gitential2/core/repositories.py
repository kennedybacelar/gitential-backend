from typing import List
from gitential2.integrations import REPOSITORY_SOURCES
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryInDB, GitProtocol
from gitential2.utils import levenshtein
from .context import GitentialContext
from .credentials import list_credentials_for_workspace


def list_available_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryCreate]:
    def _fixme(*args, **kwargs):
        print("update token called", args, kwargs)

    results: List[RepositoryCreate] = []
    for credential in list_credentials_for_workspace(g, workspace_id):

        if credential.integration_type in REPOSITORY_SOURCES and credential.integration_name in g.integrations:

            integration = g.integrations[credential.integration_name]
            token = credential.to_token_dict(fernet=g.fernet)
            results += integration.list_available_private_repositories(token=token, update_token=_fixme)

    results += list_ssh_repositories(g, workspace_id)
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

    def _fixme(*args, **kwargs):
        print("update token called", args, kwargs)

    for credential in list_credentials_for_workspace(g, workspace_id):

        if credential.integration_type in REPOSITORY_SOURCES and credential.integration_name in g.integrations:

            integration = g.integrations[credential.integration_name]
            token = credential.to_token_dict(fernet=g.fernet)
            results += integration.search_public_repositories(query=search, token=token, update_token=_fixme)

    return sorted(results, key=lambda i: levenshtein(search, i.name))


def create_repositories(
    g: GitentialContext, workspace_id: int, repository_creates: List[RepositoryCreate]
) -> List[RepositoryInDB]:
    return [
        g.backend.repositories.create_or_update(workspace_id, repository_create)
        for repository_create in repository_creates
    ]


def delete_repositories(g: GitentialContext, workspace_id: int, repository_ids: List[int]):
    for project in g.backend.projects.all(workspace_id):
        g.backend.project_repositories.remove_repo_ids_from_project(workspace_id, project.id, repository_ids)

    for repo_id in repository_ids:
        g.backend.repositories.delete(workspace_id, repo_id)

    return True
