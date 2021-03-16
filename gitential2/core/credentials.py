from typing import List, Optional
from gitential2.datatypes.credentials import CredentialInDB, CredentialCreate, CredentialType
from gitential2.datatypes.repositories import RepositoryInDB

from gitential2.utils.ssh import create_ssh_keypair
from gitential2.integrations import REPOSITORY_SOURCES
from .context import GitentialContext
from ..exceptions import NotImplementedException, NotFoundException


def list_credentials_for_user(g: GitentialContext, user_id: int) -> List[CredentialInDB]:
    return g.backend.credentials.get_for_user(user_id)


def list_credentials_for_workspace(g: GitentialContext, workspace_id: int):
    workspace = g.backend.workspaces.get_or_error(workspace_id)
    return list_credentials_for_user(g, user_id=workspace.created_by)


def create_credential(g: GitentialContext, credential_create: CredentialCreate, owner_id: int) -> CredentialInDB:
    if credential_create.type == CredentialType.keypair:
        private_key, public_key = create_ssh_keypair()
        credential_create.private_key = g.fernet.encrypt_bytes(private_key)
        credential_create.public_key = g.fernet.encrypt_bytes(public_key)

        credential_create.owner_id = owner_id
        return g.backend.credentials.create(credential_create)
    else:
        raise NotImplementedException("Only ssh keypair credential creation supported.")


def delete_credential_from_workspace(g: GitentialContext, workspace_id: int, credential_id: int):
    credential = g.backend.credentials.get(credential_id)
    if credential:
        if credential.type == CredentialType.keypair:

            repo_ids_to_remove = [
                repository.id
                for repository in g.backend.repositories.all(workspace_id)
                if repository.credential_id == credential.id
            ]
            for project in g.backend.projects.all(workspace_id):
                g.backend.project_repositories.remove_repo_ids_from_project(
                    workspace_id, project.id, repo_ids_to_remove
                )
            for repo_id in repo_ids_to_remove:
                g.backend.repositories.delete(workspace_id, repo_id)

            return g.backend.credentials.delete(credential_id)
        else:
            raise NotImplementedException("Only ssh keypair credential delete supported.")
    else:
        raise NotFoundException("Credential not found.")

    # remove repositories


def create_credential_for_workspace(
    g: GitentialContext, workspace_id: int, credential_create: CredentialCreate
) -> CredentialInDB:
    workspace = g.backend.workspaces.get_or_error(workspace_id)
    return create_credential(g, credential_create, owner_id=workspace.created_by)


def get_credential_for_repository(
    g: GitentialContext, workspace_id: int, repository: RepositoryInDB
) -> Optional[CredentialInDB]:
    if repository.credential_id:
        return g.backend.credentials.get(repository.credential_id)
    if repository.integration_name:
        workspace = g.backend.workspaces.get_or_error(workspace_id)
        return g.backend.credentials.get_by_user_and_integration(
            owner_id=workspace.created_by, integration_name=repository.integration_name
        )
    return None


def list_connected_repository_sources(g: GitentialContext, workspace_id: int) -> List[str]:
    return [
        credential.integration_name
        for credential in list_credentials_for_workspace(g, workspace_id)
        if (
            credential.integration_name
            and credential.integration_type in REPOSITORY_SOURCES
            and credential.integration_name in g.integrations
        )
    ]
