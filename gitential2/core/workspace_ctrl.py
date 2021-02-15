from typing import List, Optional

from gitential2.integrations import REPOSITORY_SOURCES
from gitential2.backends import GitentialBackend
from gitential2.datatypes import (
    CredentialInDB,
    WorkspaceInDB,
    ProjectInDB,
    ProjectCreateWithRepositories,
    ProjectCreate,
    ProjectUpdate,
    RepositoryCreate,
)
from gitential2.datatypes.projects import ProjectUpdateWithRepositories
from gitential2.datatypes.repositories import RepositoryInDB

from .tasks import refresh_repository
from .abc import WorkspaceCtrl, Gitential


class WorkspaceCtrlImpl(WorkspaceCtrl):
    def __init__(self, id_: int, backend: GitentialBackend, core: Gitential):
        self._ws: Optional[WorkspaceInDB] = None
        self._id = id_
        self.backend = backend
        self.core = core

    def initialize(self):
        self.backend.initialize_workspace(self._id)

    @property
    def workspace(self) -> WorkspaceInDB:
        if self._ws is None:
            self._ws = self.backend.workspaces.get(id_=self._id)
            if self._ws is None:
                raise ValueError(f"Missing workspace: {self._id} ")
        return self._ws

    def get_credentials(self) -> List[CredentialInDB]:
        return self.backend.credentials.get_for_user(self.workspace.created_by)

    def list_projects(self) -> List[ProjectInDB]:
        return list(self.backend.projects.all(self._id))

    def create_project(self, project_create: ProjectCreateWithRepositories) -> ProjectInDB:
        project = self.backend.projects.create(self._id, ProjectCreate(**project_create.dict(exclude={"repos"})))
        self._update_project_repos(project=project, repos=project_create.repos)
        self.schedule_project_refresh(project_id=project.id)
        return project

    def get_project(self, project_id: int) -> ProjectInDB:
        return self.backend.projects.get_or_error(workspace_id=self._id, id_=project_id)

    def delete_project(self, project_id: int) -> bool:
        return False

    def update_project(self, project_id: int, project_update: ProjectUpdateWithRepositories) -> ProjectInDB:
        project = self.backend.projects.update(
            workspace_id=self._id, id_=project_id, obj=ProjectUpdate(**project_update.dict(exclude={"repos"}))
        )
        self._update_project_repos(project=project, repos=project_update.repos)
        self.schedule_project_refresh(project_id)
        return project

    def _update_project_repos(self, project: ProjectInDB, repos=List[RepositoryCreate]):
        repositories = [self.backend.repositories.create_or_update(workspace_id=self._id, obj=r) for r in repos]
        return self.backend.project_repositories.update_project_repositories(
            workspace_id=self._id, project_id=project.id, repo_ids=[r.id for r in repositories]
        )

    def schedule_project_refresh(self, project_id: int):
        for repo_id in self.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=self._id, project_id=project_id
        ):
            self.schedule_repository_refresh(repo_id=repo_id)

    def schedule_repository_refresh(self, repo_id: int):
        refresh_repository.delay(self.core.settings.dict(), self._id, repo_id)

    def refresh_repository(self, repository_id: int):
        pass

    def list_connected_repository_sources(self) -> List[str]:
        return [
            credential.integration_name
            for credential in self.get_credentials()
            if (
                credential.integration_name
                and credential.integration_type in REPOSITORY_SOURCES
                and credential.integration_name in self.core.integrations
            )
        ]

    def list_available_repositories(self) -> List[RepositoryCreate]:
        def _fixme(*args, **kwargs):
            print("update token called", args, kwargs)

        results: List[RepositoryCreate] = []
        for credential in self.get_credentials():

            if (
                credential.integration_type in REPOSITORY_SOURCES
                and credential.integration_name in self.core.integrations
            ):

                integration = self.core.integrations[credential.integration_name]
                token = credential.to_token_dict(fernet=self.core.fernet)
                results += integration.list_available_private_repositories(token=token, update_token=_fixme)

        return results

    def list_project_repositories(self, project_id: int) -> List[RepositoryInDB]:
        ret = []
        for repo_id in self.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=self._id, project_id=project_id
        ):
            repository = self.backend.repositories.get(workspace_id=self._id, id_=repo_id)
            if repository:
                ret.append(repository)
        return ret
