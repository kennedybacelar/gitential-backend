from typing import List, Optional, cast
import pandas as pd
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
    StatsRequest,
    AuthorAlias,
    CredentialType,
    CredentialCreate,
)
from gitential2.utils import levenshtein
from gitential2.datatypes.projects import ProjectUpdateWithRepositories, ProjectStatus
from gitential2.datatypes.repositories import GitProtocol, RepositoryInDB, RepositoryStatus
from gitential2.extraction.repository import extract_incremental
from gitential2.core.stats import calculate_stats

from gitential2.utils.ssh import create_ssh_keypair

from .tasks import refresh_repository
from .abc import WorkspaceCtrl
from .context import GitentialContext


class WorkspaceCtrlImpl(WorkspaceCtrl):
    def __init__(self, id_: int, backend: GitentialBackend, g: GitentialContext):
        self._ws: Optional[WorkspaceInDB] = None
        self._id = id_
        self.backend = backend
        self.g = g

    def initialize(self):
        self.backend.initialize_workspace(self._id)

    @property
    def workspace(self) -> WorkspaceInDB:
        if self._ws is None:
            self._ws = self.backend.workspaces.get(id_=self._id)
            if self._ws is None:
                raise ValueError(f"Missing workspace: {self._id} ")
        return self._ws

    # def get_credentials(self) -> List[CredentialInDB]:
    #     return self.backend.credentials.get_for_user(self.workspace.created_by)

    # def create_credential(self, credential_create: CredentialCreate) -> CredentialInDB:
    #     if credential_create.type == CredentialType.keypair:
    #         private_key, public_key = create_ssh_keypair()
    #         credential_create.private_key = self.g.fernet.encrypt_string(private_key.decode()).encode()
    #         credential_create.public_key = self.g.fernet.encrypt_string(public_key.decode()).encode()

    #     credential_create.owner_id = self.workspace.created_by
    #     return self.g.backend.credentials.create(credential_create)

    # def list_projects(self) -> List[ProjectInDB]:
    #     return list(self.backend.projects.all(self._id))

    # def create_project(self, project_create: ProjectCreateWithRepositories) -> ProjectInDB:
    #     project = self.backend.projects.create(self._id, ProjectCreate(**project_create.dict(exclude={"repos"})))
    #     self._update_project_repos(project=project, repos=project_create.repos)
    #     self.schedule_project_refresh(project_id=project.id)
    #     return project

    # def get_project(self, project_id: int) -> ProjectInDB:
    #     return self.backend.projects.get_or_error(workspace_id=self._id, id_=project_id)

    # def delete_project(self, project_id: int) -> bool:
    #     return False

    # def update_project(self, project_id: int, project_update: ProjectUpdateWithRepositories) -> ProjectInDB:
    #     project = self.backend.projects.update(
    #         workspace_id=self._id, id_=project_id, obj=ProjectUpdate(**project_update.dict(exclude={"repos"}))
    #     )
    #     self._update_project_repos(project=project, repos=project_update.repos)
    #     self.schedule_project_refresh(project_id)
    #     return project

    # def _update_project_repos(self, project: ProjectInDB, repos=List[RepositoryCreate]):
    #     repositories = [self.backend.repositories.create_or_update(workspace_id=self._id, obj=r) for r in repos]
    #     return self.backend.project_repositories.update_project_repositories(
    #         workspace_id=self._id, project_id=project.id, repo_ids=[r.id for r in repositories]
    #     )

    def schedule_project_refresh(self, project_id: int):
        for repo_id in self.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=self._id, project_id=project_id
        ):
            self.schedule_repository_refresh(repo_id=repo_id)

    def schedule_repository_refresh(self, repo_id: int):
        refresh_repository.delay(self.g.settings.dict(), self._id, repo_id)

    def refresh_repository(self, repository_id: int):
        self.refresh_repository_commits(repository_id)
        self.refresh_repository_pull_requests(repository_id)

        self.update_repository_status(repository_id, done=True, phase="done", status="finished")

    def refresh_repository_commits(self, repository_id: int):
        repository = self.backend.repositories.get_or_error(self._id, repository_id)
        credential = self._get_credential_for_repository(repository)
        self._extract_commits_patches(repository, credential)

    def refresh_repository_pull_requests(self, repository_id: int):
        repository = self.backend.repositories.get_or_error(self._id, repository_id)

        credential = self._get_credential_for_repository(repository)
        if not credential:
            # log ...
            return

        integration = self.g.integrations.get(repository.integration_name)
        if not integration:
            # log...
            return
        output = self.backend.output_handler(self._id)
        if hasattr(integration, "collect_pull_requests"):
            token = credential.to_token_dict(self.g.fernet)

            def _update_token(*args, **kwargs):
                print("****** UPDATE TOKEN *****")
                print(args, kwargs)
                print("****** UPDATE TOKEN *****")

            integration.collect_pull_requests(
                repository=repository, token=token, update_token=_update_token, output=output
            )
        else:
            # log ...
            pass

    def _extract_commits_patches(self, repository: RepositoryInDB, credential: Optional[CredentialInDB] = None):
        # repository_credential = credential.as_repository_credential() if credential else None
        # git_repository = repository.as_git_repository()
        # extract_incremental
        output = self.backend.output_handler(self._id)

        extract_incremental(
            repository=repository,
            output=output,
            settings=self.g.settings,
            credentials=credential.to_repository_credential(self.g.fernet) if credential else None,
        )

    # def _get_credential_for_repository(self, repository: RepositoryInDB) -> Optional[CredentialInDB]:
    #     if repository.credential_id:
    #         return self.backend.credentials.get(repository.credential_id)
    #     if repository.integration_name:
    #         return self.backend.credentials.get_by_user_and_integration(
    #             owner_id=self.workspace.created_by, integration_name=repository.integration_name
    #         )
    #     return None

    # def list_connected_repository_sources(self) -> List[str]:
    #     return [
    #         credential.integration_name
    #         for credential in self.get_credentials()
    #         if (
    #             credential.integration_name
    #             and credential.integration_type in REPOSITORY_SOURCES
    #             and credential.integration_name in self.g.integrations
    #         )
    #     ]

    # def list_available_repositories(self) -> List[RepositoryCreate]:
    #     def _fixme(*args, **kwargs):
    #         print("update token called", args, kwargs)

    #     results: List[RepositoryCreate] = []
    #     for credential in self.get_credentials():

    #         if credential.integration_type in REPOSITORY_SOURCES and credential.integration_name in self.g.integrations:

    #             integration = self.g.integrations[credential.integration_name]
    #             token = credential.to_token_dict(fernet=self.g.fernet)
    #             results += integration.list_available_private_repositories(token=token, update_token=_fixme)

    #     results += self.list_ssh_repositories()
    #     return results

    # def list_ssh_repositories(self) -> List[RepositoryCreate]:
    #     all_repositories = self.list_repositories()
    #     return [RepositoryCreate(**repo.dict()) for repo in all_repositories if repo.credential_id is not None]

    # def search_public_repositories(self, search: str) -> List[RepositoryCreate]:
    #     results: List[RepositoryCreate] = []

    #     def _fixme(*args, **kwargs):
    #         print("update token called", args, kwargs)

    #     for credential in self.get_credentials():

    #         if credential.integration_type in REPOSITORY_SOURCES and credential.integration_name in self.g.integrations:

    #             integration = self.g.integrations[credential.integration_name]
    #             token = credential.to_token_dict(fernet=self.g.fernet)
    #             results += integration.search_public_repositories(query=search, token=token, update_token=_fixme)

    #     return sorted(results, key=lambda i: levenshtein(search, i.name))

    # def list_project_repositories(self, project_id: int) -> List[RepositoryInDB]:
    #     ret = []
    #     for repo_id in self.backend.project_repositories.get_repo_ids_for_project(
    #         workspace_id=self._id, project_id=project_id
    #     ):
    #         repository = self.backend.repositories.get(workspace_id=self._id, id_=repo_id)
    #         if repository:
    #             ret.append(repository)
    #     return ret

    # def list_repositories(self) -> List[RepositoryInDB]:
    #     all_projects = self.list_projects()
    #     project_ids = [project.id for project in all_projects]

    #     repos = {}

    #     # HACK: Needed for ssh repositories
    #     for repo in self.backend.repositories.all(workspace_id=self._id):
    #         if repo.protocol == GitProtocol.ssh:
    #             repos[repo.id] = repo

    #     for project_id in project_ids:
    #         for repo_id in self.backend.project_repositories.get_repo_ids_for_project(
    #             workspace_id=self._id, project_id=project_id
    #         ):
    #             if repo_id not in repos:
    #                 repository = self.backend.repositories.get(workspace_id=self._id, id_=repo_id)
    #                 if repository:
    #                     repos[repo_id] = repository
    #     return list(repos.values())

    def create_repositories(self, repository_creates: List[RepositoryCreate]) -> List[RepositoryInDB]:
        return [
            self.g.backend.repositories.create_or_update(self._id, repository_create)
            for repository_create in repository_creates
        ]

    # def get_project_status(self, project_id: int) -> ProjectStatus:
    #     repositories = self.list_project_repositories(project_id=project_id)
    #     project = self.get_project(project_id=project_id)
    #     repo_statuses = [self.get_repo_status(r.id) for r in repositories]
    #     return ProjectStatus(
    #         id=project_id,
    #         name=project.name,
    #         status="FIXME",
    #         done=all(rs.done for rs in repo_statuses),
    #         repos=repo_statuses,
    #     )

    # def _repo_status_key(self, repository_id: int) -> str:
    #     return f"ws-{self._id}:repository-status-{repository_id}"

    # def get_repo_status(self, repository_id: int) -> RepositoryStatus:
    #     current_status_dict = self.g.kvstore.get_value(self._repo_status_key(repository_id))
    #     if current_status_dict:
    #         return RepositoryStatus(**cast(dict, current_status_dict))
    #     else:
    #         repository = self.backend.repositories.get(workspace_id=self._id, id_=repository_id)
    #         if repository:
    #             initial_status = RepositoryStatus(id=repository_id, name=repository.name)
    #             self.g.kvstore.set_value(self._repo_status_key(repository_id), initial_status.dict())
    #             return initial_status
    #         else:
    #             raise ValueError(f"No repository find for id {repository_id}")

    # def update_repository_status(self, repository_id: int, **kwargs) -> RepositoryStatus:
    #     current_status = self.get_repo_status(repository_id)
    #     status_dict = current_status.dict()
    #     status_dict.update(**kwargs)
    #     self.g.kvstore.set_value(self._repo_status_key(repository_id), status_dict)
    #     return RepositoryStatus(**status_dict)

    def calculate_stats(self, request: StatsRequest):
        return calculate_stats(request, workspace_id=self._id, settings=self.g.settings)

    def recalculate_repository_values(self, repository_id: int):
        extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df = self.backend.get_extracted_dataframes(
            workspace_id=self._id, repository_id=repository_id
        )
        # print(extracted_commits_df, extracted_patches_df, extracted_patch_rewrites_df)

        email_author_map = self._get_or_create_authors_from_commits(extracted_commits_df)

        calculated_commits_df = extracted_commits_df.copy()
        calculated_commits_df["date"] = calculated_commits_df["atime"]
        calculated_commits_df["aid"] = calculated_commits_df.apply(
            lambda row: email_author_map.get(row["aemail"]), axis=1
        )
        calculated_commits_df["cid"] = calculated_commits_df.apply(
            lambda row: email_author_map.get(row["cemail"]), axis=1
        )

        calculated_patches_df = extracted_patches_df.copy()

        # print(calculated_commits_df[["aemail", "aid", "cemail", "cid"]])

        self.backend.save_calculated_dataframes(
            workspace_id=self._id,
            repository_id=repository_id,
            calculated_commits_df=calculated_commits_df,
            calculated_patches_df=calculated_patches_df,
        )

    def _get_or_create_authors_from_commits(self, extracted_commits_df):
        authors_df = (
            extracted_commits_df[["aname", "aemail"]]
            .drop_duplicates()
            .rename(columns={"aname": "name", "aemail": "email"})
            .set_index("email")
        )

        commiters_df = (
            extracted_commits_df[["cname", "cemail"]]
            .drop_duplicates()
            .rename(columns={"cname": "name", "cemail": "email"})
            .set_index("email")
        )

        developers_df = pd.concat([authors_df, commiters_df])
        developers_df = developers_df[~developers_df.index.duplicated(keep="first")]

        authors = [
            self.backend.authors.get_or_create_author_for_alias(self._id, AuthorAlias(name=name, email=email))
            for email, name in developers_df["name"].to_dict().items()
        ]
        email_aid_map = {}
        for author in authors:
            for alias in author.aliases:
                email_aid_map[alias.email] = author.id

        return email_aid_map


# def get_workspace_ctrl(g: GitentialContext, workspace_id: int) -> WorkspaceCtrl:
#     return WorkspaceCtrlImpl(
#         id_=workspace_id,
#         backend=g.backend,
#         g=g,
#     )
