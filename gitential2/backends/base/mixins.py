from .repositories import (
    ProjectRepositoryRepository,
    RepositoryRepository,
    UserRepository,
    UserInfoRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    CredentialRepository,
    ProjectRepository,
)


class WithRepositoriesMixin:
    _users: UserRepository
    _user_infos: UserInfoRepository
    _workspaces: WorkspaceRepository
    _workspace_members: WorkspaceMemberRepository
    _credentials: CredentialRepository
    _projects: ProjectRepository
    _repositories: RepositoryRepository
    _project_repositories: ProjectRepositoryRepository

    @property
    def users(self) -> UserRepository:
        return self._users

    @property
    def user_infos(self) -> UserInfoRepository:
        return self._user_infos

    @property
    def workspaces(self) -> WorkspaceRepository:
        return self._workspaces

    @property
    def workspace_members(self) -> WorkspaceMemberRepository:
        return self._workspace_members

    @property
    def credentials(self) -> CredentialRepository:
        return self._credentials

    @property
    def projects(self) -> ProjectRepository:
        return self._projects

    @property
    def repositories(self) -> RepositoryRepository:
        return self._repositories

    @property
    def project_repositories(self) -> ProjectRepositoryRepository:
        return self._project_repositories
