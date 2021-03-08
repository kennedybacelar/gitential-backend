from .repositories import (
    AuthorRepository,
    ProjectRepositoryRepository,
    RepositoryRepository,
    UserRepository,
    SubscriptionRepository,
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
    _authors: AuthorRepository
    _subscriptions: SubscriptionRepository

    @property
    def users(self) -> UserRepository:
        return self._users

    @property
    def subscriptions(self) -> SubscriptionRepository:
        return self._subscriptions

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

    @property
    def authors(self) -> AuthorRepository:
        return self._authors