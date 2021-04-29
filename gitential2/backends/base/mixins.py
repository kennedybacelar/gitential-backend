from .repositories import (
    AccessLogRepository,
    AuthorRepository,
    TeamMemberRepository,
    TeamRepository,
    ExtractedCommitRepository,
    ExtractedPatchRepository,
    ExtractedPatchRewriteRepository,
    ProjectRepositoryRepository,
    PullRequestRepository,
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
    _access_logs: AccessLogRepository
    _users: UserRepository
    _user_infos: UserInfoRepository
    _workspaces: WorkspaceRepository
    _workspace_members: WorkspaceMemberRepository
    _credentials: CredentialRepository
    _projects: ProjectRepository
    _repositories: RepositoryRepository
    _project_repositories: ProjectRepositoryRepository
    _authors: AuthorRepository
    _teams: TeamRepository
    _team_members: TeamMemberRepository
    _subscriptions: SubscriptionRepository
    _extracted_commits: ExtractedCommitRepository
    _extracted_patches: ExtractedPatchRepository
    _extracted_patch_rewrites: ExtractedPatchRewriteRepository
    _pull_requests: PullRequestRepository

    @property
    def access_logs(self):
        return self._access_logs

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

    @property
    def extracted_commits(self) -> ExtractedCommitRepository:
        return self._extracted_commits

    @property
    def extracted_patches(self) -> ExtractedPatchRepository:
        return self._extracted_patches

    @property
    def extracted_patch_rewrites(self) -> ExtractedPatchRewriteRepository:
        return self._extracted_patch_rewrites

    @property
    def pull_requests(self) -> PullRequestRepository:
        return self._pull_requests

    @property
    def teams(self) -> TeamRepository:
        return self._teams

    @property
    def team_members(self) -> TeamMemberRepository:
        return self._team_members
