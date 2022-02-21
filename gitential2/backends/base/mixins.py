from .repositories import (
    AccessLogRepository,
    AuthorRepository,
    EmailLogRepository,
    CalculatedCommitRepository,
    CalculatedPatchRepository,
    ITSProjectRepository,
    ProjectITSProjectRepository,
    TeamMemberRepository,
    TeamRepository,
    ExtractedCommitRepository,
    ExtractedPatchRepository,
    ExtractedPatchRewriteRepository,
    ProjectRepositoryRepository,
    PullRequestRepository,
    PullRequestCommitRepository,
    PullRequestCommentRepository,
    PullRequestLabelRepository,
    RepositoryRepository,
    UserRepository,
    SubscriptionRepository,
    UserInfoRepository,
    WorkspaceRepository,
    WorkspaceInvitationRepository,
    WorkspaceMemberRepository,
    CredentialRepository,
    ProjectRepository,
    ExtractedCommitBranchRepository,
)


from .repositories_its import (
    ITSIssueRepository,
    ITSIssueChangeRepository,
    ITSIssueCommentRepository,
    ITSIssueTimeInStatusRepository,
)


class WithRepositoriesMixin:
    _access_logs: AccessLogRepository
    _users: UserRepository
    _user_infos: UserInfoRepository
    _workspaces: WorkspaceRepository
    _workspace_invitations: WorkspaceInvitationRepository
    _workspace_members: WorkspaceMemberRepository
    _credentials: CredentialRepository
    _projects: ProjectRepository
    _repositories: RepositoryRepository
    _its_projects: ITSProjectRepository
    _project_repositories: ProjectRepositoryRepository
    _project_its_projects: ProjectITSProjectRepository
    _authors: AuthorRepository
    _teams: TeamRepository
    _team_members: TeamMemberRepository
    _subscriptions: SubscriptionRepository
    _extracted_commits: ExtractedCommitRepository
    _extracted_patches: ExtractedPatchRepository
    _extracted_commit_branches: ExtractedCommitBranchRepository
    _extracted_patch_rewrites: ExtractedPatchRewriteRepository
    _calculated_commits: CalculatedCommitRepository
    _calculated_patches: CalculatedPatchRepository

    _pull_requests: PullRequestRepository
    _pull_request_commits: PullRequestCommitRepository
    _pull_request_comments: PullRequestCommentRepository
    _pull_request_labels: PullRequestLabelRepository
    _email_log: EmailLogRepository

    _its_issues: ITSIssueRepository
    _its_issue_changes: ITSIssueChangeRepository
    _its_issue_times_in_statuses: ITSIssueTimeInStatusRepository
    _its_issue_comments: ITSIssueCommentRepository

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
    def workspace_invitations(self) -> WorkspaceInvitationRepository:
        return self._workspace_invitations

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
    def its_projects(self) -> ITSProjectRepository:
        return self._its_projects

    @property
    def project_repositories(self) -> ProjectRepositoryRepository:
        return self._project_repositories

    @property
    def project_its_projects(self) -> ProjectITSProjectRepository:
        return self._project_its_projects

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
    def extracted_commit_branches(self) -> ExtractedCommitBranchRepository:
        return self._extracted_commit_branches

    @property
    def extracted_patch_rewrites(self) -> ExtractedPatchRewriteRepository:
        return self._extracted_patch_rewrites

    @property
    def calculated_commits(self) -> CalculatedCommitRepository:
        return self._calculated_commits

    @property
    def calculated_patches(self) -> CalculatedPatchRepository:
        return self._calculated_patches

    @property
    def pull_requests(self) -> PullRequestRepository:
        return self._pull_requests

    @property
    def pull_request_commits(self) -> PullRequestCommitRepository:
        return self._pull_request_commits

    @property
    def pull_request_comments(self) -> PullRequestCommentRepository:
        return self._pull_request_comments

    @property
    def pull_request_labels(self) -> PullRequestLabelRepository:
        return self._pull_request_labels

    @property
    def teams(self) -> TeamRepository:
        return self._teams

    @property
    def team_members(self) -> TeamMemberRepository:
        return self._team_members

    @property
    def its_issues(self) -> ITSIssueRepository:
        return self._its_issues

    @property
    def its_issue_changes(self) -> ITSIssueChangeRepository:
        return self._its_issue_changes

    @property
    def its_issue_times_in_statuses(self) -> ITSIssueTimeInStatusRepository:
        return self._its_issue_times_in_statuses

    @property
    def its_issue_comments(self) -> ITSIssueCommentRepository:
        return self._its_issue_comments

    @property
    def email_log(self) -> EmailLogRepository:
        return self._email_log
