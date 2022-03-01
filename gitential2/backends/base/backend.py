from datetime import datetime
from typing import Tuple, Set
from abc import ABC, abstractmethod
from ibis.expr.types import TableExpr
import pandas as pd
from gitential2.extraction.output import OutputHandler
from gitential2.settings import GitentialSettings
from gitential2.datatypes.stats import IbisTables

from .repositories import (
    AccessLogRepository,
    AuthorRepository,
    CalculatedPatchRepository,
    TeamMemberRepository,
    TeamRepository,
    ExtractedCommitRepository,
    ExtractedPatchRepository,
    ExtractedPatchRewriteRepository,
    CalculatedCommitRepository,
    UserRepository,
    UserInfoRepository,
    SubscriptionRepository,
    CredentialRepository,
    WorkspaceInvitationRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    ProjectRepository,
    RepositoryRepository,
    ITSProjectRepository,
    ProjectRepositoryRepository,
    ProjectITSProjectRepository,
    PullRequestRepository,
    PullRequestCommitRepository,
    PullRequestCommentRepository,
    PullRequestLabelRepository,
    EmailLogRepository,
    ExtractedCommitBranchRepository,
)

from .repositories_its import (
    ITSIssueRepository,
    ITSIssueChangeRepository,
    ITSIssueCommentRepository,
    ITSIssueTimeInStatusRepository,
)


class GitentialBackend(ABC):
    def __init__(self, settings: GitentialSettings):
        self.settings = settings

    @property
    @abstractmethod
    def access_logs(self) -> AccessLogRepository:
        pass

    @property
    @abstractmethod
    def users(self) -> UserRepository:
        pass

    @property
    @abstractmethod
    def subscriptions(self) -> SubscriptionRepository:
        pass

    @property
    @abstractmethod
    def user_infos(self) -> UserInfoRepository:
        pass

    @property
    @abstractmethod
    def credentials(self) -> CredentialRepository:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> WorkspaceRepository:
        pass

    @property
    @abstractmethod
    def workspace_invitations(self) -> WorkspaceInvitationRepository:
        pass

    @property
    @abstractmethod
    def workspace_members(self) -> WorkspaceMemberRepository:
        pass

    @property
    @abstractmethod
    def projects(self) -> ProjectRepository:
        pass

    @property
    @abstractmethod
    def repositories(self) -> RepositoryRepository:
        pass

    @property
    @abstractmethod
    def its_projects(self) -> ITSProjectRepository:
        pass

    @property
    @abstractmethod
    def project_repositories(self) -> ProjectRepositoryRepository:
        pass

    @property
    @abstractmethod
    def project_its_projects(self) -> ProjectITSProjectRepository:
        pass

    @property
    @abstractmethod
    def authors(self) -> AuthorRepository:
        pass

    @property
    @abstractmethod
    def teams(self) -> TeamRepository:
        pass

    @property
    @abstractmethod
    def team_members(self) -> TeamMemberRepository:
        pass

    @property
    @abstractmethod
    def extracted_commits(self) -> ExtractedCommitRepository:
        pass

    @property
    @abstractmethod
    def extracted_patches(self) -> ExtractedPatchRepository:
        pass

    @property
    @abstractmethod
    def extracted_commit_branches(self) -> ExtractedCommitBranchRepository:
        pass

    @property
    @abstractmethod
    def extracted_patch_rewrites(self) -> ExtractedPatchRewriteRepository:
        pass

    @property
    @abstractmethod
    def calculated_commits(self) -> CalculatedCommitRepository:
        pass

    @property
    @abstractmethod
    def calculated_patches(self) -> CalculatedPatchRepository:
        pass

    @property
    @abstractmethod
    def pull_requests(self) -> PullRequestRepository:
        pass

    @property
    @abstractmethod
    def pull_request_commits(self) -> PullRequestCommitRepository:
        pass

    @property
    @abstractmethod
    def pull_request_comments(self) -> PullRequestCommentRepository:
        pass

    @property
    @abstractmethod
    def pull_request_labels(self) -> PullRequestLabelRepository:
        pass

    @property
    @abstractmethod
    def its_issues(self) -> ITSIssueRepository:
        pass

    @property
    @abstractmethod
    def its_issue_changes(self) -> ITSIssueChangeRepository:
        pass

    @property
    @abstractmethod
    def its_issue_times_in_statuses(self) -> ITSIssueTimeInStatusRepository:
        pass

    @property
    @abstractmethod
    def its_issue_comments(self) -> ITSIssueCommentRepository:
        pass

    @property
    @abstractmethod
    def email_log(self) -> EmailLogRepository:
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def initialize_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def migrate(self):
        pass

    @abstractmethod
    def migrate_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def create_missing_materialized_views(self, workspace_id: int):
        pass

    @abstractmethod
    def drop_existing_materialized_views(self, workspace_id: int):
        pass

    @abstractmethod
    def refresh_materialized_views(self, workspace_id: int):
        pass

    @abstractmethod
    def output_handler(self, workspace_id: int) -> OutputHandler:
        pass

    @abstractmethod
    def get_commit_ids_for_repository(self, workspace_id: int, repository_id: int) -> Set[str]:
        pass

    @abstractmethod
    def get_extracted_dataframes(
        self, workspace_id: int, repository_id: int, from_: datetime, to_: datetime
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        pass

    @abstractmethod
    def save_calculated_dataframes(
        self,
        workspace_id: int,
        repository_id: int,
        calculated_commits_df: pd.DataFrame,
        calculated_patches_df: pd.DataFrame,
        from_: datetime,
        to_: datetime,
    ):
        pass

    @abstractmethod
    def get_ibis_tables(self, workspace_id: int) -> IbisTables:
        pass

    @abstractmethod
    def get_ibis_table(self, workspace_id: int, source_name: str) -> TableExpr:
        pass
