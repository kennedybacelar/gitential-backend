from datetime import datetime
from typing import Tuple, Set
from abc import ABC, abstractmethod
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
    WorkspaceRepository,
    WorkspaceMemberRepository,
    ProjectRepository,
    RepositoryRepository,
    ProjectRepositoryRepository,
    PullRequestRepository,
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
    def project_repositories(self) -> ProjectRepositoryRepository:
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

    @abstractmethod
    def initialize_workspace(self, workspace_id: int):
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
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
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
