from typing import Tuple
from abc import ABC, abstractmethod
from gitential2 import datatypes
import pandas as pd
from gitential2.extraction.output import OutputHandler
from gitential2.settings import GitentialSettings

from .repositories import (
    AuthorRepository,
    UserRepository,
    UserInfoRepository,
    SubscriptionRepository,
    CredentialRepository,
    WorkspaceRepository,
    WorkspaceMemberRepository,
    ProjectRepository,
    RepositoryRepository,
    ProjectRepositoryRepository,
)


class GitentialBackend(ABC):
    def __init__(self, settings: GitentialSettings):
        self.settings = settings

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

    @abstractmethod
    def initialize_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def output_handler(self, workspace_id: int) -> OutputHandler:
        pass

    @abstractmethod
    def get_extracted_dataframes(
        self, workspace_id: int, repository_id: int
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        pass

    @abstractmethod
    def save_calculated_dataframes(
        self,
        workspace_id: int,
        repository_id: int,
        calculated_commits_df: pd.DataFrame,
        calculated_patches_df: pd.DataFrame,
    ):
        pass
