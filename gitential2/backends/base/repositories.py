# pylint: disable=unsubscriptable-object
from datetime import datetime
from abc import ABC, abstractmethod

from typing import Iterable, Optional, TypeVar, Generic, List, Tuple, Dict, Union, cast

import pandas
from gitential2.datatypes import (
    CoreModel,
    UserCreate,
    UserUpdate,
    UserInDB,
    WorkspaceCreate,
    WorkspaceUpdate,
    WorkspaceInDB,
    UserInfoCreate,
    UserInfoUpdate,
    UserInfoInDB,
    CredentialCreate,
    CredentialUpdate,
    CredentialInDB,
    AccessLog,
)
from gitential2.datatypes.calculated import CalculatedCommit, CalculatedCommitId, CalculatedPatch, CalculatedPatchId

from gitential2.datatypes.pull_requests import (
    PullRequest,
    PullRequestId,
    PullRequestComment,
    PullRequestCommentId,
    PullRequestCommit,
    PullRequestCommitId,
    PullRequestLabel,
    PullRequestLabelId,
)
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB
from gitential2.datatypes.projects import ProjectCreate, ProjectUpdate, ProjectInDB
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryInDB, RepositoryUpdate
from gitential2.datatypes.project_repositories import (
    ProjectRepositoryCreate,
    ProjectRepositoryInDB,
    ProjectRepositoryUpdate,
)
from gitential2.datatypes.authors import AuthorCreate, AuthorUpdate, AuthorInDB
from gitential2.datatypes.teams import TeamCreate, TeamUpdate, TeamInDB
from gitential2.datatypes.teammembers import TeamMemberCreate, TeamMemberInDB, TeamMemberUpdate

from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB
from gitential2.datatypes.extraction import (
    ExtractedCommit,
    ExtractedCommitId,
    ExtractedPatch,
    ExtractedPatchId,
    ExtractedPatchRewrite,
    ExtractedPatchRewriteId,
)
from gitential2.datatypes.email_log import (
    EmailLogCreate,
    EmailLogUpdate,
    EmailLogInDB,
    EmailLogStatus,
    EmailLogTemplate,
)

IdType = TypeVar("IdType")
CreateType = TypeVar("CreateType", bound=CoreModel)
UpdateType = TypeVar("UpdateType", bound=CoreModel)
InDBType = TypeVar("InDBType", bound=CoreModel)


class NotFoundException(Exception):
    pass


class AccessLogRepository(ABC):
    @abstractmethod
    def create(self, log: AccessLog) -> AccessLog:
        pass

    @abstractmethod
    def last_interaction(self, user_id: int) -> Optional[AccessLog]:
        pass


class BaseRepository(ABC, Generic[IdType, CreateType, UpdateType, InDBType]):
    @abstractmethod
    def get(self, id_: IdType) -> Optional[InDBType]:
        pass

    def get_or_error(self, id_: IdType) -> InDBType:
        obj = self.get(id_)
        if obj:
            return obj
        else:
            raise NotFoundException("Object not found.")

    @abstractmethod
    def create(self, obj: CreateType) -> InDBType:
        pass

    @abstractmethod
    def create_or_update(self, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        pass

    @abstractmethod
    def insert(self, id_: IdType, obj: InDBType) -> InDBType:
        pass

    @abstractmethod
    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        pass

    @abstractmethod
    def delete(self, id_: IdType) -> int:
        pass

    @abstractmethod
    def all(self) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def truncate(self):
        pass

    @abstractmethod
    def reset_primary_key_id(self):
        pass


class BaseWorkspaceScopedRepository(ABC, Generic[IdType, CreateType, UpdateType, InDBType]):
    @abstractmethod
    def get(self, workspace_id: int, id_: IdType) -> Optional[InDBType]:
        pass

    def get_or_error(self, workspace_id: int, id_: IdType) -> InDBType:
        obj = self.get(workspace_id, id_)
        if obj:
            return obj
        else:
            raise NotFoundException("Object not found.")

    @abstractmethod
    def create(self, workspace_id: int, obj: CreateType) -> InDBType:
        pass

    @abstractmethod
    def create_or_update(self, workspace_id: int, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        pass

    @abstractmethod
    def insert(self, workspace_id: int, id_: IdType, obj: InDBType) -> InDBType:
        pass

    @abstractmethod
    def update(self, workspace_id: int, id_: IdType, obj: UpdateType) -> InDBType:
        pass

    @abstractmethod
    def delete(self, workspace_id: int, id_: IdType) -> int:
        pass

    @abstractmethod
    def all(self, workspace_id: int) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def iterate_all(self, workspace_id: int) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def truncate(self, workspace_id: int):
        pass

    @abstractmethod
    def reset_primary_key_id(self, workspace_id: int):
        pass

    # TODO: Implement search for rest of the children -
    # @abstractmethod
    # def search(self, workspace_id: int, q: str) -> Iterable[InDBType]:
    #     pass


class UserRepository(BaseRepository[int, UserCreate, UserUpdate, UserInDB]):
    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserInDB]:
        pass


class SubscriptionRepository(BaseRepository[int, SubscriptionCreate, SubscriptionUpdate, SubscriptionInDB]):
    @abstractmethod
    def get_subscriptions_for_user(self, user_id: int) -> List[SubscriptionInDB]:
        pass


class UserInfoRepository(BaseRepository[int, UserInfoCreate, UserInfoUpdate, UserInfoInDB]):
    @abstractmethod
    def get_by_sub_and_integration(self, sub: str, integration_name: str) -> Optional[UserInfoInDB]:
        pass

    @abstractmethod
    def get_for_user(self, user_id: int) -> List[UserInfoInDB]:
        pass

    @abstractmethod
    def get_by_email(self, email: str) -> Optional[UserInfoInDB]:
        pass


class CredentialRepository(BaseRepository[int, CredentialCreate, CredentialUpdate, CredentialInDB]):
    @abstractmethod
    def get_by_user_and_integration(self, owner_id: int, integration_name: str) -> Optional[CredentialInDB]:
        pass

    @abstractmethod
    def get_for_user(self, owner_id) -> List[CredentialInDB]:
        pass


class WorkspaceRepository(BaseRepository[int, WorkspaceCreate, WorkspaceUpdate, WorkspaceInDB]):
    @abstractmethod
    def get_worskpaces_by_ids(self, workspace_ids: List[int]) -> List[WorkspaceInDB]:
        pass


class WorkspaceMemberRepository(BaseRepository[int, WorkspaceMemberCreate, WorkspaceMemberUpdate, WorkspaceMemberInDB]):
    @abstractmethod
    def get_for_user(self, user_id: int) -> List[WorkspaceMemberInDB]:
        pass

    @abstractmethod
    def get_for_workspace(self, workspace_id: int) -> List[WorkspaceMemberInDB]:
        pass

    @abstractmethod
    def get_for_workspace_and_user(self, workspace_id: int, user_id: int) -> Optional[WorkspaceMemberInDB]:
        pass


class ProjectRepository(BaseWorkspaceScopedRepository[int, ProjectCreate, ProjectUpdate, ProjectInDB]):
    @abstractmethod
    def search(self, workspace_id: int, q: str) -> Dict[ProjectInDB]:
        pass


class RepositoryRepository(BaseWorkspaceScopedRepository[int, RepositoryCreate, RepositoryUpdate, RepositoryInDB]):
    @abstractmethod
    def get_by_clone_url(self, workspace_id: int, clone_url: str) -> Optional[RepositoryInDB]:
        pass

    @abstractmethod
    def search(self, workspace_id: int, q: str) -> Dict[RepositoryInDB]:
        pass

    def create_or_update_by_clone_url(
        self, workspace_id: int, obj: Union[RepositoryCreate, RepositoryUpdate, RepositoryInDB]
    ) -> RepositoryInDB:
        existing = self.get_by_clone_url(workspace_id, obj.clone_url)
        if existing:
            return self.update(workspace_id=workspace_id, id_=existing.id, obj=RepositoryUpdate(**obj.dict()))
        else:
            return self.create(workspace_id=workspace_id, obj=cast(RepositoryCreate, obj))


class ProjectRepositoryRepository(
    BaseWorkspaceScopedRepository[int, ProjectRepositoryCreate, ProjectRepositoryUpdate, ProjectRepositoryInDB]
):
    @abstractmethod
    def get_repo_ids_for_project(self, workspace_id: int, project_id: int) -> List[int]:
        pass

    @abstractmethod
    def add_repo_ids_to_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        pass

    @abstractmethod
    def remove_repo_ids_from_project(self, workspace_id: int, project_id: int, repo_ids: List[int]):
        pass

    def update_project_repositories(
        self, workspace_id: int, project_id: int, repo_ids: List[int]
    ) -> Tuple[List[int], List[int], List[int]]:
        current_ids = self.get_repo_ids_for_project(workspace_id=workspace_id, project_id=project_id)
        ids_needs_addition = [r_id for r_id in repo_ids if r_id not in current_ids]
        ids_needs_removal = [r_id for r_id in current_ids if r_id not in repo_ids]
        ids_kept = [r_id for r_id in current_ids if r_id in repo_ids]
        if ids_needs_addition:
            self.add_repo_ids_to_project(workspace_id, project_id, ids_needs_addition)
        if ids_needs_removal:
            self.remove_repo_ids_from_project(workspace_id, project_id, ids_needs_removal)
        return ids_needs_addition, ids_needs_removal, ids_kept


class RepoDFMixin(ABC):
    @abstractmethod
    def get_repo_df(self, workspace_id: int, repo_id: int) -> pandas.DataFrame:
        pass


class ExtractedCommitRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[ExtractedCommitId, ExtractedCommit, ExtractedCommit, ExtractedCommit],
):
    pass


class ExtractedPatchRepository(
    RepoDFMixin, BaseWorkspaceScopedRepository[ExtractedPatchId, ExtractedPatch, ExtractedPatch, ExtractedPatch]
):
    pass


class ExtractedPatchRewriteRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[
        ExtractedPatchRewriteId, ExtractedPatchRewrite, ExtractedPatchRewrite, ExtractedPatchRewrite
    ],
):
    pass


class CalculatedCommitRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[CalculatedCommitId, CalculatedCommit, CalculatedCommit, CalculatedCommit],
):
    pass


class CalculatedPatchRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[CalculatedPatchId, CalculatedPatch, CalculatedPatch, CalculatedPatch],
):
    pass


class PullRequestRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestId, PullRequest, PullRequest, PullRequest],
):
    @abstractmethod
    def get_prs_updated_at(self, workspace_id: int, repository_id: int) -> Dict[int, datetime]:
        pass


class PullRequestCommitRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestCommitId, PullRequestCommit, PullRequestCommit, PullRequestCommit],
):
    pass


class PullRequestCommentRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestCommentId, PullRequestComment, PullRequestComment, PullRequestComment],
):
    pass


class PullRequestLabelRepository(
    RepoDFMixin,
    BaseWorkspaceScopedRepository[PullRequestLabelId, PullRequestLabel, PullRequestLabel, PullRequestLabel],
):
    pass


class AuthorRepository(BaseWorkspaceScopedRepository[int, AuthorCreate, AuthorUpdate, AuthorInDB]):
    @abstractmethod
    def search(self, workspace_id: int, q: str) -> Dict[AuthorInDB]:
        pass


class TeamRepository(BaseWorkspaceScopedRepository[int, TeamCreate, TeamUpdate, TeamInDB]):
    pass


class TeamMemberRepository(BaseWorkspaceScopedRepository[int, TeamMemberCreate, TeamMemberUpdate, TeamMemberInDB]):
    @abstractmethod
    def add_members_to_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> List[TeamMemberInDB]:
        pass

    @abstractmethod
    def remove_members_from_team(self, workspace_id: int, team_id: int, author_ids: List[int]) -> int:
        pass

    @abstractmethod
    def get_team_member_author_ids(self, workspace_id: int, team_id: int) -> List[int]:
        pass

    @abstractmethod
    def get_author_team_ids(self, workspace_id: int, author_id: int) -> List[int]:
        pass

    def remove_all_members_from_team(self, workspace_id: int, team_id) -> int:
        author_ids = self.get_team_member_author_ids(workspace_id, team_id)
        return self.remove_members_from_team(workspace_id, team_id, author_ids)


class EmailLogRepository(BaseRepository[int, EmailLogCreate, EmailLogUpdate, EmailLogInDB]):
    def schedule_email(self, user_id: int, template_name: str, scheduled_at: Optional[datetime] = None) -> EmailLogInDB:
        email_log_create = EmailLogCreate(
            user_id=user_id, template_name=template_name, scheduled_at=scheduled_at or datetime.utcnow()
        )
        return self.create(email_log_create)

    @abstractmethod
    def email_log_status_update(self, row_id: int, status: EmailLogStatus) -> Optional[EmailLogInDB]:
        pass

    @abstractmethod
    def get_emails_to_send(self) -> List[EmailLogInDB]:
        pass

    @abstractmethod
    def cancel_email(self, user_id: int, template: EmailLogTemplate) -> Optional[List[EmailLogInDB]]:
        pass
