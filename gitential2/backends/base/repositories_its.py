from abc import abstractmethod
from typing import Optional
from gitential2.datatypes.its import (
    ITSIssue,
    ITSIssueChange,
    ITSIssueHeader,
    ITSIssueSprint,
    ITSIssueTimeInStatus,
    ITSIssueComment,
    ITSIssueLinkedIssue,
    ITSIssueWorklog,
    ITSSprint,
)
from .repositories_base import BaseWorkspaceScopedRepository


class ITSIssueRepository(
    BaseWorkspaceScopedRepository[str, ITSIssue, ITSIssue, ITSIssue],
):
    @abstractmethod
    def get_header(self, workspace_id: int, id_: str) -> Optional[ITSIssueHeader]:
        pass


class ITSIssueChangeRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueChange, ITSIssueChange, ITSIssueChange],
):
    pass


class ITSIssueTimeInStatusRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueTimeInStatus, ITSIssueTimeInStatus, ITSIssueTimeInStatus],
):
    pass


class ITSIssueCommentRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueComment, ITSIssueComment, ITSIssueComment],
):
    pass


class ITSIssueLinkedIssueRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueLinkedIssue, ITSIssueLinkedIssue, ITSIssueLinkedIssue],
):
    pass


class ITSSprintRepository(
    BaseWorkspaceScopedRepository[str, ITSSprint, ITSSprint, ITSSprint],
):
    pass


class ITSIssueSprintRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueSprint, ITSIssueSprint, ITSIssueSprint],
):
    pass


class ITSIssueWorklogRepository(
    BaseWorkspaceScopedRepository[str, ITSIssueWorklog, ITSIssueWorklog, ITSIssueWorklog],
):
    pass
