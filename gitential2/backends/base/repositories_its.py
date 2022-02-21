from abc import abstractmethod
from typing import Optional
from gitential2.datatypes.its import ITSIssue, ITSIssueChange, ITSIssueHeader, ITSIssueTimeInStatus, ITSIssueComment
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
