from gitential2.datatypes.its import ITSIssue, ITSIssueChange, ITSIssueTimeInStatus, ITSIssueComment
from .repositories_base import BaseWorkspaceScopedRepository


class ITSIssueRepository(
    BaseWorkspaceScopedRepository[str, ITSIssue, ITSIssue, ITSIssue],
):
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
