from gitential2.datatypes.its import ITSIssue, ITSIssueChange, ITSIssueTimeInStatus, ITSIssueComment
from gitential2.backends.base import (
    ITSIssueRepository,
    ITSIssueChangeRepository,
    ITSIssueTimeInStatusRepository,
    ITSIssueCommentRepository,
)
from .repositories import SQLWorkspaceScopedRepository


class SQLITSIssueRepository(
    ITSIssueRepository,
    SQLWorkspaceScopedRepository[str, ITSIssue, ITSIssue, ITSIssue],
):
    pass


class SQLITSIssueChangeRepository(
    ITSIssueChangeRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueChange, ITSIssueChange, ITSIssueChange],
):
    pass


class SQLITSIssueTimeInStatusRepository(
    ITSIssueTimeInStatusRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueTimeInStatus, ITSIssueTimeInStatus, ITSIssueTimeInStatus],
):
    pass


class SQLITSIssueCommentRepository(
    ITSIssueCommentRepository,
    SQLWorkspaceScopedRepository[str, ITSIssueComment, ITSIssueComment, ITSIssueComment],
):
    pass
