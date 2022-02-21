from typing import Optional
from sqlalchemy.sql import select
from gitential2.datatypes.its import ITSIssue, ITSIssueChange, ITSIssueTimeInStatus, ITSIssueComment, ITSIssueHeader
from gitential2.backends.base import (
    ITSIssueRepository,
    ITSIssueChangeRepository,
    ITSIssueTimeInStatusRepository,
    ITSIssueCommentRepository,
)
from .repositories import SQLWorkspaceScopedRepository, fetchone_


class SQLITSIssueRepository(
    ITSIssueRepository,
    SQLWorkspaceScopedRepository[str, ITSIssue, ITSIssue, ITSIssue],
):
    def get_header(self, workspace_id: int, id_: str) -> Optional[ITSIssueHeader]:
        query = (
            select(
                [
                    self.table.c.id,
                    self.table.c.itsp_id,
                    self.table.c.api_url,
                    self.table.c.api_id,
                    self.table.c.key,
                    self.table.c.status_name,
                    self.table.c.status_id,
                    self.table.c.status_category,
                    self.table.c.summary,
                    self.table.c.created_at,
                    self.table.c.updated_at,
                ]
            )
            .where(self.identity(id_))
            .limit(1)
        )
        row = self._execute_query(query, workspace_id=workspace_id, callback_fn=fetchone_)
        return ITSIssueHeader(**row) if row else None


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
