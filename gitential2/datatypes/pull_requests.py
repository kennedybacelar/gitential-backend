from typing import Optional
from datetime import datetime
from enum import Enum
from .common import CoreModel, ExtraFieldMixin


class PullRequestState(str, Enum):
    open = "open"
    merged = "merged"
    closed = "closed"

    @classmethod
    def from_gitlab(cls, state):
        if state == "merged":
            return cls.merged
        elif state in ["opened", "locked"]:
            return cls.open
        elif state == "closed":
            return cls.closed
        else:
            raise ValueError("invalid state for MR")

    @classmethod
    def from_github(cls, state, merged_at):
        if merged_at and state == "closed":
            return cls.merged
        elif state in ["opened", "locked", "open"]:
            return cls.open
        elif state == "closed":
            return cls.closed
        else:
            raise ValueError(f'invalid state for Github PR "{state}"')


class PullRequestId(CoreModel):
    repo_id: int
    number: int


class PullRequest(ExtraFieldMixin, CoreModel):
    repo_id: int
    number: int
    title: str
    platform: str
    id_platform: int
    api_resource_uri: str
    state_platform: str
    state: str
    created_at: datetime
    closed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    merged_at: Optional[datetime] = None
    additions: int
    deletions: int
    changed_files: int
    draft: bool
    user: str
    commits: int
    merged_by: Optional[str] = None
    first_reaction_at: Optional[datetime] = None
    first_commit_authored_at: Optional[datetime] = None

    @property
    def id_(self):
        return PullRequestId(repo_id=self.repo_id, number=self.number)


class PullRequestCommit(ExtraFieldMixin, CoreModel):
    repo_id: int
    pr_number: int
    commit_id: str
    author_name: str
    author_email: str
    author_date: datetime
    author_login: str
    committer_name: str
    committer_email: str
    committer_date: datetime
    committer_login: str
