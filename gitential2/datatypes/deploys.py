from datetime import datetime
from typing import Optional, List

from .common import CoreModel, ExtraFieldMixin, StringIdModelMixin, DateTimeModelMixin


class DeployedPullRequest(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    repo_id: int
    number: Optional[int]
    repo_name: Optional[str]
    title: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime] = None


class DeployedCommit(CoreModel):
    repo_id: int
    pr_number: int
    commit_id: str


class DeployedIssue(StringIdModelMixin, DateTimeModelMixin, CoreModel):
    repo_id: int
    pr_number: int
    issue_id: int


class Deploy(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    pull_requests: List[DeployedPullRequest]
    commits: List[DeployedCommit]
    issues: List[DeployedIssue]
    environment: str
    deployed_at: datetime
