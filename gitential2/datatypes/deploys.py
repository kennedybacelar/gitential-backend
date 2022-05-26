from datetime import datetime
from typing import Optional, List

from .common import CoreModel, ExtraFieldMixin, StringIdModelMixin


class DeployedPullRequest(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    repository_name: Optional[str]
    title: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime] = None


class DeployedCommit(CoreModel):
    id: str
    title: str
    repository_name: str
    git_ref: str


class DeployedIssue(CoreModel):
    repository_name: str
    issue_id: str


class Deploy(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    repositories: List[str]
    environments: List[str]
    pull_requests: List[DeployedPullRequest]
    commits: List[DeployedCommit]
    issues: List[DeployedIssue]
    deployed_at: datetime
