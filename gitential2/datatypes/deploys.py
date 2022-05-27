from datetime import datetime
from typing import Optional, List, Tuple

from .common import CoreModel, ExtraFieldMixin, StringIdModelMixin
from .export import ExportableModel


class DeployedPullRequest(StringIdModelMixin, ExtraFieldMixin, CoreModel):
    repository_name: Optional[str]
    title: Optional[str]
    created_at: datetime
    merged_at: Optional[datetime]


class DeployedCommit(StringIdModelMixin, CoreModel):
    title: str
    repository_name: str
    git_ref: str


class DeployedIssue(CoreModel):
    repository_name: str
    issue_id: str


class Deploy(StringIdModelMixin, ExtraFieldMixin, CoreModel, ExportableModel):
    repositories: Optional[List[str]]
    environments: Optional[List[str]]
    pull_requests: Optional[List[DeployedPullRequest]]
    commits: List[DeployedCommit]
    issues: Optional[List[DeployedIssue]]
    deployed_at: datetime

    def export_names(self) -> Tuple[str, str]:
        return ("deploy", "deploys")

    def export_fields(self) -> List[str]:
        return [
            "id",
            "repositories",
            "environments",
            "pull_requests",
            "commits",
            "issues",
            "deployed_at",
            "extra",
        ]
