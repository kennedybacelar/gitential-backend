from datetime import datetime
from enum import Enum
from typing import Optional, Tuple, List

from pydantic import BaseModel
from .common import CoreModel, DateTimeModelMixin, ExtraFieldMixin, StringIdModelMixin
from .export import ExportableModel


class ITSIssueChangeType(str, Enum):
    other = "other"

    # Important changes
    sprint = "sprint"
    status = "status"
    assignee = "assignee"


class ITSIssueStatusCategory(str, Enum):
    unknown = "unknown"
    new = "new"
    in_progress = "in_progress"
    done = "done"


def its_issue_status_category_from_str(integration_type: str, value: str) -> ITSIssueStatusCategory:
    jira_values = {
        "indeterminate": ITSIssueStatusCategory.in_progress,
        "done": ITSIssueStatusCategory.done,
        "new": ITSIssueStatusCategory.new,
    }
    if integration_type == "jira":
        return jira_values.get(value.lower(), ITSIssueStatusCategory.unknown)
    else:
        raise ValueError(f"Invalid integration_type for status category: {integration_type}")


class ITSIssueHeader(StringIdModelMixin, DateTimeModelMixin, CoreModel):
    itsp_id: int
    api_url: str
    api_id: str
    key: Optional[str] = None
    status_name: str
    status_id: Optional[str] = None
    status_category: Optional[str] = None  # todo, in progress/indeterminate, done
    summary: str = ""


class ITSIssue(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    itsp_id: int
    api_url: str
    api_id: str
    key: Optional[str] = None

    status_name: str
    status_id: Optional[str] = None
    status_category_api: Optional[str] = None  # todo, inprogress, done
    status_category: Optional[ITSIssueStatusCategory] = None  # todo, inprogress, done

    issue_type_name: str
    issue_type_id: Optional[str] = None

    parent_id: Optional[str] = None

    resolution_name: Optional[str] = None
    resolution_id: Optional[str] = None
    resolution_date: Optional[datetime] = None

    priority_name: Optional[str] = None
    priority_id: Optional[str] = None
    priority_order: Optional[int] = None

    summary: str = ""
    description: str = ""

    # creator
    creator_api_id: Optional[str] = None
    creator_email: Optional[str] = None
    creator_name: Optional[str] = None
    creator_dev_id: Optional[int] = None

    # reporter
    reporter_api_id: Optional[str] = None
    reporter_email: Optional[str] = None
    reporter_name: Optional[str] = None
    reporter_dev_id: Optional[int] = None

    # assignee
    assignee_api_id: Optional[str] = None
    assignee_email: Optional[str] = None
    assignee_name: Optional[str] = None
    assignee_dev_id: Optional[int] = None

    labels: Optional[List[str]] = None

    # calculated fields

    is_started: Optional[bool] = None
    started_at: Optional[datetime] = None

    is_closed: Optional[bool] = None
    closed_at: Optional[datetime] = None

    comment_count: int = 0
    last_comment_at: Optional[datetime] = None
    change_count: int = 0
    last_change_at: Optional[datetime] = None

    # is_planned: Optional[bool] = None
    # sprint_count: int = 0

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue", "its_issues")


class ITSIssueChange(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int
    api_id: str

    author_api_id: Optional[str] = None
    author_email: Optional[str] = None
    author_name: Optional[str] = None
    author_dev_id: Optional[int] = None

    field_name: Optional[str] = None
    field_id: Optional[str] = None
    field_type: Optional[str] = None

    change_type: ITSIssueChangeType = ITSIssueChangeType.other

    v_from: Optional[str] = None
    v_from_string: Optional[str] = None
    v_to: Optional[str] = None
    v_to_string: Optional[str] = None

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_change", "its_issue_changes")


class ITSIssueTimeInStatus(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int
    status_name: str
    status_id: Optional[str] = None
    status_category_api: Optional[str] = None  # todo, inprogress, done
    status_category: Optional[ITSIssueStatusCategory] = None  # todo, inprogress, done

    started_issue_change_id: Optional[str] = None
    started_at: datetime
    ended_at: datetime
    ended_issue_change_id: Optional[str] = None
    ended_with_status_name: Optional[str] = None
    ended_with_status_id: Optional[str] = None
    seconds_in_status: int

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_time_in_status", "its_issue_times_in_status")


class ITSIssueComment(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int

    author_api_id: Optional[str] = None
    author_email: Optional[str] = None
    author_name: Optional[str] = None
    author_dev_id: Optional[int] = None

    comment: Optional[str] = None

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_comment", "its_issue_comments")


class ITSIssueLinkedIssue(StringIdModelMixin, ExtraFieldMixin, DateTimeModelMixin, CoreModel, ExportableModel):
    issue_id: str
    itsp_id: int

    linked_issue_id: str
    # link_type

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue_linked_issue", "its_issue_linked_issues")


class ITSIssueAllData(BaseModel):
    issue: ITSIssue
    comments: List[ITSIssueComment]
    changes: List[ITSIssueChange]
    times_in_statuses: List[ITSIssueTimeInStatus]
