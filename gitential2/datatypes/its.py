from datetime import datetime
from typing import Optional, Tuple
from .common import CoreModel, DateTimeModelMixin, ExtraFieldMixin, StringIdModelMixin
from .export import ExportableModel


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
    status_category: Optional[str] = None  # todo, inprogress, done

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

    def export_names(self) -> Tuple[str, str]:
        return ("its_issue", "its_issues")
