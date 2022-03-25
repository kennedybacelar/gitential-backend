from typing import Callable, List
from pydantic.datetime_parse import parse_datetime
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.its import (
    ITSIssueAllData,
    ITSIssue,
    ITSIssueComment,
    ITSIssueChange,
    ITSIssueTimeInStatus,
)

from .common import to_author_alias


def _transform_to_its_ITSIssueComment(
    comment_dict: dict, its_project: ITSProjectInDB, developer_map_callback: Callable
) -> ITSIssueComment:
    return ITSIssueComment(
        id=comment_dict["id"],
        issue_id=comment_dict["workItemId"],
        itsp_id=its_project.id,
        author_api_id=comment_dict["createdBy"].get("id"),
        author_email=comment_dict["createdBy"].get("uniqueName"),
        author_name=comment_dict["createdBy"].get("displayName"),
        author_dev_id=developer_map_callback(to_author_alias(comment_dict["createdBy"])),
        comment=comment_dict.get("text"),
        created_at=parse_datetime(comment_dict["createdDate"]),
        updated_at=parse_datetime(comment_dict["modifiedDate"]) if comment_dict.get("modifiedDate") else None,
    )


def _transform_to_its_ITSIssueAllData(
    issue: ITSIssue,
    comments: List[ITSIssueComment],
    changes: List[ITSIssueChange],
    times_in_statuses: List[ITSIssueTimeInStatus],
) -> ITSIssueAllData:
    return ITSIssueAllData(
        issue=issue,
        comments=comments,
        changes=changes,
        times_in_statuses=times_in_statuses,
    )
