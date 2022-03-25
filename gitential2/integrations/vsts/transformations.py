from typing import Callable, List, Tuple
from pydantic.datetime_parse import parse_datetime
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.its import (
    ITSIssueAllData,
    ITSIssue,
    ITSIssueComment,
    ITSIssueChange,
    ITSIssueTimeInStatus,
    ITSIssueChangeType,
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


def _transform_to_ITSIssueChange(
    its_issue_change_static_info: dict, its_project: ITSProjectInDB, single_update: dict, single_field: Tuple[str, dict]
) -> ITSIssueChange:

    field_name, field_content = single_field

    v_from_string = (
        field_content["oldValue"].get("displayName")
        if isinstance(field_content.get("oldValue"), dict)
        else field_content.get("oldValue")
    )

    v_to_string = (
        field_content["newValue"].get("displayName")
        if isinstance(field_content.get("newValue"), dict)
        else field_content.get("newValue")
    )

    return ITSIssueChange(
        id=1,  # Hardcoded - to be defined
        issue_id=single_update["workItemId"],
        itsp_id=its_project.id,
        api_id=single_update["id"],
        author_api_id=single_update["revisedBy"].get("id"),
        author_email=single_update["revisedBy"].get("uniqueName"),
        author_name=single_update["revisedBy"].get("displayName"),
        author_dev_id=its_issue_change_static_info["author_dev_id"],
        field_name=field_name,
        field_id=None,
        field_type=None,
        change_type=ITSIssueChangeType.other,
        v_from=str(field_content.get("oldValue")),
        v_from_string=v_from_string,
        v_to=str(field_content.get("newValue")),
        v_to_string=v_to_string,
        created_at=its_issue_change_static_info["created_at"],
        updated_at=its_issue_change_static_info["updated_at"],
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
