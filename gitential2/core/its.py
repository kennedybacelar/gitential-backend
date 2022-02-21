from typing import List, Union, Optional
from functools import partial
from datetime import datetime, timezone
from structlog import get_logger
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.its import ITSIssueAllData, ITSIssueHeader
from gitential2.settings import IntegrationType
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import list_credentials_for_workspace, get_fresh_credential
from gitential2.core.authors import developer_map_callback


logger = get_logger(__name__)

ISSUE_SOURCES = [IntegrationType.jira]


def _merge_its_project_lists(first: List[ITSProjectCreate], second: List[ITSProjectCreate]) -> List[ITSProjectCreate]:
    existing_api_urls = [r.api_url for r in first]
    new_repos = [r for r in second if r.api_url not in existing_api_urls]
    return first + new_repos


def list_available_its_projects(g: GitentialContext, workspace_id: int) -> List[ITSProjectCreate]:
    results: List[ITSProjectCreate] = []
    for credential_ in list_credentials_for_workspace(g, workspace_id):
        if credential_.integration_type in ISSUE_SOURCES and credential_.integration_name in g.integrations:
            try:
                credential = get_fresh_credential(g, credential_id=credential_.id)
                if credential:
                    integration = g.integrations[credential.integration_name]
                    token = credential.to_token_dict(fernet=g.fernet)

                    collected_repositories = integration.list_available_its_projects(
                        token=token,
                    )
                    results = _merge_its_project_lists(collected_repositories, results)
                else:
                    logger.error(
                        "Cannot get fresh credential",
                        credential_id=credential_.id,
                        owner_id=credential_.owner_id,
                        integration_name=credential_.integration_name,
                    )
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "Error during collecting its projects",
                    integration_name=credential_.integration_name,
                    credential_id=credential_.id,
                    workspace_id=workspace_id,
                )

    return results


# pylint: disable=unused-argument
def list_project_its_projects(g: GitentialContext, workspace_id: int, project_id: int) -> List[ITSProjectInDB]:

    ret = []
    for itsp_id in g.backend.project_its_projects.get_itsp_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        itsp = g.backend.its_projects.get(workspace_id=workspace_id, id_=itsp_id)
        if itsp:
            ret.append(itsp)
    return ret


def refresh_its_project(
    g: GitentialContext, workspace_id: int, itsp_id: int, date_from: Optional[datetime] = None, force: bool = False
):
    itsp = g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp_id)
    integration = g.integrations.get(itsp.integration_name)
    if not hasattr(integration, "list_recently_updated_issues"):
        logger.warning(
            "Skipping ITS Project refresh: list_recently_updated_issues not implemented",
            workspace_id=workspace_id,
            itsp_id=itsp.id,
            integration=itsp.integration_name,
        )
        return
    if not integration:
        logger.warning(
            "Skipping ITS Project refresh: integration not configured",
            workspace_id=workspace_id,
            itsp_id=itsp.id,
            integration=itsp.integration_name,
        )
        return

    token = _get_fresh_token_for_itsp(g, workspace_id, itsp)
    if token:
        recently_updated_issues: List[ITSIssueHeader] = integration.list_recently_updated_issues(
            token, itsp, date_from=date_from
        )
        for ih in recently_updated_issues:
            if force or _is_issue_new_or_updated(g, workspace_id, ih):
                collect_and_save_data_for_issue(g, workspace_id, itsp=itsp, issue_id_or_key=ih.api_id)
            else:
                logger.info(
                    "Issue is up-to-date", workspace_id=workspace_id, itsp_id=itsp.id, issue_id_or_key=ih.api_id
                )
    else:
        logger.info("Skipping ITS Project refresh: no fresh credential", workspace_id=workspace_id, itsp_id=itsp.id)


def _is_issue_new_or_updated(g: GitentialContext, workspace_id: int, issue_header: ITSIssueHeader) -> bool:
    existing_ih = g.backend.its_issues.get_header(workspace_id, issue_header.id)
    if (
        existing_ih
        and existing_ih.updated_at
        and issue_header.updated_at
        and existing_ih.updated_at.replace(tzinfo=timezone.utc) == issue_header.updated_at.astimezone(timezone.utc)
    ):
        return False
    return True


def _get_fresh_token_for_itsp(g: GitentialContext, workspace_id: int, itsp: ITSProjectInDB) -> Optional[dict]:
    token = None
    credential = get_fresh_credential(
        g,
        credential_id=itsp.credential_id,
        workspace_id=workspace_id,
        integration_name=itsp.integration_name,
    )
    if credential:
        token = credential.to_token_dict(g.fernet)
    return token


def collect_and_save_data_for_issue(
    g: GitentialContext,
    workspace_id: int,
    itsp: Union[ITSProjectInDB, int],
    issue_id_or_key: str,
    token: Optional[dict] = None,
):
    itsp = (
        itsp
        if isinstance(itsp, ITSProjectInDB)
        else g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp)
    )

    token = token or _get_fresh_token_for_itsp(g, workspace_id, itsp)
    if not token:
        logger.info("Skipping issue data collection: no fresh credential", workspace_id=workspace_id, itsp_id=itsp.id)
        return

    integration = g.integrations.get(itsp.integration_name)
    if not integration:
        logger.warning(
            "Skipping issue data collection: integration not configured",
            workspace_id=workspace_id,
            itsp_id=itsp.id,
            integration=itsp.integration_name,
            issue_id_or_key=issue_id_or_key,
        )
        return
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)
    logger.info(
        "Starting collection of issue data",
        integration_name=itsp.integration_name,
        workspace_id=workspace_id,
        issue_id_or_key=issue_id_or_key,
    )
    issue_data = integration.get_all_data_for_issue(token, itsp, issue_id_or_key, dev_map_callback)
    _save_collected_issue_data(g, workspace_id, issue_data)


def _save_collected_issue_data(g: GitentialContext, workspace_id: int, issue_data: ITSIssueAllData):
    output = g.backend.output_handler(workspace_id)
    output.write(ExtractedKind.ITS_ISSUE, issue_data.issue)
    for change in issue_data.changes:
        output.write(ExtractedKind.ITS_ISSUE_CHANGE, change)
    for time_in_status in issue_data.times_in_statuses:
        output.write(ExtractedKind.ITS_ISSUE_TIME_IN_STATUS, time_in_status)
    for comment in issue_data.comments:
        output.write(ExtractedKind.ITS_ISSUE_COMMENT, comment)

    logger.info(
        "Issue data saved",
        workspace_id=workspace_id,
        issue_id=issue_data.issue.id,
    )
