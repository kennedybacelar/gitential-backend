from typing import List, Union, Optional
import traceback
from functools import partial
from datetime import datetime, timezone
from structlog import get_logger
from structlog.threadlocal import tmp_bind
from gitential2.datatypes.extraction import ExtractedKind
from gitential2.datatypes.its import ITSIssueAllData, ITSIssueHeader
from gitential2.datatypes.refresh import RefreshStrategy, RefreshType
from gitential2.datatypes.refresh_statuses import ITSProjectRefreshPhase, ITSProjectRefreshStatus
from gitential2.settings import IntegrationType
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import list_credentials_for_workspace, get_fresh_credential
from gitential2.core.authors import developer_map_callback


from gitential2.utils import find_first
from .credentials import (
    get_fresh_credential,
    list_credentials_for_workspace,
    get_update_token_callback,
)


logger = get_logger(__name__)

ISSUE_SOURCES = [IntegrationType.jira, IntegrationType.vsts]


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

                    userinfo: UserInfoInDB = (
                        find_first(
                            lambda ui: ui.integration_name
                            == credential.integration_name,  # pylint: disable=cell-var-from-loop
                            g.backend.user_infos.get_for_user(credential.owner_id),
                        )
                        if credential.owner_id
                        else None
                    )

                    collected_repositories = integration.list_available_its_projects(
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
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


def list_project_its_projects(g: GitentialContext, workspace_id: int, project_id: int) -> List[ITSProjectInDB]:
    ret = []
    for itsp_id in g.backend.project_its_projects.get_itsp_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        itsp = g.backend.its_projects.get(workspace_id=workspace_id, id_=itsp_id)
        if itsp:
            ret.append(itsp)
    return ret


SKIP_REFRESH_MSG = "Skipping ITS Project refresh"


def refresh_its_project(
    g: GitentialContext,
    workspace_id: int,
    itsp_id: int,
    strategy: RefreshStrategy = RefreshStrategy.parallel,
    refresh_type: RefreshType = RefreshType.everything,
    date_from: Optional[datetime] = None,
    force: bool = False,
):
    itsp = g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp_id)
    integration = g.integrations.get(itsp.integration_name)

    with tmp_bind(
        logger, workspace_id=workspace_id, itsp_id=itsp_id, itsp_name=itsp.name, integration_name=itsp.integration_name
    ) as log:

        if (not integration) or (not hasattr(integration, "list_recently_updated_issues")):
            log.warning(SKIP_REFRESH_MSG, reason="integration not configured or missing implementation")
            return

        if _is_refresh_already_running(g, workspace_id, itsp_id) and not force:
            log.warning(SKIP_REFRESH_MSG, reason="already running")
            return

        log.info(
            "Starting ITS project refresh",
            strategy=strategy,
            refresh_type=refresh_type,
        )
        update_itsp_status(g, workspace_id, itsp_id, phase=ITSProjectRefreshPhase.running)
        try:
            token = _get_fresh_token_for_itsp(g, workspace_id, itsp)
            if token:
                recently_updated_issues: List[ITSIssueHeader] = get_recently_updated_issues(
                    g, workspace_id, itsp_id, date_from=date_from, itsp=itsp
                )
                count_processed_items = 0
                update_itsp_status(
                    g,
                    workspace_id,
                    itsp_id,
                    count_recently_updated_items=len(recently_updated_issues),
                    count_processed_items=count_processed_items,
                )
                for ih in recently_updated_issues:
                    if force or _is_issue_new_or_updated(g, workspace_id, ih):
                        collect_and_save_data_for_issue(g, workspace_id, itsp=itsp, issue_id_or_key=ih.api_id)
                    else:
                        log.info("Issue is up-to-date", issue_api_id=ih.api_id, issue_key=ih.key)
                    count_processed_items += 1
                    update_itsp_status(g, workspace_id, itsp_id, count_processed_items=count_processed_items)
                    # TODO: increment count in status
            else:
                log.info(SKIP_REFRESH_MSG, workspace_id=workspace_id, itsp_id=itsp.id, reason="no fresh credential")

            update_itsp_status(g, workspace_id, itsp_id, phase=ITSProjectRefreshPhase.done)
        except:  # pylint: disable=bare-except
            update_itsp_status(
                g,
                workspace_id,
                itsp_id,
                phase=ITSProjectRefreshPhase.done,
                is_error=True,
                error_msg=traceback.format_exc(limit=1),
            )
            log.exception("Failed to refresh ITS Project")


def update_itsp_status(
    g: GitentialContext, workspace_id: int, itsp_id: int, phase: Optional[ITSProjectRefreshPhase] = None, **kwargs
):

    status = get_itsp_status(g, workspace_id, itsp_id)
    status_dict = status.dict()

    if phase == ITSProjectRefreshPhase.scheduled:
        status_dict["finished_at"] = None
        status_dict["started_at"] = None
        status_dict["scheduled_at"] = g.current_time()
        status_dict["phase"] = ITSProjectRefreshPhase.scheduled
    elif phase == ITSProjectRefreshPhase.running:
        status_dict["finished_at"] = None
        status_dict["started_at"] = g.current_time()
        status_dict["phase"] = ITSProjectRefreshPhase.running
    elif phase == ITSProjectRefreshPhase.done:
        status_dict["finished_at"] = g.current_time()
        status_dict["phase"] = ITSProjectRefreshPhase.done
        if not kwargs.get("is_error", False):
            status_dict["last_successful_at"] = status_dict["started_at"]
            status_dict["is_error"] = False
            status_dict["error_msg"] = None

    status_dict.update(**kwargs)
    return set_itsp_status(g, workspace_id, itsp_id, status_dict)


def _get_itsp_status_key(workspace_id: int, itsp_id: int) -> str:
    return f"ws-{workspace_id}:itsp-{itsp_id}"


def get_itsp_status(g: GitentialContext, workspace_id: int, itsp_id: int) -> ITSProjectRefreshStatus:
    key = _get_itsp_status_key(workspace_id, itsp_id)
    status_dict = g.kvstore.get_value(key)
    if status_dict and isinstance(status_dict, dict):
        return ITSProjectRefreshStatus(**status_dict)
    else:
        itsp = g.backend.its_projects.get_or_error(workspace_id, itsp_id)
        default_values = {
            "scheduled_at": None,
            "started_at": None,
            "finished_at": None,
            "last_successful_at": None,
            "phase": ITSProjectRefreshPhase.unknown,
            "count_recently_updated_items": 0,
            "count_processed_items": 0,
            "is_error": False,
            "error_msg": None,
        }
        status_dict = {"workspace_id": workspace_id, "id": itsp.id, "name": itsp.name, **default_values}
        g.kvstore.set_value(key, status_dict)
        return ITSProjectRefreshStatus(**status_dict)


def set_itsp_status(g: GitentialContext, workspace_id: int, itsp_id: int, status_dict: dict) -> ITSProjectRefreshStatus:
    key = _get_itsp_status_key(workspace_id, itsp_id)
    g.kvstore.set_value(key, status_dict)
    return ITSProjectRefreshStatus(**status_dict)


def get_recently_updated_issues(
    g: GitentialContext,
    workspace_id: int,
    itsp_id: int,
    date_from: Optional[datetime] = None,
    itsp: Optional[ITSProjectInDB] = None,
) -> List[ITSIssueHeader]:
    itsp = itsp or g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp_id)
    integration = g.integrations.get(itsp.integration_name)

    with tmp_bind(
        logger, workspace_id=workspace_id, itsp_id=itsp_id, itsp_name=itsp.name, integration_name=itsp.integration_name
    ) as log:
        if (not integration) or (not hasattr(integration, "list_recently_updated_issues")):
            log.warning(SKIP_REFRESH_MSG, reason="integration not configured or missing implementation")
            return []

        token = _get_fresh_token_for_itsp(g, workspace_id, itsp)
        if token:
            date_from = date_from or _get_last_successful_refresh_run(g, workspace_id, itsp_id)
            if not date_from:
                log.info("Getting all issues in the project")
                return integration.list_all_issues_for_project(token, itsp)
            log.info("Getting issues changed since", date_from=date_from)
            return integration.list_recently_updated_issues(token, itsp, date_from=date_from)
        else:
            log.info(SKIP_REFRESH_MSG, workspace_id=workspace_id, itsp_id=itsp.id, reason="no fresh credential")
            return []


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


def _is_refresh_already_running(g: GitentialContext, workspace_id: int, itsp_id: int) -> bool:
    status = get_itsp_status(g, workspace_id, itsp_id)
    return status.phase == ITSProjectRefreshPhase.running


def _get_last_successful_refresh_run(g: GitentialContext, workspace_id: int, itsp_id) -> Optional[datetime]:
    status = get_itsp_status(g, workspace_id, itsp_id)
    return status.last_successful_at


def collect_and_save_data_for_issue(
    g: GitentialContext,
    workspace_id: int,
    itsp: Union[ITSProjectInDB, int],
    issue_id_or_key: str,
    token: Optional[dict] = None,
):
    dev_map_callback = partial(developer_map_callback, g=g, workspace_id=workspace_id)
    itsp = (
        itsp
        if isinstance(itsp, ITSProjectInDB)
        else g.backend.its_projects.get_or_error(workspace_id=workspace_id, id_=itsp)
    )

    with tmp_bind(
        logger,
        workspace_id=workspace_id,
        itsp_id=itsp.id,
        itsp_name=itsp.name,
        integration_name=itsp.integration_name,
        issue_id_or_key=issue_id_or_key,
    ) as log:

        token = token or _get_fresh_token_for_itsp(g, workspace_id, itsp)
        if not token:
            log.info("Skipping issue data collection: no fresh credential")
            return

        integration = g.integrations.get(itsp.integration_name)
        if not integration:
            log.warning("Skipping issue data collection: integration not configured")
            return

        log.info("Starting collection of issue data")
        issue_data = integration.get_all_data_for_issue(token, itsp, issue_id_or_key, dev_map_callback)
        _save_collected_issue_data(g, workspace_id, issue_data)
        log.info(
            "Issue data saved",
            issue_id=issue_data.issue.id,
        )


def _save_collected_issue_data(g: GitentialContext, workspace_id: int, issue_data: ITSIssueAllData):
    output = g.backend.output_handler(workspace_id)
    output.write(ExtractedKind.ITS_ISSUE, issue_data.issue)
    for change in issue_data.changes:
        output.write(ExtractedKind.ITS_ISSUE_CHANGE, change)
    for time_in_status in issue_data.times_in_statuses:
        output.write(ExtractedKind.ITS_ISSUE_TIME_IN_STATUS, time_in_status)
    for comment in issue_data.comments:
        output.write(ExtractedKind.ITS_ISSUE_COMMENT, comment)
    for linked_issue in issue_data.linked_issues:
        output.write(ExtractedKind.ITS_ISSUE_LINKED_ISSUE, linked_issue)
    for sprint in issue_data.sprints:
        output.write(ExtractedKind.ITS_SPRINT, sprint)
    for issue_sprint in issue_data.issue_sprints:
        output.write(ExtractedKind.ITS_ISSUE_SPRINT, issue_sprint)
    for worklog in issue_data.worklogs:
        output.write(ExtractedKind.ITS_ISSUE_WORKLOG, worklog)
