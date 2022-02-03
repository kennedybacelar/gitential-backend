from typing import List
from structlog import get_logger
from gitential2.settings import IntegrationType
from gitential2.datatypes.its_projects import ITSProjectCreate, ITSProjectInDB
from gitential2.core.context import GitentialContext
from gitential2.core.credentials import list_credentials_for_workspace, get_fresh_credential

logger = get_logger(__name__)

ISSUE_SOURCES = [IntegrationType.jira]


def _merge_its_project_lists(first: List[ITSProjectCreate], second: List[ITSProjectCreate]) -> List[ITSProjectCreate]:
    existing_api_urls = [r.api_url for r in first]
    new_repos = [r for r in second if r.api_url not in existing_api_urls]
    return first + new_repos


def list_available_its_projects(g: GitentialContext, workspace_id: int) -> List[ITSProjectCreate]:
    # all_already_used_repositories = [RepositoryCreate(**r.dict()) for r in list_repositories(g, workspace_id)]
    # results: List[ITSProjectCreate] = all_already_used_its_projects
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
    return []
