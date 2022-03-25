from typing import List, Tuple, Callable
from urllib.parse import urlparse
from email_validator import validate_email, EmailNotValidError
from pydantic.datetime_parse import parse_datetime

from gitential2.datatypes import RepositoryInDB
from gitential2.datatypes.authors import AuthorAlias
from gitential2.datatypes.its_projects import ITSProjectInDB
from gitential2.datatypes.its import ITSIssueStatusCategory


def _get_organization_and_project_from_its_project(its_project_namespace: str) -> Tuple[str, str]:
    if len(its_project_namespace.split("/")) == 2:
        splitted = its_project_namespace.split("/")
        return (splitted[0], splitted[1])
    raise ValueError(f"Don't know how to parse vsts {its_project_namespace} namespace")


def get_db_issue_id(issue_dict: dict, its_project: ITSProjectInDB) -> str:
    return f"{its_project.id}-{issue_dict['id']}"


def _parse_labels(labels: str) -> List[str]:
    if labels:
        return [label.strip() for label in labels.split(";")]
    return []


def _parse_status_category(status_category_api: str) -> ITSIssueStatusCategory:
    assignment_state_category_api_to_its = {
        "Proposed": "new",
        "InProgress": "in_progress",
        "Resolved": "done",
        "Completed": "done",
        "Removed": "done",
    }
    if status_category_api in assignment_state_category_api_to_its:
        return ITSIssueStatusCategory(assignment_state_category_api_to_its[status_category_api])
    return ITSIssueStatusCategory.unknown


def _its_ITSIssueChange_static_part(developer_map_callback: Callable, single_update: dict, created_date: str) -> dict:

    author_dev_id = developer_map_callback(to_author_alias(single_update.get("revisedBy")))
    created_at = parse_datetime(created_date)
    updated_at = parse_datetime(single_update["fields"]["System.ChangedDate"].get("newValue"))

    ret = {
        "author_dev_id": author_dev_id,
        "created_at": created_at,
        "updated_at": updated_at,
    }

    return ret


def _get_project_organization_and_repository(repository: RepositoryInDB) -> Tuple[str, str, str]:

    if repository.extra and "project" in repository.extra:
        repository_url = repository.extra["url"]
        return _parse_azure_repository_url(repository_url)
    else:
        return _parse_clone_url(repository.clone_url)


def _parse_azure_repository_url(url: str) -> Tuple[str, str, str]:
    parsed_url = urlparse(url)

    if parsed_url.hostname and parsed_url.path:
        _splitted_path = parsed_url.path.split("/")

        if "visualstudio.com" in parsed_url.hostname and "_apis/git/repositories" in parsed_url.path:
            # "https://ORGANIZATION_NAME.visualstudio.com/PROJECT_ID/_apis/git/repositories/REPOSITORY_ID"
            organization_name = parsed_url.hostname.split(".")[0]
            project_id = _splitted_path[1]
            repository_id = _splitted_path[-1]
            return organization_name, project_id, repository_id
        elif "dev.azure.com" in parsed_url.hostname:
            organization_name = _splitted_path[1]

    raise ValueError(f"Don't know how to parse AZURE Resource URL: {url}")


# pylint: disable=unused-argument
def _parse_clone_url(url: str) -> Tuple[str, str, str]:
    return ("", "", "")


def _paginate_with_skip_top(client, starting_url, top=100) -> list:
    ret: list = []
    skip = 0

    while True:
        url = starting_url + f"&$top={top}&$skip={skip}"
        resp = client.get(url)
        if resp.status_code != 200:
            return ret
        elif resp.status_code == 200:
            json_resp = resp.json()
            count = json_resp["count"]
            value = json_resp["value"]
            ret += value
            if count >= top:
                skip = skip + top
            else:
                return ret


def to_author_alias(raw_user):
    name = raw_user.get("displayName")
    uniq_name = raw_user.get("uniqueName")
    try:
        valid = validate_email(uniq_name)
        email = valid.email
        return AuthorAlias(name=name, email=email)
    except EmailNotValidError:
        return AuthorAlias(name=name, login=uniq_name)
