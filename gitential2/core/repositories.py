from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from functools import partial
from typing import List, Optional

from dateutil.parser import parse as parse_date_str
from structlog import get_logger

from gitential2.datatypes.credentials import CredentialInDB
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryInDB, GitProtocol
from gitential2.datatypes.userinfos import UserInfoInDB
from gitential2.integrations import REPOSITORY_SOURCES
from gitential2.utils import levenshtein, find_first, is_list_not_empty, is_string_not_empty
from .context import GitentialContext
from .credentials import (
    get_fresh_credential,
    list_credentials_for_workspace,
    get_update_token_callback,
)
from ..datatypes.user_repositories_cache import UserRepositoryCacheInDB, UserRepositoryCacheCreate, UserRepositoryGroup

logger = get_logger(__name__)


def get_repository(g: GitentialContext, workspace_id: int, repository_id: int) -> Optional[RepositoryInDB]:
    return g.backend.repositories.get(workspace_id, repository_id)


def list_available_repositories(
    g: GitentialContext, workspace_id: int, user_id: int, user_organization_name_list: Optional[List[str]]
) -> List[RepositoryCreate]:
    def _merge_repo_lists(first: List[RepositoryCreate], second: List[RepositoryCreate]):
        existing_clone_urls = [r.clone_url for r in first]
        new_repos = [r for r in second if r.clone_url not in existing_clone_urls]
        return first + new_repos

    # Get all already used repositories
    results: List[RepositoryCreate] = [RepositoryCreate(**r.dict()) for r in list_repositories(g, workspace_id)]

    available_repos_for_credential = partial(
        list_available_repositories_for_credential, g, workspace_id, user_id, user_organization_name_list
    )
    with ThreadPoolExecutor() as executor:
        collected_results = executor.map(
            available_repos_for_credential, list_credentials_for_workspace(g, workspace_id)
        )

    for collected_repositories in collected_results:
        results = _merge_repo_lists(collected_repositories, results)

    results = _merge_repo_lists(list_ssh_repositories(g, workspace_id), results)

    logger.debug(
        "list_of_all_user_repositories",
        number_of_all_user_repositories=len(results),
        repo_clone_urls=[
            repo.dict().get("clone_url", None)
            for repo in results
            if repo is not None and is_string_not_empty(repo.clone_url)
        ]
        if is_list_not_empty(results)
        else "No repos found!",
    )

    return results


def list_available_repositories_for_credential(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    user_organization_name_list: Optional[List[str]],
    credential: CredentialInDB,
) -> List[RepositoryCreate]:
    results: List[RepositoryCreate] = []

    if credential.integration_type in REPOSITORY_SOURCES and credential.integration_name in g.integrations:
        try:
            credential_ = get_fresh_credential(g, credential_id=credential.id)
            if credential_:
                # with acquire_credential(g, credential_id=credential_.id) as credential:
                integration = g.integrations[credential_.integration_name]
                token = credential_.to_token_dict(fernet=g.fernet)
                userinfo: UserInfoInDB = (
                    find_first(
                        lambda ui: ui.integration_name
                        == credential.integration_name,  # pylint: disable=cell-var-from-loop
                        g.backend.user_infos.get_for_user(credential.owner_id),
                    )
                    if credential.owner_id
                    else None
                )

                refresh = _get_repos_last_refresh_date(
                    g=g, user_id=user_id, integration_type=credential.integration_type
                )
                if isinstance(refresh, datetime):
                    # If the last refresh date older than 1 day -> get new repos since last refresh date
                    if (g.current_time() - timedelta(days=1)) > refresh:
                        new_repos: List[RepositoryCreate] = integration.get_newest_repos_since_last_refresh(
                            token=token,
                            update_token=get_update_token_callback(g, credential),
                            last_refresh=refresh,
                            provider_user_id=userinfo.sub if userinfo else None,
                            user_organization_names=user_organization_name_list,
                        )

                        _save_repos_to_repos_cache(g=g, user_id=user_id, repo_list=new_repos)
                        _save_repos_last_refresh_date(
                            g=g, user_id=user_id, integration_type=credential.integration_type
                        )

                        logger.debug(
                            "Saved new repositories to cache.",
                            integration_type=credential.integration_type,
                            new_repos=[getattr(r, "clone_url", None) for r in new_repos]
                            if is_list_not_empty(new_repos)
                            else [],
                        )

                    results = _get_repos_cache(g=g, user_id=user_id)
                else:
                    # no last refresh date found -> list all available repositories
                    results = integration.list_available_private_repositories(
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
                        user_organization_name_list=user_organization_name_list,
                    )
                    _save_repos_to_repos_cache(g=g, user_id=user_id, repo_list=results)
                    _save_repos_last_refresh_date(g=g, user_id=user_id, integration_type=credential.integration_type)

                logger.debug(
                    "collected_private_repositories",
                    integration_name=credential_.integration_name,
                    number_of_collected_private_repositories=len(results),
                )
            else:
                logger.error(
                    "Cannot get fresh credential",
                    credential_id=credential.id,
                    owner_id=credential.owner_id,
                    integration_name=credential.integration_name,
                )
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                "Error during collecting repositories",
                integration_name=credential.integration_name,
                credential_id=credential.id,
                workspace_id=workspace_id,
            )
    return results


def list_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryInDB]:
    all_projects = g.backend.projects.all(workspace_id)
    project_ids = [project.id for project in all_projects]

    repos = {}

    # HACK: Needed for ssh repositories
    for repo in g.backend.repositories.all(workspace_id=workspace_id):
        if repo.protocol == GitProtocol.ssh:
            repos[repo.id] = repo

    for project_id in project_ids:
        for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
            workspace_id=workspace_id, project_id=project_id
        ):
            if repo_id not in repos:
                repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repo_id)
                if repository:
                    repos[repo_id] = repository
    return list(repos.values())


def list_ssh_repositories(g: GitentialContext, workspace_id: int) -> List[RepositoryCreate]:
    all_repositories = list_repositories(g, workspace_id)
    return [
        RepositoryCreate(**repo.dict())
        for repo in all_repositories
        if repo.credential_id is not None and repo.protocol == GitProtocol.ssh
    ]


def list_project_repositories(g: GitentialContext, workspace_id: int, project_id: int) -> List[RepositoryInDB]:
    ret = []
    for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        repository = g.backend.repositories.get(workspace_id=workspace_id, id_=repo_id)
        if repository:
            ret.append(repository)
    return ret


def search_public_repositories(g: GitentialContext, workspace_id: int, search: str) -> List[RepositoryCreate]:
    results: List[RepositoryCreate] = []

    for credential_ in list_credentials_for_workspace(g, workspace_id):
        if credential_.integration_type in REPOSITORY_SOURCES and credential_.integration_name in g.integrations:
            userinfo: UserInfoInDB = find_first(
                lambda ui: ui.integration_name == credential_.integration_name,  # pylint: disable=cell-var-from-loop
                g.backend.user_infos.get_for_user(credential_.owner_id),
            )
            try:
                # with acquire_credential(g, credential_id=credential_.id) as credential:
                credential = get_fresh_credential(g, credential_id=credential_.id)
                if credential:
                    integration = g.integrations[credential.integration_name]
                    token = credential.to_token_dict(fernet=g.fernet)
                    results += integration.search_public_repositories(
                        query=search,
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
                    )
                else:
                    logger.error(
                        "Cannot get fresh credential",
                        credential_id=credential_.id,
                        owner_id=credential_.owner_id,
                        integration_name=credential_.integration_name,
                    )
            except:  # pylint: disable=bare-except
                logger.exception("Error during public repo search")

    return sorted(results, key=lambda i: levenshtein(search, i.name))


def create_repositories(
    g: GitentialContext, workspace_id: int, repository_creates: List[RepositoryCreate]
) -> List[RepositoryInDB]:
    return [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id, repository_create)
        for repository_create in repository_creates
    ]


def delete_repositories(g: GitentialContext, workspace_id: int, repository_ids: List[int]):
    for project in g.backend.projects.all(workspace_id):
        g.backend.project_repositories.remove_repo_ids_from_project(workspace_id, project.id, repository_ids)

    for repo_id in repository_ids:
        g.backend.repositories.delete(workspace_id, repo_id)

    return True


def list_available_repo_groups(g: GitentialContext, workspace_id: int, user_id: int) -> List[UserRepositoryGroup]:
    repo_groups_from_cache = g.backend.user_repositories_cache.get_repo_groups(user_id=user_id)
    repo_groups = g.backend.repositories.get_repo_groups(workspace_id=workspace_id)

    def is_repo_group_in_cache(user_repo_group: UserRepositoryGroup):
        return any(
            gc.integration_type == user_repo_group.integration_type
            and gc.namespace == user_repo_group.namespace
            and gc.credential_id == user_repo_group.credential_id
            for gc in repo_groups_from_cache
        )

    for group in repo_groups:
        if not is_repo_group_in_cache(group):
            repo_groups_from_cache.append(group)

    return repo_groups_from_cache


def _save_repos_to_repos_cache(g: GitentialContext, user_id: int, repo_list: List[RepositoryCreate]):
    def get_repo_provider_id(repo: RepositoryCreate) -> Optional[str]:
        result = None
        if isinstance(repo.extra, dict):
            if "id" in repo.extra:
                result = str(repo.extra["id"])
            elif "uuid" in repo.extra:
                result = repo.extra["uuid"]
        return result

    repos_to_cache: List[UserRepositoryCacheCreate] = [
        UserRepositoryCacheCreate(
            user_id=user_id,
            repo_provider_id=get_repo_provider_id(repo),
            clone_url=repo.clone_url,
            protocol=repo.protocol,
            name=repo.name,
            namespace=repo.namespace,
            private=repo.private,
            integration_type=repo.integration_type,
            integration_name=repo.integration_name,
            credential_id=repo.credential_id,
            extra=repo.extra,
        )
        for repo in repo_list
    ]
    g.backend.user_repositories_cache.insert_repositories_cache_for_user(repos_to_cache)


def _get_repos_last_refresh_kvstore_key(user_id: int, integration_type: str):
    return f"repository_cache_for_user_last_refresh_datetime--{integration_type}--{user_id}"


def _get_repos_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str) -> Optional[datetime]:
    result = None
    redis_key = _get_repos_last_refresh_kvstore_key(user_id, integration_type)
    refresh_raw = g.kvstore.get_value(redis_key)
    if is_string_not_empty(refresh_raw):
        try:
            result = parse_date_str(refresh_raw).replace(tzinfo=timezone.utc)
        except ValueError:
            logger.debug(f"Last refresh date is invalid for user_id: {user_id}")
    return result


def _save_repos_last_refresh_date(g: GitentialContext, user_id: int, integration_type: str):
    refresh_save = str(datetime.utcnow())
    redis_key = _get_repos_last_refresh_kvstore_key(user_id, integration_type)
    g.kvstore.set_value(redis_key, refresh_save)


def _get_repos_cache(g: GitentialContext, user_id: int) -> List[RepositoryCreate]:
    collected_repositories_cache: List[
        UserRepositoryCacheInDB
    ] = g.backend.user_repositories_cache.get_all_repositories_for_user(user_id)
    return [
        RepositoryCreate(
            clone_url=repo.clone_url,
            protocol=repo.protocol,
            name=repo.name,
            namespace=repo.namespace,
            private=repo.private,
            integration_type=repo.integration_type,
            integration_name=repo.integration_name,
            extra=repo.extra,
        )
        for repo in collected_repositories_cache
    ]
