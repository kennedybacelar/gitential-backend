from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import partial
from typing import List, Optional, Union, Tuple

from dateutil.parser import parse as parse_date_str
from sqlalchemy import exc
from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes import RepositoryCreate, CredentialInDB, UserInfoInDB
from gitential2.exceptions import SettingsException
from gitential2.integrations import REPOSITORY_SOURCES
from gitential2.utils import find_first, is_list_not_empty, is_string_not_empty
from .credentials import (
    get_fresh_credential,
    list_credentials_for_workspace,
    get_update_token_callback,
)
from ..datatypes.user_repositories_cache import UserRepositoryCacheInDB, UserRepositoryCacheCreate, UserRepositoryPublic

logger = get_logger(__name__)


class OrderByOptions(str, Enum):
    name = "name"
    namespace = "namespace"
    protocol = "protocol"
    clone_url = "clone_url"
    integration_type = "integration_type"
    integration_name = "integration_name"


class OrderByDirections(str, Enum):
    asc = "ASC"
    desc = "DESC"


def list_available_repositories_paginated(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    user_organization_name_list: Optional[List[str]] = None,
    limit: Optional[int] = 15,
    offset: Optional[int] = 0,
    order_by_option: Optional[OrderByOptions] = OrderByOptions.name,
    order_by_direction: Optional[OrderByDirections] = OrderByDirections.asc,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> Tuple[int, List[UserRepositoryPublic]]:
    _refresh_repos_cache(g, workspace_id, user_id, user_organization_name_list)
    return __get_repos(
        g,
        workspace_id,
        user_id,
        limit,
        offset,
        order_by_option,
        order_by_direction,
        integration_type,
        namespace,
        credential_id,
        search_pattern,
    )


def __get_repos(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    limit: Optional[int] = 15,
    offset: Optional[int] = 0,
    order_by_option: Optional[OrderByOptions] = OrderByOptions.name,
    order_by_direction: Optional[OrderByDirections] = OrderByDirections.asc,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> Tuple[int, List[UserRepositoryPublic]]:
    query: str = __get_query_of_get_repositories(
        workspace_id=workspace_id,
        user_id=user_id,
        limit=limit,
        offset=offset,
        order_by_option=order_by_option,
        order_by_direction=order_by_direction,
        integration_type=integration_type,
        namespace=namespace,
        credential_id=credential_id,
        search_pattern=search_pattern,
    )

    try:
        logger.info("Executing query to get list of repositories paginated result.", query=query)
        rows = g.backend.execute_query(query)
    except exc.SQLAlchemyError as se:
        raise SettingsException(
            "Exception while trying to run query to get list of repositories paginated result!"
        ) from se

    repositories = (
        [
            UserRepositoryPublic(
                clone_url=row["clone_url"],
                repo_provider_id=row["repo_provider_id"],
                protocol=row["protocol"],
                name=row["name"],
                namespace=row["namespace"],
                private=row["private"],
                integration_type=row["integration_type"],
                integration_name=row["integration_name"],
                credential_id=row["credential_id"],
            )
            for row in rows
        ]
        if is_list_not_empty(rows)
        else []
    )

    total_count = rows[0]["count"] if is_list_not_empty(rows) else 0

    return total_count, repositories


def __get_query_of_get_repositories(
    workspace_id: int,
    user_id: int,
    limit: Optional[int] = 15,
    offset: Optional[int] = 0,
    order_by_option: Optional[OrderByOptions] = OrderByOptions.name,
    order_by_direction: Optional[OrderByDirections] = OrderByDirections.asc,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search_pattern: Optional[str] = None,
) -> str:
    def get_filter(column_name: str, filter_value: Union[str, int, None]):
        return f"{column_name} = '{filter_value}'" if filter_value else None

    limit = limit if 0 < limit < 50 else 15
    offset = offset if -1 < limit else 0

    name_filter = f"name ILIKE '{search_pattern}'" if is_string_not_empty(search_pattern) else None
    integration_type_filter = get_filter("integration_type", integration_type)
    namespace_filter = get_filter("namespace", namespace)
    credential_id_filter = get_filter("credential_id", credential_id)

    filters: str = " AND ".join(
        [
            f
            for f in [name_filter, integration_type_filter, namespace_filter, credential_id_filter]
            if is_string_not_empty(f)
        ]
    )

    get_repo_uuid = "CAST(r.extra::json -> 'uuid' AS TEXT)"
    get_repo_id = "CAST(r.extra::json -> 'id' AS TEXT)"
    repo_provider_id = f"COALESCE({get_repo_uuid}, {get_repo_id})"
    repo_provider_id_trimmed = f"TRIM(BOTH '\"' FROM {repo_provider_id}) AS repo_provider_id, "

    query = (
        "("
        "    SELECT "
        "        clone_url, "
        "        repo_provider_id, "
        "        protocol, "
        "        name, "
        "        namespace, "
        "        private, "
        "        integration_type, "
        "        integration_name, "
        "        credential_id "
        "    FROM public.user_repositories_cache "
        f"       WHERE {filters} AND user_id = {user_id}"
        "    UNION "
        "    SELECT "
        "        clone_url, "
        f"       {repo_provider_id_trimmed}"
        "        protocol, "
        "        name, "
        "        namespace, "
        "        private, "
        "        integration_type, "
        "        integration_name, "
        "        credential_id "
        f"    FROM ws_{workspace_id}.repositories r "
        f"       WHERE {filters}"
        ") "
        f"ORDER BY {order_by_option} {order_by_direction}"
        f"LIMIT {limit} "
        f"OFFSET {offset};"
    )

    return query


def _refresh_repos_cache(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    user_organization_name_list: Optional[List[str]],
):
    credentials_for_workspace: List[CredentialInDB] = list_credentials_for_workspace(g, workspace_id)
    repos_for_credential = partial(
        _refresh_repos_cache_for_credential, g, workspace_id, user_id, user_organization_name_list
    )
    with ThreadPoolExecutor() as executor:
        executor.map(repos_for_credential, credentials_for_workspace)


def _refresh_repos_cache_for_credential(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    user_organization_name_list: Optional[List[str]],
    credential: CredentialInDB,
):
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
                        repos_newly_created: List[RepositoryCreate] = integration.get_newest_repos_since_last_refresh(
                            token=token,
                            update_token=get_update_token_callback(g, credential),
                            last_refresh=refresh,
                            provider_user_id=userinfo.sub if userinfo else None,
                            user_organization_names=user_organization_name_list,
                        )
                        _save_repos_to_repos_cache(g=g, user_id=user_id, repo_list=repos_newly_created)
                        _save_repos_last_refresh_date(
                            g=g, user_id=user_id, integration_type=credential.integration_type
                        )

                        logger.debug(
                            "Saved new repositories to cache.",
                            integration_type=credential.integration_type,
                            new_repos=[getattr(r, "clone_url", None) for r in repos_newly_created]
                            if is_list_not_empty(repos_newly_created)
                            else [],
                        )
                else:
                    # no last refresh date found -> list all available repositories
                    repos_all = integration.list_available_private_repositories(
                        token=token,
                        update_token=get_update_token_callback(g, credential),
                        provider_user_id=userinfo.sub if userinfo else None,
                        user_organization_name_list=user_organization_name_list,
                    )
                    _save_repos_to_repos_cache(g=g, user_id=user_id, repo_list=repos_all)
                    _save_repos_last_refresh_date(g=g, user_id=user_id, integration_type=credential.integration_type)
                    logger.debug(
                        "collected_private_repositories",
                        integration_name=credential_.integration_name,
                        number_of_collected_private_repositories=len(repos_all),
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
