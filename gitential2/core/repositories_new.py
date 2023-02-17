from enum import Enum
from typing import List, Optional, Union

from sqlalchemy import exc
from structlog import get_logger

from gitential2.core import GitentialContext
from gitential2.datatypes import RepositoryCreate
from gitential2.exceptions import SettingsException
from gitential2.utils import is_string_not_empty

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
) -> List[RepositoryCreate]:
    pass


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
):
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
        results = g.backend.execute_query(query)
    except exc.SQLAlchemyError as se:
        raise SettingsException(
            "Exception while trying to run query to get list of repositories paginated result!"
        ) from se

    return results


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
