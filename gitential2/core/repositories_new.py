from typing import List, Optional

from gitential2.core import GitentialContext
from gitential2.datatypes import RepositoryCreate
from gitential2.utils import is_string_not_empty


def list_available_repositories_paginated(
    g: GitentialContext,
    workspace_id: int,
    user_id: int,
    limit: int,
    offset: int,
    user_organization_name_list: Optional[List[str]] = None,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search: Optional[str] = None,
) -> List[RepositoryCreate]:
    pass


def _get_query(
    workspace_id: int,
    user_id: int,
    limit: int,
    offset: int,
    user_organization_name_list: Optional[List[str]] = None,
    integration_type: Optional[str] = None,
    namespace: Optional[str] = None,
    credential_id: Optional[int] = None,
    search: Optional[str] = None,
):
    search_query = f"name ILIKE '{search}'" if is_string_not_empty(search) else ""

    query = (
        "("
        "SELECT "
        "    clone_url, "
        "    repo_provider_id, "
        "    protocol, "
        "    name, "
        "    namespace, "
        "    private, "
        "    integration_type, "
        "    integration_name, "
        "    credential_id "
        "FROM public.user_repositories_cache "
        f"   WHERE user_id = {user_id} "
        f"   {search_query if is_string_not_empty(search_query) else ''} "
        "UNION "
        "SELECT "
        "    clone_url, "
        "    COALESCE(CAST(r.extra::json -> 'uuid' AS TEXT), CAST(r.extra::json -> 'id' AS TEXT)) AS repo_provider_id, "
        "    protocol, "
        "    name, "
        "    namespace, "
        "    private, "
        "    integration_type, "
        "    integration_name, "
        "    credential_id "
        f"FROM ws_{workspace_id}.repositories r "
        f"   WHERE {search_query} "
        ") "
        "ORDER BY name "
        f"LIMIT {limit} "
        f"OFFSET {offset};"
    )
    pass
