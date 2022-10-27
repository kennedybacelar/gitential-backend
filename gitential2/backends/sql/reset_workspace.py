from typing import Optional, List

from sqlalchemy import exc
from sqlalchemy.engine import Engine
from structlog import get_logger

from ...exceptions import SettingsException
from ...utils import get_schema_name, is_string_not_empty, is_list_not_empty

logger = get_logger(__name__)


def __get_reset_workspace_query(workspace_id: int) -> Optional[str]:
    schema_name: str = get_schema_name(workspace_id)
    return (
        (
            "DO LANGUAGE PLPGSQL $$ "
            "  DECLARE "
            "    row RECORD; "
            "BEGIN "
            "  FOR row IN "
            "    SELECT tables.table_name "
            "    FROM information_schema.tables tables "
            f"   WHERE table_schema = '{schema_name}' "
            "    LOOP "
            f"     EXECUTE FORMAT('TRUNCATE TABLE {schema_name}.%%I RESTART IDENTITY CASCADE;', row.table_name); "
            "    END LOOP; "
            "  END; "
            "$$;"
        )
        if is_string_not_empty(schema_name)
        else None
    )


def remove_workspace_keys_from_kvstore(gitential_context, workspace_id: int):
    # Type of gitential_context is not defined because it would cause a cyclic import error

    logger.info("Starting to remove keys from key-value store related to workspace.", workspace_id=workspace_id)

    repo_ids: List[int] = gitential_context.backend.repositories.all_ids(workspace_id=workspace_id)

    keys_to_remove: List[str] = [f"active-authors-{workspace_id}", f"authors_ws_{workspace_id}"]

    if is_list_not_empty(repo_ids):
        for rid in repo_ids:
            keys_to_remove.append(f"ws-{workspace_id}:repository-refresh-{rid}")
            keys_to_remove.append(f"ws-{workspace_id}:r-{rid}:extraction")
            keys_to_remove.append(f"ws-{workspace_id}:repository-status-{rid}")

    for key in keys_to_remove:
        value = gitential_context.kvstore.get_value(key)
        logger.info("Attempting to delete key from key-value store.", key=key, value=value)
        gitential_context.kvstore.delete_value(key)


def reset_workspace(engine: Engine, workspace_id: int):
    reset_workspace_query = __get_reset_workspace_query(workspace_id)
    if reset_workspace_query:
        connection = engine.connect()
        trans = connection.begin()
        try:
            logger.info("Executing query for reset workspace.", query=reset_workspace_query)
            connection.execute(reset_workspace_query)
            trans.commit()
        except exc.SQLAlchemyError as se:
            trans.rollback()
            raise SettingsException("Exception in database migration!") from se
    else:
        logger.exception(
            "Can not execute query for reset database!", query=reset_workspace_query, workspace_id=workspace_id
        )
