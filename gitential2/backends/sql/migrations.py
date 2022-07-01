from typing import List, Optional
from pydantic import BaseModel
from sqlalchemy.engine import Engine
from structlog import get_logger

from gitential2.datatypes.common import CoreModel
from ..base.repositories import BaseRepository
from .repositories import SQLRepository
from .tables import schema_revisions_table, get_workspace_metadata


logger = get_logger(__name__)


class MigrationRevision(BaseModel):
    revision_id: str
    steps: List[str]


MigrationList = List[MigrationRevision]


class SchemaRevision(CoreModel):
    id: str
    revision_id: str


class SchemaRevisionRepository(BaseRepository[str, SchemaRevision, SchemaRevision, SchemaRevision]):
    pass


class SQLSchemaRevisionRepository(
    SchemaRevisionRepository, SQLRepository[str, SchemaRevision, SchemaRevision, SchemaRevision]
):
    pass


def public_schema_migrations() -> MigrationList:
    return [
        MigrationRevision(
            revision_id="000",
            steps=[
                # users
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id VARCHAR(256);",
                # subscriptions
                "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS stripe_subscription_id VARCHAR(256);",
                "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS features JSON;",
            ],
        )
    ]


def workspace_schema_migrations(schema_name: str) -> MigrationList:
    return [
        MigrationRevision(
            revision_id="000",
            steps=[
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_id_external VARCHAR(64);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_name_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_username_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_aid INTEGER;",
                # merged_by who?
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_id_external VARCHAR(64);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_name_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_username_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_aid INTEGER;",
                # calculated_patches & pull_requests & calculated_commits
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS loc_effort_p INTEGER;",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS is_bugfix BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS is_collaboration BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS is_new_code BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS is_bugfix BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_commits ADD COLUMN IF NOT EXISTS is_pr_exists BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_commits ADD COLUMN IF NOT EXISTS is_pr_open BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_commits ADD COLUMN IF NOT EXISTS is_pr_closed BOOLEAN;",
                # add extra indexes to calculated_patches and calculated_commits
                f"CREATE INDEX IF NOT EXISTS calculated_patches_date_idx ON {schema_name}.calculated_patches USING btree (date);",
                f"CREATE INDEX IF NOT EXISTS calculated_commits_date_idx ON {schema_name}.calculated_commits USING btree (date);",
            ],
        ),
        MigrationRevision(
            revision_id="001",
            steps=[
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN status_category_api TYPE VARCHAR(32);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN issue_type_name TYPE VARCHAR(48);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN issue_type_id TYPE VARCHAR(48);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN resolution_id TYPE VARCHAR(48);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN priority_name TYPE VARCHAR(32);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN priority_id TYPE VARCHAR(48);",
            ],
        ),
        MigrationRevision(
            revision_id="002",
            steps=[
                f"ALTER TABLE {schema_name}.charts ADD COLUMN IF NOT EXISTS filters JSON;",
            ],
        ),
        MigrationRevision(
            revision_id="003",
            steps=[
                f"ALTER TABLE {schema_name}.dashboards DROP COLUMN IF EXISTS config;",
                f"ALTER TABLE {schema_name}.dashboards ADD COLUMN IF NOT EXISTS filters JSON;",
            ],
        ),
        MigrationRevision(
            revision_id="004",
            steps=[
                f"ALTER TABLE {schema_name}.projects ADD COLUMN IF NOT EXISTS sprints_enabled BOOLEAN;",
                f"ALTER TABLE {schema_name}.projects ADD COLUMN IF NOT EXISTS sprint JSON;",
            ],
        ),
    ]


def migrate_database(engine: Engine, workspace_ids: List[int]):
    schema_revisions = SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    _do_migration("public", public_schema_migrations(), schema_revisions, engine)
    for workspace_id in workspace_ids:
        migrate_workspace(engine, workspace_id, _schema_revisions=schema_revisions)


def migrate_workspace(engine: Engine, workspace_id: int, _schema_revisions: Optional[SchemaRevisionRepository] = None):
    schema_revisions = _schema_revisions or SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    schema_name = f"ws_{workspace_id}"
    create_missing_workspace_tables(engine, schema_name)
    _do_migration(schema_name, workspace_schema_migrations(schema_name), schema_revisions, engine)


def create_missing_workspace_tables(engine: Engine, schema_name: str):
    workspace_metadata, _ = get_workspace_metadata(schema_name)
    workspace_metadata.create_all(engine)


def _do_migration(
    schema_name: str, migrations: MigrationList, schema_revisions: SchemaRevisionRepository, engine: Engine
):
    logger.info("Migration: Running database migration for schema", schema_name=schema_name)
    current_rev = schema_revisions.get(schema_name)
    if current_rev:
        revision_ids = [m.revision_id for m in migrations]
        rev_index = revision_ids.index(current_rev.revision_id)
        remaining_steps = migrations[rev_index + 1 :]
    else:
        remaining_steps = migrations

    if remaining_steps:
        for ms in remaining_steps:
            logger.info("Migration: applying step", schema_name=schema_name, revision_id=ms.revision_id)
            for query_ in ms.steps:
                logger.debug(
                    "Migrations: executing query", query=query_, schema_name=schema_name, revision_id=ms.revision_id
                )
                engine.execute(query_)
        new_rev = SchemaRevision(id=schema_name, revision_id=remaining_steps[-1].revision_id)
        if current_rev:
            schema_revisions.update(schema_name, new_rev)
        else:
            schema_revisions.create(new_rev)
        logger.info("Migrations: schema is updated to revision", schema_name=schema_name, new_rev=new_rev.revision_id)
    else:
        logger.info(
            "Migrations: schema is up-to-date",
            schema_name=schema_name,
            current_rev=current_rev.revision_id if current_rev else None,
        )


def get_latest_ws_revision():
    return [m.revision_id for m in workspace_schema_migrations("ws_1")][-1]


def set_schema_to_revision(schema_name: str, revision_id: str, engine: Engine):
    schema_revisions = SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    current_rev = schema_revisions.get(schema_name)
    new_rev = SchemaRevision(id=schema_name, revision_id=revision_id)
    if current_rev:
        schema_revisions.update(schema_name, new_rev)
    else:
        schema_revisions.create(new_rev)


def set_ws_migration_revision_after_create(workspace_id: int, engine: Engine):
    revision_id = get_latest_ws_revision()
    set_schema_to_revision(f"ws_{workspace_id}", revision_id, engine)
