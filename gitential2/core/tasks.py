from typing import Optional
from celery import Celery
from structlog import get_logger
from gitential2.settings import GitentialSettings, load_settings
from gitential2.logging import initialize_logging

logger = get_logger(__name__)

celery_app = Celery()


def configure_celery(settings: Optional[GitentialSettings] = None):
    settings = settings or load_settings()
    initialize_logging(settings)
    global celery_app  # pylint: disable=global-statement
    celery_app.conf.update(
        result_expires=120,
        task_acks_late=True,
        broker_url=settings.celery.broker_url or settings.connections.redis_url,
        result_backend=settings.celery.result_backend_url or settings.connections.redis_url,
        imports=("gitential2.core.tasks",),
    )
    return celery_app


@celery_app.task
def ping(settings_dict: dict):
    return GitentialSettings(**settings_dict).dict()


@celery_app.task
def refresh_repository_task(settings_dict: dict, workspace_id: int, repository_id: int, force_rebuild: bool):
    # pylint: disable=import-outside-toplevel,cyclic-import
    from gitential2.core import (
        init_context_from_settings,
        refresh_repository,
    )

    settings = GitentialSettings(**settings_dict)
    g = init_context_from_settings(settings)

    refresh_repository(g, workspace_id, repository_id, force_rebuild)
    logger.info("refreshing repository", repository_id=repository_id, workspace_id=workspace_id)


__all__ = ["configure_celery", "ping", "refresh_repository_task"]
