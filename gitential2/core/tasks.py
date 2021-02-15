from typing import Optional
from celery import Celery

from gitential2.settings import GitentialSettings, load_settings


celery_app = Celery()


def configure_celery(settings: Optional[GitentialSettings] = None):
    settings = settings or load_settings()
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
def refresh_repository(settings_dict: dict, workspace_id: int, repository_id: int):
    from gitential2.core import init_from_settings  # pylint: disable=import-outside-toplevel,cyclic-import

    settings = GitentialSettings(**settings_dict)
    gitential = init_from_settings(settings)
    workspace_ctrl = gitential.get_workspace_ctrl(workspace_id)
    workspace_ctrl.refresh_repository(repository_id)
    print(f"refreshing repository {repository_id} in workspace {workspace_id}")


__all__ = ["configure_celery", "ping", "refresh_repository"]
