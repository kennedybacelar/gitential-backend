from collections import namedtuple
from typing import Callable, Optional, Dict, Union
from importlib import import_module

from celery import Celery
from celery.app.task import Task
from celery.schedules import crontab
from structlog import get_logger
from gitential2.settings import GitentialSettings, load_settings
from gitential2.logging import initialize_logging
from gitential2.datatypes.refresh import (
    ExtractProjectBranchesParams,
    ExtractRepositoryBranchesParams,
    RefreshITSProjectParams,
    RefreshProjectParams,
    RefreshRepositoryParams,
    RefreshWorkspaceParams,
    MaintainWorkspaceParams,
)

from gitential2.exceptions import LockError

from .context import GitentialContext

logger = get_logger(__name__)


# pylint: disable=abstract-method
class CeleryTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.exception("Unhandled exception in task", task_id=task_id, exc=exc, args=args, kwargs=kwargs)
        return super().on_failure(exc, task_id, args, kwargs, einfo)


celery_app = Celery(task_cls="gitential2.core.tasks:CeleryTask")


# pylint: disable=unused-argument
def schedule_task(
    g: GitentialContext,
    task_name: str,
    params: Dict[str, Union[int, str, float, bool]],
    countdown: Optional[float] = None,
):
    # pylint: disable=import-outside-toplevel

    logger.info("Scheduling core task", task_name=task_name, params=params)
    core_task.apply_async(kwargs={"task_name": task_name, "params": params}, countdown=countdown)


def configure_celery(settings: Optional[GitentialSettings] = None):
    settings = settings or load_settings()
    initialize_logging(settings)
    # global celery_app  # pylint: disable=global-statement,
    celery_app.conf.update(
        result_expires=120,
        task_acks_late=True,
        worker_prefetch_multiplier=settings.celery.worker_prefetch_multiplier,
        worker_max_tasks_per_child=settings.celery.worker_max_tasks_per_child,
        worker_max_memory_per_child=settings.celery.worker_max_memory_per_child,
        broker_url=settings.celery.broker_url or settings.connections.redis_url,
        result_backend=settings.celery.result_backend_url or settings.connections.redis_url,
        imports=("gitential2.core.tasks",),
    )

    # beat_scheduled_conf = {}

    # TEMPORARY DISABLED
    # beat_scheduled_conf["send_scheduled_emails"] = {
    #     "task": "gitential2.core.tasks.send_scheduled_emails",
    #     "schedule": crontab(minute=0, hour=12),
    #     "args": (settings),
    # }

    # TEMPORARY DISABLED
    # if settings.refresh.hourly_maintenance_enabled:
    #     beat_scheduled_conf["hourly_maintenance"] = {
    #         "task": "gitential2.core.tasks.hourly_maintenance",
    #         "schedule": crontab(hour=23),
    #         "args": (),
    #     }
    # if settings.features.enable_additional_materialized_views:
    #     beat_scheduled_conf["refresh_materialized_views"] = {
    #         "task": "gitential2.core.tasks.refresh_materialized_views",
    #         "schedule": crontab(hour=23),
    #         "args": (),
    #     }

    # celery_app.conf.beat_schedule = beat_scheduled_conf
    return celery_app


@celery_app.task
def ping(settings_dict: dict):
    return GitentialSettings(**settings_dict).dict()


_gitential_context: Optional[GitentialContext] = None


CoreFunction = namedtuple("CoreFunction", ["module_name", "function_name"])

available_core_tasks = {
    "maintain_workspace": (MaintainWorkspaceParams, CoreFunction("gitential2.core.refresh_v2", "maintain_workspace")),
    "refresh_workspace": (RefreshWorkspaceParams, CoreFunction("gitential2.core.refresh_v2", "refresh_workspace")),
    "refresh_project": (RefreshProjectParams, CoreFunction("gitential2.core.refresh_v2", "refresh_project")),
    "refresh_repository": (RefreshRepositoryParams, CoreFunction("gitential2.core.refresh_v2", "refresh_repository")),
    "refresh_its_project": (RefreshITSProjectParams, CoreFunction("gitential2.core.its", "refresh_its_project")),
    "extract_project_branches": (
        ExtractProjectBranchesParams,
        CoreFunction("gitential2.core.refresh_v2", "extract_project_branches"),
    ),
    "extract_repository_branches": (
        ExtractRepositoryBranchesParams,
        CoreFunction("gitential2.core.refresh_v2", "extract_repository_branches"),
    ),
}


def import_core_function(core_function: CoreFunction) -> Callable:
    module = import_module(core_function.module_name)
    return getattr(module, core_function.function_name)


@celery_app.task
def core_task(task_name: str, params: Dict[str, Union[int, str, float, bool]]):
    global _gitential_context  # pylint: disable=global-statement
    try:
        if not _gitential_context:
            # pylint: disable=import-outside-toplevel,cyclic-import
            from gitential2.core.context import init_context_from_settings

            _gitential_context = init_context_from_settings(load_settings())

        if task_name in available_core_tasks:
            logger.info("Preparing core task", task_name=task_name, params=params)
            params_model, core_function = available_core_tasks[task_name]
            validated_params = params_model(**params)
            fn = import_core_function(core_function)
            logger.info("Starting core task", task_name=task_name, params=params)
            result = fn(g=_gitential_context, **validated_params.dict())
            logger.info("Core task finished", task_name=task_name, params=params, result=result)
        else:
            logger.error("Unknown core task", task_name=task_name)

    except LockError:
        logger.warning("Failed to get a lock, rescheduling core task")
        if _gitential_context:
            schedule_task(_gitential_context, task_name, params=params, countdown=60)
    except:  # pylint: disable=bare-except
        logger.exception("Failed core task")


@celery_app.task
def send_scheduled_emails(settings: Optional[GitentialSettings] = None):
    # pylint: disable=import-outside-toplevel,cyclic-import
    from gitential2.core import init_context_from_settings
    from gitential2.core.users import send_trial_end_soon_emails, send_trial_ended_emails, send_getting_started_emails

    settings = settings or load_settings()
    g = init_context_from_settings(settings)
    for s in g.backend.email_log.get_emails_to_send():
        if s.template_name == "free_trial_expiration":
            send_trial_end_soon_emails(g, s.user_id)
        elif s.template_name == "free_trial_ended":
            g.backend.email_log.email_log_status_update(s.user_id, "free_trial_ended", "sent")
            send_trial_ended_emails(g, s.user_id)
        else:
            send_getting_started_emails(g, s.user_id, s.template_name)
        g.backend.email_log.email_log_status_update(s.user_id, s.template_name, "sent")


__all__ = ["schedule_task", "core_task", "configure_celery", "ping", "send_scheduled_emails"]


@celery_app.task
def hourly_maintenance(settings: Optional[GitentialSettings] = None):
    # pylint: disable=import-outside-toplevel,cyclic-import
    from gitential2.core.context import init_context_from_settings
    from gitential2.core.maintenance import maintenance

    settings = settings or load_settings()
    g = init_context_from_settings(settings)
    maintenance(g)


@celery_app.task
def refresh_materialized_views(settings: Optional[GitentialSettings] = None):
    # pylint: disable=import-outside-toplevel,cyclic-import

    from gitential2.core.context import init_context_from_settings

    settings = settings or load_settings()
    g = init_context_from_settings(settings)
    workspaces = g.backend.workspaces.all()
    for w in workspaces:
        try:
            logger.info("Refreshing materialized views in workspace", workspace_id=w.id)
            g.backend.refresh_materialized_views(workspace_id=w.id)
            logger.info("Finished refreshing materialized views in workspace", workspace_id=w.id)

        except:  # pylint: disable=bare-except
            logger.exception("Failed to refresh materialized views", workspace_id=w.id)
    logger.info("Finished refreshing materialized views")
