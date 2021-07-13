from collections import namedtuple
from typing import Callable, Optional, Dict, Union
from importlib import import_module

from celery import Celery
from celery.schedules import crontab
from structlog import get_logger
from gitential2.settings import GitentialSettings, load_settings
from gitential2.logging import initialize_logging
from gitential2.datatypes.refresh import (
    RefreshProjectParams,
    RefreshRepositoryParams,
    RefreshWorkspaceParams,
    MaintainWorkspaceParams,
)
from gitential2.datatypes.subscriptions import SubscriptionType

from gitential2.exceptions import LockError

from .context import GitentialContext

logger = get_logger(__name__)

celery_app = Celery()

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
    global celery_app  # pylint: disable=global-statement
    celery_app.conf.update(
        result_expires=120,
        task_acks_late=True,
        broker_url=settings.celery.broker_url or settings.connections.redis_url,
        result_backend=settings.celery.result_backend_url or settings.connections.redis_url,
        imports=("gitential2.core.tasks",),
    )
    celery_app.conf.beat_schedule = {
        # TEMPORARY DISABLED
        # Executes every day at Noon (UTC).
        # "send_scheduled_emails": {
        #     "task": "tasks.send_scheduled_emails",
        #     "schedule": crontab(minute=0, hour=12),
        #     "args": (settings),
        # },
    }

    if settings.refresh.hourly_maintenance_enabled:
        celery_app.conf.beat_schedule["hourly_maintenance"] = {
            "task": "gitential2.core.tasks.hourly_maintenance",
            "schedule": crontab(minute=0),
            "args": (),
        }

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
    from gitential2.core.workspaces import get_workspace_subscription

    settings = settings or load_settings()
    g = init_context_from_settings(settings)

    def _is_pro_or_trial_subscription(workspace_id):
        subscription = get_workspace_subscription(g, workspace_id)
        return (
            subscription.subscription_type in [SubscriptionType.professional, SubscriptionType.trial]
            and subscription.subscription_start < g.current_time()
            and (subscription.subscription_end is None or subscription.subscription_end > g.current_time())
        )

    def _should_schedule_maintenance(workspace_id):
        return g.license.is_valid() and (g.license.is_on_premises or _is_pro_or_trial_subscription(workspace_id))

    for workspace in g.backend.workspaces.all():
        if _should_schedule_maintenance(workspace.id):
            schedule_task(
                g,
                task_name="maintain_workspace",
                params={
                    "workspace_id": workspace.id,
                },
            )
