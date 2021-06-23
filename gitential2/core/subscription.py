from typing import Optional
from datetime import datetime, timedelta

from structlog import get_logger

from gitential2.datatypes.stats import Query

from gitential2.datatypes.subscriptions import (
    SubscriptionInDB,
    SubscriptionCreate,
    SubscriptionType,
    SubscriptionUpdate,
)
from gitential2.datatypes.stats import FilterName

from .context import GitentialContext


logger = get_logger(__name__)
TRIAL_FILTER_PERIOD_DAY = 90


def is_free_user(g: GitentialContext, user_id: int):
    sub = get_current_subscription(g, user_id)
    if sub.subscription_type == SubscriptionType.trial:
        return True
    return False


def get_current_subscription(g: GitentialContext, user_id: int) -> SubscriptionInDB:
    if g.license.is_on_premises:
        return SubscriptionInDB(
            id=0,
            user_id=user_id,
            subscription_type=SubscriptionType.professional,
            subscription_start=datetime(1970, 1, 1),
            subscription_end=datetime(2099, 12, 31),
        )

    current_subscription_from_db = _get_current_subscription_from_db(g, user_id)
    if current_subscription_from_db:
        return current_subscription_from_db
    else:
        return SubscriptionInDB(
            id=0,
            user_id=user_id,
            subscription_type=SubscriptionType.free,
            subscription_start=datetime.utcnow(),
        )


def _get_current_subscription_from_db(g: GitentialContext, user_id: int) -> Optional[SubscriptionInDB]:
    current_time = datetime.utcnow()

    def _is_subscription_valid(s: SubscriptionInDB):
        return s.subscription_start < current_time and (s.subscription_end is None or s.subscription_end > current_time)

    subscriptions = g.backend.subscriptions.get_subscriptions_for_user(user_id)

    valid_subscriptions = [s for s in subscriptions if _is_subscription_valid(s)]
    if valid_subscriptions:
        return valid_subscriptions[0]
    else:
        return None


def limit_filter_time(ws_id: int, query: Query) -> Query:
    min_allowed_dt = datetime.now() - timedelta(days=TRIAL_FILTER_PERIOD_DAY)
    if FilterName.day not in query.filters:
        query.filters[FilterName.day].append(
            [
                min_allowed_dt.strftime("%Y-%m-%d"),
                datetime.now().strftime("%Y-%m-%d"),
            ]
        )
        logger.debug("limiting query by adding filters", workspace_id=ws_id)
    else:
        filter_start_dt = datetime.strptime(query.filters[FilterName.day][0], "%Y-%m-%d")
        filter_end_dt = datetime.strptime(query.filters[FilterName.day][1], "%Y-%m-%d")
        if filter_start_dt < min_allowed_dt:
            query.filters[FilterName.day][0] = min_allowed_dt.strftime("%Y-%m-%d")
            logger.debug("limiting query, limit start filtertime", workspace_id=ws_id)
        if filter_end_dt < min_allowed_dt:
            query.filters[FilterName.day][1] = (min_allowed_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            logger.debug("limiting query, limit end filtertime", workspace_id=ws_id)

    return query


def set_as_professional(g: GitentialContext, user_id: int, number_of_developers: int) -> SubscriptionInDB:
    user = g.backend.users.get_or_error(user_id)
    current_subs = _get_current_subscription_from_db(g, user_id=user.id)

    if current_subs and current_subs.subscription_type == SubscriptionType.professional:
        su = SubscriptionUpdate(**current_subs.dict())
        su.number_of_developers = number_of_developers
        return g.backend.subscriptions.update(current_subs.id, su)
    elif current_subs and current_subs.subscription_type != SubscriptionType.professional:
        su = SubscriptionUpdate(**current_subs.dict())
        su.subscription_end = datetime.utcnow()
        g.backend.subscriptions.update(current_subs.id, su)
        cancel_trial_emails(g, user_id)
        return _create_new_prof_subs(g, user_id, number_of_developers)
    else:
        return _create_new_prof_subs(g, user_id, number_of_developers)


def _create_new_prof_subs(g: GitentialContext, user_id: int, number_of_developers: int) -> SubscriptionInDB:
    return g.backend.subscriptions.create(
        SubscriptionCreate(
            user_id=user_id,
            number_of_developers=number_of_developers,
            subscription_type=SubscriptionType.professional,
            subscription_start=datetime.utcnow(),
        )
    )


def cancel_trial_emails(g: GitentialContext, user_id: int):
    for template in ["free_trial_expiration", "free_trial_ended"]:
        g.backend.email_log.cancel_email(user_id, template)
