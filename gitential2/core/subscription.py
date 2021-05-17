from datetime import datetime, timedelta

from structlog import get_logger
from gitential2.datatypes.stats import Query
from gitential2.datatypes.users import UserInDB
from gitential2.core import GitentialContext
from gitential2.core.users import get_current_subscription

from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.datatypes.stats import FilterName


logger = get_logger(__name__)
TRIAL_FILTER_PERIOD_DAY = 90


def is_subscription_professional(g: GitentialContext, user_id: int) -> bool:
    sub = get_current_subscription(g, user_id)
    if sub.subscription_type == SubscriptionType.professional:
        return True
    return False


def limit_filter_time(g: GitentialContext, ws_id: int, query: Query, current_user: UserInDB) -> Query:
    if is_subscription_professional(g, current_user.id):
        return query
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
