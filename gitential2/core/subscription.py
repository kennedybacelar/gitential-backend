from typing import Optional
from datetime import datetime, timedelta

from structlog import get_logger
from gitential2.exceptions import InvalidStateException

from gitential2.datatypes.stats import Query
from gitential2.datatypes.users import UserInDB

from gitential2.core import GitentialContext
from gitential2.core.users import get_current_subscription

from gitential2.datatypes.subscriptions import SubscriptionType, SubscriptionInDB
from gitential2.datatypes.stats import FilterName
from gitential2.datatypes.workspacemember import WorkspaceRole

logger = get_logger(__name__)
TRIAL_FILTER_PERIOD_DAY = 90


def get_workspace_subscription(g: GitentialContext, ws_id: int) -> SubscriptionInDB:
    owner = get_workspace_owner(g, ws_id)
    if not owner:
        raise InvalidStateException("no owner of the workspace")
    return get_current_subscription(g, owner.id)


def get_workspace_owner(g: GitentialContext, ws_id: int) -> Optional[UserInDB]:
    members = g.backend.workspace_members.get_for_workspace(ws_id)
    for member in members:
        if member.role == WorkspaceRole.owner:
            return g.backend.users.get(member.user_id)
    raise InvalidStateException("No owner of the workspace")


def is_workspace_subs_prof(g: GitentialContext, ws_id: int) -> bool:
    sub = get_workspace_subscription(g, ws_id)
    return sub.subscription_type in [SubscriptionType.professional, SubscriptionType.trial]


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
