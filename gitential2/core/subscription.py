from typing import List
from datetime import datetime, timedelta

from structlog import get_logger
from gitential2.datatypes.stats import Query
from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.core.workspaces import get_members
from gitential2.core import GitentialContext

from gitential2.datatypes.subscriptions import SubscriptionInDB, SubscriptionType
from gitential2.datatypes.stats import FilterName


logger = get_logger(__name__)
TRIAL_FILTER_PERIOD_DAY = 90


def get_current_workspace_owner_subscriptions(g: GitentialContext, ws_id: int):
    members = get_members(g, ws_id)
    for member in members:
        if member.role == WorkspaceRole.owner:
            if member.user_id:
                return g.backend.subscriptions.get_subscriptions_for_user(member.user_id)
    raise Exception("No owner for this workspace")


def is_subscription_professional(subscriptions: List[SubscriptionInDB]) -> bool:
    for sub in subscriptions:
        if sub.subscription_end:
            if sub.subscription_type == SubscriptionType.professional and sub.subscription_end > datetime.now():
                return True
    return False


def limit_filter_time(g: GitentialContext, ws_id: int, query: Query) -> Query:
    if g.license.is_on_premises:
        return query
    active_user_subs: List[SubscriptionInDB] = get_current_workspace_owner_subscriptions(g, ws_id)
    if is_subscription_professional(active_user_subs):
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
