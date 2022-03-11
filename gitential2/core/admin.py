from typing import List

from gitential2.datatypes.users import UserInAdminRepr, UserInDB
from .context import GitentialContext
from .users import list_users
from .access_log import get_last_interaction_at


def admin_list_users(g: GitentialContext) -> List[UserInAdminRepr]:
    return [_to_user_in_admin_repr(g, user) for user in list_users(g)]


def _to_user_in_admin_repr(g: GitentialContext, user: UserInDB) -> UserInAdminRepr:
    user_dict = user.dict(include={"id", "created_at", "updated_at", "is_admin", "is_active", "email"})
    return UserInAdminRepr(
        name=user.full_name,
        last_interaction_at=get_last_interaction_at(g, user.id),
        access_approved=is_access_approved(g, user),
        **user_dict,
    )


def is_access_approved(g: GitentialContext, user: UserInDB) -> bool:
    return (not g.settings.features.enable_access_approval) or (user.is_active and user.is_admin)
