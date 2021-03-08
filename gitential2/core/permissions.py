from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.permissions import Entity, Action
from .context import GitentialContext


def check_permission(g: GitentialContext, current_user: UserInDB, entity: Entity, action: Action, **kwargs) -> bool:
    return True
