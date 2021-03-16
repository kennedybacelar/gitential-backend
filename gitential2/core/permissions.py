from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.permissions import Entity, Action
from .context import GitentialContext
from .workspaces import get_accessible_workspaces


def check_permission(g: GitentialContext, current_user: UserInDB, entity: Entity, action: Action, **kwargs) -> bool:
    if not current_user.is_active:
        return False
    if current_user.is_admin:
        return True

    if "workspace_id" in kwargs:
        acessible_workspaces = get_accessible_workspaces(g, current_user=current_user)
        try:
            workspace = [ws for ws in acessible_workspaces if ws.id == kwargs["workspace_id"]][0]
            if workspace.membership:
                return workspace.membership.role == WorkspaceRole.owner or (
                    workspace.membership.role == WorkspaceRole.collaborator
                    and (
                        entity
                        in [
                            Entity.project,
                            Entity.team,
                            Entity.repository,
                        ]
                        or action in [Action.read]
                    )
                )
        except IndexError:
            return False

    return True
