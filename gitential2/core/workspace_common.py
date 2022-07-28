from gitential2.exceptions import PermissionException
from gitential2.datatypes.workspaces import WorkspaceCreate, WorkspaceInDB, WorkspaceDuplicate

from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate

from .subscription import get_current_subscription
from .context import GitentialContext


def create_workspace(
    g: GitentialContext, workspace: WorkspaceCreate, current_user: UserInDB, primary=False
) -> WorkspaceInDB:
    if not primary and g.license.is_cloud:
        sub = get_current_subscription(g, current_user.id)
        if sub.subscription_type == SubscriptionType.free:
            raise PermissionException("Users with FREE subscription can only have one workspace")

    workspace.created_by = current_user.id
    workspace_in_db = g.backend.workspaces.create(workspace)
    g.backend.workspace_members.create(
        WorkspaceMemberCreate(
            workspace_id=workspace_in_db.id, user_id=current_user.id, role=WorkspaceRole.owner, primary=primary
        )
    )
    g.backend.initialize_workspace(workspace_id=workspace_in_db.id)
    return workspace_in_db


def duplicate_workspace(
    g: GitentialContext, workspace_duplicate: WorkspaceDuplicate, current_user: UserInDB
) -> WorkspaceInDB:
    workspace_create = WorkspaceCreate(name=workspace_duplicate.name)
    result: WorkspaceInDB = create_workspace(g=g, workspace=workspace_create, current_user=current_user)
    g.backend.duplicate_workspace(
        workspace_id_from=workspace_duplicate.id_of_workspace_to_be_duplicated, workspace_id_to=result.id
    )
    return result
