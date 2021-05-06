from typing import Optional, List

from gitential2.datatypes.workspaces import WorkspaceCreate, WorkspaceInDB, WorkspaceUpdate, WorkspacePublic
from gitential2.datatypes.workspacemember import (
    WorkspaceMemberCreate,
    WorkspaceRole,
    WorkspaceMemberPublic,
    WorkspaceMemberInDB,
    MemberInvite,
)
from gitential2.datatypes.users import UserInDB, UserHeader
from .context import GitentialContext
from .users_common import get_user_by_email
from .projects import list_projects
from .emails import send_email_to_user


def get_accessible_workspaces(
    g: GitentialContext, current_user: UserInDB, include_members: bool = False, include_projects: bool = False
) -> List[WorkspacePublic]:
    workspace_memberships = g.backend.workspace_members.get_for_user(user_id=current_user.id)
    workspaces = [
        get_workspace(
            g=g,
            workspace_id=membership.workspace_id,
            current_user=current_user,
            include_members=include_members,
            include_projects=include_projects,
            _membership=membership,
        )
        for membership in workspace_memberships
    ]
    if current_user.is_admin:
        workspaces = _add_admin_as_collaborator_to_all_workspaces(
            g, current_user, workspaces, include_members, include_projects
        )
    return workspaces


def get_own_workspaces(g: GitentialContext, user_id: int) -> List[WorkspaceInDB]:
    workspace_memberships = g.backend.workspace_members.get_for_user(user_id=user_id)
    ret = []
    for wm in workspace_memberships:
        if wm.role == WorkspaceRole.owner:
            workspace = g.backend.workspaces.get(wm.workspace_id)
            if workspace:
                ret.append(workspace)
    return ret


def _add_admin_as_collaborator_to_all_workspaces(
    g: GitentialContext,
    current_user: UserInDB,
    workspaces: List[WorkspacePublic],
    include_members: bool = False,
    include_projects: bool = False,
) -> List[WorkspacePublic]:
    allready_added_workspace_ids = [ws.id for ws in workspaces]
    all_workspaces = g.backend.workspaces.all()
    for ws in all_workspaces:
        if ws.id not in allready_added_workspace_ids:
            ws_data = ws.dict()
            ws_data["name"] = ws_data["name"] + " [A]"
            if include_members:
                ws_data["members"] = get_members(g, workspace_id=ws.id)

            if include_projects:
                ws_data["projects"] = list_projects(g=g, workspace_id=ws.id)

            ws_data["membership"] = WorkspaceMemberInDB(
                id=0,
                user_id=current_user.id,
                workspace_id=ws.id,
                role=WorkspaceRole.collaborator,
            )
            workspaces.append(WorkspacePublic(**ws_data))
    return workspaces


def get_workspace(
    g: GitentialContext,
    workspace_id: int,
    current_user: UserInDB,
    include_members: bool = False,
    include_projects: bool = False,
    _membership: Optional[WorkspaceMemberInDB] = None,
) -> WorkspacePublic:

    membership = _membership or g.backend.workspace_members.get_for_workspace_and_user(
        workspace_id=workspace_id, user_id=current_user.id
    )

    if membership:
        workspace = g.backend.workspaces.get_or_error(workspace_id)
        workspace_data = workspace.dict()
        workspace_data["membership"] = membership.dict()

        if include_members:
            workspace_data["members"] = get_members(g, workspace_id=workspace.id)

        if include_projects:
            workspace_data["projects"] = list_projects(g=g, workspace_id=workspace_id)

        return WorkspacePublic(**workspace_data)
    else:
        raise Exception("Access Denied")


def create_workspace(
    g: GitentialContext, workspace: WorkspaceCreate, current_user: UserInDB, primary=False
) -> WorkspaceInDB:
    workspace.created_by = current_user.id

    workspace_in_db = g.backend.workspaces.create(workspace)
    g.backend.workspace_members.create(
        WorkspaceMemberCreate(
            workspace_id=workspace_in_db.id, user_id=current_user.id, role=WorkspaceRole.owner, primary=primary
        )
    )
    g.backend.initialize_workspace(workspace_id=workspace_in_db.id)
    return workspace_in_db


def update_workspace(
    g: GitentialContext, workspace_id: int, workspace: WorkspaceUpdate, current_user: UserInDB
) -> WorkspaceInDB:
    membership = g.backend.workspace_members.get_for_workspace_and_user(
        workspace_id=workspace_id, user_id=current_user.id
    )
    if membership:
        return g.backend.workspaces.update(workspace_id, workspace)
    else:
        raise Exception("Authentication error")


def delete_workspace(g: GitentialContext, workspace_id: int, current_user: UserInDB) -> int:
    membership = g.backend.workspace_members.get_for_workspace_and_user(
        workspace_id=workspace_id, user_id=current_user.id
    )
    if membership:
        return g.backend.workspaces.delete(workspace_id)
    else:
        raise Exception("Authentication error")


def get_members(g: GitentialContext, workspace_id: int, include_user_header=True) -> List[WorkspaceMemberPublic]:
    def _process(member):
        member_data = member.dict()
        if include_user_header:
            user = g.backend.users.get_or_error(member.user_id)
            member_data["user"] = UserHeader(id=user.id, login=user.login)
        return WorkspaceMemberPublic(**member_data)

    return [_process(member) for member in g.backend.workspace_members.get_for_workspace(workspace_id=workspace_id)]


def invite_members(
    g: GitentialContext, current_user: UserInDB, workspace_id: int, invitations: List[MemberInvite]
) -> int:
    workspace = g.backend.workspaces.get_or_error(workspace_id)
    for invitation in invitations:
        existing_user = get_user_by_email(g, invitation.email)
        if existing_user:
            user = existing_user
        else:
            user = g.backend.users.create(invitation.user_create())

        g.backend.workspace_members.create(
            WorkspaceMemberCreate(
                workspace_id=workspace_id,
                user_id=user.id,
                role=WorkspaceRole.collaborator,
                primary=False,
            )
        )
        if g.license.is_cloud:
            send_email_to_user(
                g, user=user, template_name="invite_member", invitation_sender=current_user, workspace=workspace
            )
    return len(invitations)


def remove_member(g: GitentialContext, workspace_id: int, workspace_member_id: int) -> int:
    workspace_member = g.backend.workspace_members.get(workspace_member_id)
    if workspace_member and workspace_member.workspace_id == workspace_id:
        g.backend.workspace_members.delete(workspace_member_id)
    return 1
