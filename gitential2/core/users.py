from datetime import datetime, timedelta
from typing import Iterable, Optional, Tuple, cast
from structlog import get_logger
from gitential2.datatypes.users import UserCreate, UserUpdate, UserInDB
from gitential2.datatypes.subscriptions import (
    SubscriptionInDB,
    SubscriptionCreate,
)
from gitential2.datatypes.userinfos import UserInfoCreate, UserInfoUpdate
from gitential2.datatypes.credentials import CredentialCreate, CredentialUpdate
from gitential2.datatypes.workspaces import WorkspaceCreate
from gitential2.datatypes.workspacemember import WorkspaceRole
from gitential2.core.workspace_common import create_workspace

from .context import GitentialContext
from .emails import send_email_to_user, send_system_notification_email


logger = get_logger(__name__)


def handle_authorize(
    g: GitentialContext, integration_name: str, token, user_info: dict, current_user: Optional[UserInDB] = None
):
    integration = g.integrations[integration_name]

    # normalize the userinfo
    normalized_userinfo: UserInfoCreate = integration.normalize_userinfo(user_info, token=token)

    # update or create a user and the proper user_info in backend
    user, user_info, is_new_user = _create_or_update_user_and_user_info(g, normalized_userinfo, current_user)

    # update or create credentials based on integration and user
    _create_or_update_credential_from(g, user, integration_name, integration.integration_type, token)

    # Create workspace if missing
    _create_primary_workspace_if_missing(g, user)
    return {"ok": True, "user": user, "user_info": user_info, "is_new_user": is_new_user}


def _calc_user_login(user_create: UserCreate):
    if user_create.first_name and user_create.last_name:
        return " ".join([user_create.first_name, user_create.last_name])
    else:
        return user_create.email.split("@")[0]


def register_user(
    g: GitentialContext, user: UserCreate, current_user: Optional[UserInDB] = None
) -> Tuple[UserInDB, Optional[SubscriptionInDB]]:
    if not current_user:
        existing_user = g.backend.users.get_by_email(user.email)
        if existing_user:
            raise ValueError("Email already used.")
        user.registration_ready = True
        user.login = _calc_user_login(user)
        user_in_db = g.backend.users.create(user)
        subscription = _create_default_subscription_after_reg(g, user_in_db)
        _create_primary_workspace_if_missing(g, user_in_db)
    else:
        user.registration_ready = True
        user.login = _calc_user_login(user)
        user_in_db = g.backend.users.update(current_user.id, cast(UserUpdate, user))
        subscription = _create_default_subscription_after_reg(g, user_in_db)
    if g.license.is_cloud and subscription:
        send_email_to_user(g, user_in_db, template_name="welcome")
        if g.settings.notifications.request_free_trial:
            send_system_notification_email(g, user_in_db, template_name="request_free_trial")
    return user_in_db, subscription


def get_user(g: GitentialContext, user_id: int) -> Optional[UserInDB]:
    user = g.backend.users.get(user_id)
    if user and user.is_active:
        return user
    return None


def update_user(g: GitentialContext, user_id: int, user_update: UserUpdate):
    return g.backend.users.update(user_id, user_update)


def delete_user(g: GitentialContext, user_id: int):
    user = g.backend.users.get_or_error(user_id)
    user_update = UserUpdate(**user.dict())
    user_update.is_active = False
    g.backend.users.update(user_id, user_update)
    return True


def get_profile_picture(g: GitentialContext, user: UserInDB) -> Optional[str]:
    user_infos = [
        user_info for user_info in g.backend.user_infos.get_for_user(user.id) if user_info.picture is not None
    ]
    if user_infos:
        user_infos_sorted = sorted(user_infos, key=lambda ui: ui.updated_at or datetime.utcnow())
        return user_infos_sorted[-1].picture
    else:
        return None


def _create_or_update_user_and_user_info(
    g: GitentialContext, normalized_userinfo: UserInfoCreate, current_user: Optional[UserInDB] = None
):
    existing_userinfo = g.backend.user_infos.get_by_sub_and_integration(
        sub=normalized_userinfo.sub, integration_name=normalized_userinfo.integration_name
    )
    if existing_userinfo:
        if current_user and existing_userinfo.user_id != current_user.id:
            raise ValueError("Authentication error, this user's credential already existing in our system")

        user = g.backend.users.get_or_error(existing_userinfo.user_id)

        user_update = user.copy()
        user_update.login_ready = True
        user_update.is_active = True
        user = g.backend.users.update(user.id, cast(UserUpdate, user_update))

        user_info = g.backend.user_infos.update(existing_userinfo.id, cast(UserInfoUpdate, normalized_userinfo))
        return user, user_info, False
    else:
        existing_user = current_user or (
            g.backend.users.get_by_email(normalized_userinfo.email) if normalized_userinfo.email else None
        )
        if existing_user:
            user_update = existing_user.copy()
            user_update.login_ready = True
            user_update.is_active = True
            user = g.backend.users.update(existing_user.id, cast(UserUpdate, user_update))
            is_new_user = False
        else:
            new_user = UserCreate.from_user_info(normalized_userinfo)
            new_user.login_ready = True
            user = g.backend.users.create(new_user)
            is_new_user = True
        user_info_data = normalized_userinfo.dict(exclude_none=True)
        user_info_data.setdefault("user_id", user.id)
        user_info = g.backend.user_infos.create(normalized_userinfo.copy(update={"user_id": user.id}))
        return user, user_info, is_new_user


def _create_or_update_credential_from(
    g: GitentialContext, user: UserInDB, integration_name: str, integration_type: str, token: dict
):
    new_credential = CredentialCreate.from_token(
        token=token,
        fernet=g.fernet,
        owner_id=user.id,
        integration_name=integration_name,
        integration_type=integration_type,
    )

    existing_credential = g.backend.credentials.get_by_user_and_integration(
        owner_id=user.id, integration_name=integration_name
    )
    if existing_credential:
        g.backend.credentials.update(id_=existing_credential.id, obj=CredentialUpdate(**new_credential.dict()))
    else:
        g.backend.credentials.create(new_credential)


def _create_primary_workspace_if_missing(g: GitentialContext, user: UserInDB):
    existing_workspace_memberships = g.backend.workspace_members.get_for_user(user_id=user.id)
    has_primary = any(ewm.role == WorkspaceRole.owner for ewm in existing_workspace_memberships)
    if not has_primary:
        workspace = WorkspaceCreate(name=f"{user.login}'s workspace")
        create_workspace(g, workspace, current_user=user, primary=True)


def _create_default_subscription_after_reg(g: GitentialContext, user) -> Optional[SubscriptionInDB]:
    subscriptions = g.backend.subscriptions.get_subscriptions_for_user(user.id)
    if subscriptions:
        return None
    else:
        g.backend.email_log.schedule_email(
            user_id=user.id, template_name="free_trial_expiration", scheduled_at=(datetime.utcnow() + timedelta(days=7))
        )
        # TEMPORARY DISABLED
        # g.backend.email_log.schedule_email(
        #     user_id=user.id, template_name="free_trial_ended", scheduled_at=(datetime.utcnow() + timedelta(days=14))
        # )
        return g.backend.subscriptions.create(SubscriptionCreate.default_for_new_user(user.id))


def set_as_admin(g: GitentialContext, user_id: int, is_admin: bool = True) -> UserInDB:
    user = g.backend.users.get_or_error(user_id)
    user_update = UserUpdate(**user.dict())
    user_update.is_admin = is_admin
    if is_admin:
        logger.info(f"Setting user {user.login}(id: {user.id}) to admin")
    else:
        logger.info(f"Setting user {user.login}(id: {user.id}) to non-admin")
    return g.backend.users.update(user_id, user_update)


def list_users(g: GitentialContext) -> Iterable[UserInDB]:
    return g.backend.users.all()


def send_trial_end_soon_emails(g: GitentialContext, user_id: int):
    send_email_to_user(g, user=g.backend.users.get_or_error(user_id), template_name="free_trial_expiration")


def send_trial_ended_emails(g: GitentialContext, user_id: int):
    send_email_to_user(g, user=g.backend.users.get_or_error(user_id), template_name="free_trial_ended")
    g.backend.email_log.email_log_status_update(user_id, template_name="free_trial_ended", status="sent")
