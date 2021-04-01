from datetime import timedelta
from typing import List, Optional
from pydantic import ValidationError
from pydantic.datetime_parse import parse_datetime
from structlog import get_logger
from gitential2.core import GitentialContext
from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.userinfos import UserInfoCreate
from gitential2.datatypes.subscriptions import SubscriptionCreate, SubscriptionType
from gitential2.datatypes.credentials import CredentialCreate, CredentialType
from gitential2.datatypes.workspaces import WorkspaceInDB
from gitential2.datatypes.workspacemember import WorkspaceMemberCreate, WorkspaceRole


logger = get_logger(__name__)


def _truncate_public(g: GitentialContext):
    g.backend.workspace_members.truncate()
    g.backend.workspaces.truncate()
    g.backend.credentials.truncate()
    g.backend.user_infos.truncate()
    g.backend.subscriptions.truncate()
    g.backend.users.truncate()


def import_legacy_database(
    g: GitentialContext,
    legacy_users: List[dict],
    legacy_secrets: List[dict],
    legacy_accounts: List[dict],
    legacy_users_accounts: List[dict],
):

    _truncate_public(g)
    import_legacy_users(g, legacy_users)
    import_legacy_secrets(g, legacy_secrets)
    import_legacy_workspaces(
        g,
        legacy_accounts,
        legacy_users_accounts,
    )


def import_legacy_users(g: GitentialContext, legacy_users: List[dict]):

    for legacy_user in legacy_users:
        _import_legacy_user(g, legacy_user)
    # fix_seq_counter()


def import_legacy_secrets(g: GitentialContext, legacy_secrets: List[dict]):
    for legacy_secret in legacy_secrets:
        _import_legacy_secret(g, legacy_secret)
    # fix_seq_counter()


def import_legacy_workspaces(g: GitentialContext, legacy_workspaces: List[dict], legacy_workspace_members: List[dict]):
    user_login_id_map = {user.login: user.id for user in g.backend.users.all()}
    for legacy_workspace in legacy_workspaces:
        members = [member for member in legacy_workspace_members if member["account_id"] == legacy_workspace["id"]]
        _import_legacy_workspace(g, legacy_workspace, members, user_login_id_map)
    # # fix_seq_counter()


def _import_legacy_user(g: GitentialContext, legacy_user: dict):
    try:
        user_create = UserInDB(
            id=legacy_user["id"],
            login=legacy_user["login"],
            email=legacy_user["email"] or f"{legacy_user['login']}@gitential-missing-email.com",
            is_admin=bool(legacy_user["admin"]),
            login_ready=True,
            is_active=legacy_user["is_active"],
            created_at=legacy_user["created_at"],
            updated_at=legacy_user["updated_at"],
        )
        logger.info("Importing user", user_id=user_create.id_, email=user_create.email)
        g.backend.users.insert(legacy_user["id"], user_create)
        _create_user_infos(g, legacy_user)
        _create_subscription(g, legacy_user)
    except ValidationError as e:
        print(f"Failed to import user {legacy_user['id']}", e)


def _create_user_infos(g, legacy_user: dict):
    if legacy_user["github_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="github",
                integration_type="github",
                sub=str(legacy_user["github_id"]),
            )
        )
    if legacy_user["gitlab_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="gitlab",
                integration_type="gitlab",
                sub=str(legacy_user["gitlab_id"]),
            )
        )

    if legacy_user["vsts_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="vsts",
                integration_type="vsts",
                sub=str(legacy_user["vsts_id"]),
            )
        )

    if legacy_user["bitbucket_id"]:
        g.backend.user_infos.create(
            UserInfoCreate(
                user_id=legacy_user["id"],
                integration_name="bitbucket",
                integration_type="bitbucket",
                sub=str(legacy_user["bitbucket_id"]),
            )
        )


def _create_subscription(g: GitentialContext, legacy_user: dict):
    is_pro, num_of_developers = _is_paying_customer(legacy_user)
    registration_time = parse_datetime(legacy_user["created_at"])
    trial_end = registration_time + timedelta(days=14)
    subscription_create = SubscriptionCreate(
        user_id=legacy_user["id"],
        subscription_start=legacy_user["created_at"],
        subscription_end=trial_end if not is_pro else None,
        subscription_type=SubscriptionType.professional if is_pro else SubscriptionType.trial,
        number_of_developers=num_of_developers,
    )
    g.backend.subscriptions.create(subscription_create)


def _is_paying_customer(legacy_user: dict):
    paying_customers = {
        "chris.khoo@pocketpinata.com": 5,
        "janono@gmail.com": 150,
        "bill@tech9.com": 40,
        "f.sodano@wisr.com.au": 16,
        "jsudbury@vistek.ca": 4,
    }

    if legacy_user["email"] in paying_customers:
        return True, paying_customers[legacy_user["email"]]
    else:
        return False, 0


def _import_legacy_secret(g: GitentialContext, legacy_secret: dict):
    def _encrypt(s: Optional[str]):
        if s:
            return g.fernet.encrypt_string(s).encode()
        else:
            return None

    logger.info(
        "Importing secret", name=legacy_secret["name"], owner_id=legacy_secret["owner_id"], type=legacy_secret["type"]
    )

    g.backend.credentials.create(
        CredentialCreate(
            owner_id=legacy_secret["owner_id"],
            name=legacy_secret["name"],
            created_at=legacy_secret["created_at"],
            updated_at=legacy_secret["updated_at"],
            type=CredentialType.token if legacy_secret["type"] == "token" else CredentialType.keypair,
            integration_name=legacy_secret["name"].split(" ")[0] if legacy_secret["type"] == "token" else None,
            integration_type=legacy_secret["name"].split(" ")[0] if legacy_secret["type"] == "token" else None,
            token=_encrypt(legacy_secret["token"]),
            refresh_token=_encrypt(legacy_secret["refresh_token"]),
            public_key=_encrypt(legacy_secret["public_key"]),
            private_key=_encrypt(legacy_secret["private_key"]),
        )
    )


def _import_legacy_workspace(g: GitentialContext, legacy_ws: dict, members: List[dict], user_login_id_map: dict):
    def _find_owner_from_members(members: List[dict]):
        for m in members:
            if m["role"] == 1:
                return m["user_id"]
        return None

    created_by = legacy_ws["owner_id"] or _find_owner_from_members(members) or user_login_id_map.get(legacy_ws["name"])

    if created_by:
        workspace_create = WorkspaceInDB(
            id=legacy_ws["id"],
            name=legacy_ws["name"],
            created_by=created_by,
            created_at=legacy_ws["created_at"],
            updated_at=legacy_ws["updated_at"],
        )
        logger.info(
            "Importing workspace",
            name=legacy_ws["name"],
            created_by=created_by,
            id=legacy_ws["id"],
            collab_count=len(members),
        )

        g.backend.workspaces.insert(legacy_ws["id"], workspace_create)
        for m in members:
            workspace_member_create = WorkspaceMemberCreate(
                user_id=m["user_id"],
                workspace_id=m["account_id"],
                primary=bool(m["primary"]),
                role=WorkspaceRole.owner if m["role"] == 1 else WorkspaceRole.collaborator,
            )
            g.backend.workspace_members.create(workspace_member_create)
    else:
        logger.warn("skipping workspace", workspace=legacy_ws, members=members)


def import_legacy_workspace(
    workspace_id: int,
    legacy_projects: List[dict],
    legacy_repositories: List[dict],
    legacy_projects_repos: List[dict],
    legacy_authors: List[dict],
    legacy_aliases: List[dict],
    legacy_teams: List[dict],
    legacy_teams_authors: List[dict],
):  # pylint: disable=unused-argument
    pass
