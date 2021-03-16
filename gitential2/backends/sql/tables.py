import datetime as dt
import sqlalchemy as sa
from gitential2.datatypes import WorkspaceRole
from gitential2.datatypes.subscriptions import SubscriptionType

metadata = sa.MetaData()

users_table = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("login", sa.String(128), nullable=True),
    sa.Column("email", sa.String(256), nullable=True),
    sa.Column("is_admin", sa.Boolean, default=False, nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("marketing_consent_accepted", sa.Boolean, nullable=False, default=False),
    sa.Column("first_name", sa.String(256), nullable=True),
    sa.Column("last_name", sa.String(256), nullable=True),
    sa.Column("company_name", sa.String(256), nullable=True),
    sa.Column("position", sa.String(256), nullable=True),
    sa.Column("development_team_size", sa.String(256), nullable=True),
    sa.Column("registration_ready", sa.Boolean, default=False, nullable=False),
    sa.Column("login_ready", sa.Boolean, default=False, nullable=False),
    sa.Column("is_active", sa.Boolean, default=False, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
)

subscriptions_table = sa.Table(
    "subscriptions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "user_id",
        sa.Integer,
        sa.ForeignKey("users.id"),
        nullable=False,
    ),
    sa.Column("subscription_start", sa.DateTime, nullable=False),
    sa.Column("subscription_end", sa.DateTime, nullable=True),
    sa.Column("subscription_type", sa.Enum(SubscriptionType), default=SubscriptionType.trial),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("number_of_developers", sa.Integer(), nullable=False, default=5),
)

user_infos_table = sa.Table(
    "user_infos",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "user_id",
        sa.Integer,
        sa.ForeignKey("users.id"),
        nullable=False,
    ),
    sa.Column("integration_name", sa.String(128), nullable=False),
    sa.Column("integration_type", sa.String(128), nullable=False),
    sa.Column("sub", sa.String(128), nullable=False),
    sa.Column("name", sa.String(128), nullable=True),
    sa.Column("email", sa.String(128), nullable=True),
    sa.Column("preferred_username", sa.String(128), nullable=True),
    sa.Column("profile", sa.String(256), nullable=True),
    sa.Column("picture", sa.String(256), nullable=True),
    sa.Column("website", sa.String(256), nullable=True),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

credentials_table = sa.Table(
    "credentials",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("type", sa.String(32), nullable=False),
    sa.Column("integration_name", sa.String(128), nullable=True),
    sa.Column("integration_type", sa.String(128), nullable=True),
    sa.Column("name", sa.String(128), nullable=True),
    sa.Column("token", sa.LargeBinary, nullable=True),
    sa.Column("refresh_token", sa.LargeBinary, nullable=True),
    sa.Column("public_key", sa.LargeBinary, nullable=True),
    sa.Column("private_key", sa.LargeBinary, nullable=True),
    sa.Column("passphrase", sa.LargeBinary, nullable=True),
    sa.Column("expires_at", sa.DateTime, nullable=True),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

workspaces_table = sa.Table(
    "workspaces",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(128), nullable=True),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("created_by", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

workspace_members_table = sa.Table(
    "workspace_members",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("workspace_id", sa.Integer, sa.ForeignKey("workspaces.id"), nullable=False),
    sa.Column("role", sa.Enum(WorkspaceRole), default=WorkspaceRole.owner),
    sa.Column("primary", sa.Boolean, default=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)
