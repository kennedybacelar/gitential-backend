import datetime as dt
import sqlalchemy as sa

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
    sa.Column("tc_consent_accepted_at", sa.String(32), nullable=True),
    sa.Column("marketing_consent_accepted", sa.Boolean, nullable=False, default=False),
    sa.Column("extra", sa.JSON, nullable=True),
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
    sa.Column("token", sa.BLOB, nullable=True),
    sa.Column("refresh_token", sa.BLOB, nullable=True),
    sa.Column("public_key", sa.BLOB, nullable=True),
    sa.Column("private_key", sa.BLOB, nullable=True),
    sa.Column("passphrase", sa.BLOB, nullable=True),
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

workspace_roles = sa.Table(
    "workspace_roles",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("workspace_id", sa.Integer, sa.ForeignKey("workspaces.id"), nullable=False),
    sa.Column("role", sa.Integer, default=1),
)
