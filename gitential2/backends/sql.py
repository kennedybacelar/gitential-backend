import uuid
import datetime as dt
import sqlalchemy as sa
from sqlalchemy_utils.types import UUIDType


def get_metadata():
    metadata = sa.MetaData()
    users = sa.Table(
        "users",
        metadata,
        sa.Column("id", UUIDType, default=uuid.uuid4, primary_key=True),
        sa.Column("login", sa.String(128), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("is_admin", sa.Boolean, default=False, nullable=False),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.now, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.now, nullable=False),
        sa.Column("tc_consent_accepted_at", sa.String(32), nullable=True),
        sa.Column("marketing_consent_accepted", sa.Boolean, nullable=False, default=False),
    )

    workspaces = sa.Table(
        "workspaces",
        metadata,
        sa.Column("id", UUIDType, default=uuid.uuid4, primary_key=True),
        sa.Column("name", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.now, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.now, nullable=False),
    )

    workspace_roles = sa.Table(
        "workspace_roles",
        metadata,
        sa.Column("id", UUIDType, default=uuid.uuid4, primary_key=True),
        sa.Column("user_id", UUIDType, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("workspace_id", UUIDType, sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("role", sa.Integer, default=1),
    )

    credentials = sa.Table(
        "credentials",
        metadata,
        sa.Column("id", UUIDType, default=uuid.uuid4, primary_key=True),
        sa.Column("owner_id", UUIDType, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("source_name", sa.String(32), nullable=True),
        sa.Column("name", sa.String(128), nullable=True),
        sa.Column("token", sa.BLOB, nullable=True),
        sa.Column("refresh_token", sa.BLOB, nullable=True),
        sa.Column("public_key", sa.BLOB, nullable=True),
        sa.Column("private_key", sa.BLOB, nullable=True),
        sa.Column("passphrase", sa.BLOB, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.now, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.now, nullable=False),
    )

    return metadata


def get_workspace_metadata():
    metadata = sa.MetaData()
    # Extracted Commits
    extracted_commits = sa.Table(
        "extracted_commits",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("aname", sa.String(128)),
        sa.Column("ctime", sa.DateTime()),
        sa.Column("cemail", sa.String(128)),
        sa.Column("cname", sa.String(128)),
        sa.Column("message", sa.Text()),
        sa.Column("nparents", sa.Integer()),
        sa.Column("tree_id", sa.String(40)),
    )

    # Extracted Patches
    extracted_patches = sa.Table(
        "extracted_patches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("parent_commit_id", sa.String(40)),
        sa.Column("status", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("oldpath", sa.String(256)),
        sa.Column("newsize", sa.Integer()),
        sa.Column("oldsize", sa.Integer()),
        sa.Column("is_binary", sa.Boolean()),
        sa.Column("lang", sa.String(32)),
        sa.Column("langtype", sa.String(32)),
        # Extracted plain metrics
        sa.Column("loc_i", sa.Integer()),
        sa.Column("loc_d", sa.Integer()),
        sa.Column("comp_i", sa.Integer()),
        sa.Column("comp_d", sa.Integer()),
        sa.Column("loc_i_std", sa.Integer()),
        sa.Column("loc_d_std", sa.Integer()),
        sa.Column("comp_i_std", sa.Integer()),
        sa.Column("comp_d_std", sa.Integer()),
        sa.Column("nhunks", sa.Integer()),
        sa.Column("nrewrites", sa.Integer()),
        sa.Column("rewrites_loc", sa.Integer()),
    )

    # Extracted Patch Rewrites
    extracted_patch_rewrites = sa.Table(
        "extracted_patch_rewrites",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("rewritten_atime", sa.DateTime()),
        sa.Column("rewritten_aemail", sa.String(128)),
        sa.Column("rewritten_commit_id", sa.String(40)),
        sa.Column("loc_d", sa.Integer()),
    )

    # Calculated Patches

    # Calculated Commits
    return metadata
