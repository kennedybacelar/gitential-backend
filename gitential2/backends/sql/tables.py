from typing import Optional
import datetime as dt
import sqlalchemy as sa
from sqlalchemy.sql.sqltypes import String
from sqlalchemy_utils import IPAddressType

from gitential2.datatypes import WorkspaceRole
from gitential2.datatypes.subscriptions import SubscriptionType, StripeSubStatusType
from gitential2.datatypes.repositories import GitProtocol
from gitential2.datatypes.extraction import Langtype

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
    sa.Column("stripe_customer_id", sa.String(256), nullable=True),
)

access_log_table = sa.Table(
    "access_log",
    metadata,
    sa.Column("log_time", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("ip_address", IPAddressType, nullable=True),
    sa.Column("user_id", sa.Integer, nullable=False),
    sa.Column("path", sa.String(256), nullable=False),
    sa.Column("method", sa.String(16), nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Index("idx_user_id", "user_id"),
    sa.Index("idx_log_time_user_id", "log_time", "user_id"),
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
    sa.Column("stripe_subscription_id", sa.String(256), nullable=True),
    sa.Column("stripe_subscription_status", sa.Enum(StripeSubStatusType), nullable=True),
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

email_log_table = sa.Table(
    "email_log",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("template_name", sa.String, nullable=False),
    sa.Column("status", sa.String, default="scheduled", nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("scheduled_at", sa.DateTime, nullable=False),
    sa.Column("sent_at", sa.DateTime, nullable=True),
)


def get_workspace_metadata(schema: Optional[str] = None):
    metadata = sa.MetaData(schema=schema)

    # pylint: disable=unused-variable

    projects = sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(128), nullable=True),
        sa.Column("pattern", sa.String(256), nullable=True),
        sa.Column("shareable", sa.Boolean, default=False, nullable=False),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    repositories = sa.Table(
        "repositories",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("clone_url", sa.String(256), nullable=False, unique=True),
        sa.Column("protocol", sa.Enum(GitProtocol), default=GitProtocol.https),
        sa.Column("name", sa.String(128)),
        sa.Column("namespace", sa.String(128)),
        sa.Column("private", sa.Boolean, nullable=False, default=True),
        sa.Column("integration_type", sa.String(64), nullable=True),
        sa.Column("integration_name", sa.String(64), nullable=True),
        sa.Column("credential_id", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    project_repositories = sa.Table(
        "project_repositories",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_id", sa.Integer, sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("repo_id", sa.Integer, sa.ForeignKey("repositories.id"), nullable=False),
    )

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
        sa.PrimaryKeyConstraint("repo_id", "commit_id"),
    )

    calculated_commits = sa.Table(
        "calculated_commits",
        metadata,
        # same columns from extracted_commits
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
        # additional column - date
        sa.Column("date", sa.DateTime()),
        sa.Column("age", sa.Integer(), nullable=True),
        # author ids
        sa.Column("aid", sa.Integer()),
        sa.Column("cid", sa.Integer()),
        # is_merge, is_test
        sa.Column("is_merge", sa.Boolean),
        # sa.Column("is_test", sa.Boolean),
        # number of patches
        sa.Column("nfiles", sa.Integer(), nullable=True),
        # calculated from patch, outlier
        sa.Column("loc_i_c", sa.Integer(), nullable=True),
        sa.Column("loc_i_inlier", sa.Integer(), nullable=True),
        sa.Column("loc_i_outlier", sa.Integer(), nullable=True),
        sa.Column("loc_d_c", sa.Integer(), nullable=True),
        sa.Column("loc_d_inlier", sa.Integer(), nullable=True),
        sa.Column("loc_d_outlier", sa.Integer(), nullable=True),
        sa.Column("comp_i_c", sa.Integer(), nullable=True),
        sa.Column("comp_i_inlier", sa.Integer(), nullable=True),
        sa.Column("comp_i_outlier", sa.Integer(), nullable=True),
        sa.Column("comp_d_c", sa.Integer(), nullable=True),
        sa.Column("comp_d_inlier", sa.Integer(), nullable=True),
        sa.Column("comp_d_outlier", sa.Integer(), nullable=True),
        sa.Column("loc_effort_c", sa.Integer(), nullable=True),
        sa.Column("uploc_c", sa.Integer(), default=0, comment="unproductive line of code"),
        sa.Column("is_bugfix", sa.Boolean(), default=None),
        # work hour estimation
        sa.Column("hours_measured", sa.Float(), nullable=True),
        sa.Column("hours_estimated", sa.Float(), nullable=True),
        sa.Column("hours", sa.Float(), nullable=True),
        sa.Column("velocity_measured", sa.Float(), nullable=True),
        sa.Column("velocity", sa.Float(), nullable=True),
        # primary key
        sa.PrimaryKeyConstraint("repo_id", "commit_id"),
        sa.Index("idx_repo_id", "repo_id"),
        sa.Index("idx_is_merge", "is_merge"),
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
        sa.Column("langtype", sa.Enum(Langtype)),
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
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "parent_commit_id", "newpath"),
    )

    calculated_patches = sa.Table(
        "calculated_patches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("parent_commit_id", sa.String(40), nullable=True),
        # author ids
        sa.Column("aid", sa.Integer()),
        sa.Column("cid", sa.Integer()),
        # atime -> date
        sa.Column("date", sa.DateTime()),
        sa.Column("status", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("oldpath", sa.String(256)),
        sa.Column("newsize", sa.Integer()),
        sa.Column("oldsize", sa.Integer()),
        sa.Column("is_binary", sa.Boolean()),
        sa.Column("lang", sa.String(32)),
        sa.Column("langtype", sa.Enum(Langtype)),
        sa.Column("loc_i", sa.Integer()),
        sa.Column("loc_d", sa.Integer()),
        sa.Column("comp_i", sa.Integer()),
        sa.Column("comp_d", sa.Integer()),
        sa.Column("nhunks", sa.Integer()),
        sa.Column("nrewrites", sa.Integer()),
        sa.Column("rewrites_loc", sa.Integer()),
        # calculated
        sa.Column("is_merge", sa.Boolean()),
        sa.Column("is_test", sa.Boolean()),
        sa.Column("uploc", sa.Integer()),
        sa.Column("outlier", sa.Integer()),
        sa.Column("anomaly", sa.Integer()),
        sa.Column("loc_effort_p", sa.Integer(), nullable=True),
        sa.Column("is_collaboration", sa.Boolean()),
        sa.Column("is_new_code", sa.Boolean()),
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "parent_commit_id", "newpath"),
        sa.Index("idx_repo_id_commit_id", "repo_id", "commit_id"),
        sa.Index("idx_lang", "lang"),
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
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "rewritten_commit_id", "newpath"),
    )

    authors = sa.Table(
        "authors",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("active", sa.Boolean),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("aliases", sa.JSON, nullable=True),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    teams = sa.Table(
        "teams",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("sprints_enabled", sa.Boolean, default=False),
        sa.Column("sprint", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    team_members = sa.Table(
        "team_members",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("team_id", sa.Integer, sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("author_id", sa.Integer, sa.ForeignKey("authors.id"), nullable=False),
    )

    # sprints = sa.Table(
    #     "sprints",
    #     metadata,
    #     sa.Column("id", sa.Integer, primary_key=True),
    #     sa.Column("team_id", sa.Integer, sa.ForeignKey("teams.id"), nullable=False),
    #     sa.Column("date", sa.DateTime, nullable=False),
    #     sa.Column("weeks", sa.Integer, default=1),
    #     sa.Column("pattern", sa.String(64)),
    #     sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    #     sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    # )

    # Pull Requests
    pull_requests = sa.Table(
        "pull_requests",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("number", sa.Integer()),
        sa.Column("title", sa.String(256)),
        sa.Column("platform", sa.String(32)),
        sa.Column("id_platform", sa.Integer()),
        sa.Column("api_resource_uri", sa.String(256)),
        sa.Column("state_platform", sa.String(16)),
        sa.Column("state", sa.String(16)),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("closed_at", sa.DateTime, nullable=True),
        sa.Column("updated_at", sa.DateTime, nullable=True),
        sa.Column("merged_at", sa.DateTime, nullable=True),
        sa.Column("additions", sa.Integer(), nullable=True),
        sa.Column("deletions", sa.Integer(), nullable=True),
        sa.Column("changed_files", sa.Integer(), nullable=True),
        sa.Column("draft", sa.Boolean, default=False, nullable=False),
        sa.Column("user", sa.String(64)),  # NOT USED ANYMORE
        # user related fieds
        sa.Column("user_id_external", sa.String(64), nullable=True),
        sa.Column("user_name_external", sa.String(128), nullable=True),
        sa.Column("user_username_external", sa.String(128), nullable=True),
        sa.Column("user_aid", sa.Integer(), nullable=True),
        # number of commits
        sa.Column("commits", sa.Integer(), nullable=True),
        # merged_by who?
        sa.Column("merged_by", sa.String(64), nullable=True),  # NOT USED ANYMORE
        sa.Column("merged_by_id_external", sa.String(64), nullable=True),
        sa.Column("merged_by_name_external", sa.String(128), nullable=True),
        sa.Column("merged_by_username_external", sa.String(128), nullable=True),
        sa.Column("merged_by_aid", sa.Integer(), nullable=True),
        # calculated fields
        sa.Column("first_reaction_at", sa.DateTime, nullable=True),
        sa.Column("first_commit_authored_at", sa.DateTime, nullable=True),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("is_bugfix", sa.Boolean(), default=None),
        sa.PrimaryKeyConstraint("repo_id", "number"),
    )

    pull_request_commits = sa.Table(
        "pull_request_commits",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("pr_number", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("author_name", sa.String(128)),
        sa.Column("author_email", sa.String(128)),
        sa.Column("author_login", sa.String(128), nullable=True),
        sa.Column("author_date", sa.DateTime, nullable=False),
        sa.Column("committer_name", sa.String(128)),
        sa.Column("committer_email", sa.String(128)),
        sa.Column("committer_login", sa.String(128), nullable=True),
        sa.Column("committer_date", sa.DateTime, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number", "commit_id"),
    )

    pull_request_comments = sa.Table(
        "pull_request_comments",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("pr_number", sa.Integer()),
        sa.Column("comment_type", sa.String(32)),
        sa.Column("comment_id", String(32)),
        sa.Column("author_id_external", sa.String(64), nullable=True),
        sa.Column("author_name_external", sa.String(128), nullable=True),
        sa.Column("author_username_external", sa.String(128), nullable=True),
        sa.Column("author_aid", sa.Integer(), nullable=True),
        sa.Column("published_at", sa.DateTime, nullable=True),
        sa.Column("content", sa.String()),
        sa.Column("parent_comment_id", String(32)),
        sa.Column("thread_id", String(32)),
        sa.Column("review_id", String(32)),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number", "comment_type", "comment_id"),
    )

    pull_request_labels = sa.Table(
        "pull_request_labels",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("pr_number", sa.Integer()),
        sa.Column("name", sa.String(64)),
        sa.Column("color", sa.String(16), nullable=True),
        sa.Column("description", sa.String(128), nullable=True),
        sa.Column("active", sa.Boolean(), default=True),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number", "name"),
    )

    return metadata, {
        "projects": projects,
        "repositories": repositories,
        "project_repositories": project_repositories,
        "extracted_commits": extracted_commits,
        "calculated_commits": calculated_commits,
        "extracted_patches": extracted_patches,
        "calculated_patches": calculated_patches,
        "extracted_patch_rewrites": extracted_patch_rewrites,
        "authors": authors,
        "teams": teams,
        "team_members": team_members,
        "pull_requests": pull_requests,
        "pull_request_commits": pull_request_commits,
        "pull_request_comments": pull_request_comments,
        "pull_request_labels": pull_request_labels,
    }
