import json
from pprint import pprint

import click
import uvicorn

from gitential2.datatypes.stats import StatsRequest
from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector
from gitential2.datatypes.repositories import RepositoryInDB, GitProtocol
from gitential2.settings import load_settings
from gitential2.logging import initialize_logging
from gitential2.core import (
    init_context_from_settings,
    refresh_repository,
    recalculate_repository_values,
    refresh_repository_pull_requests,
    collect_stats,
    get_user,
    list_users,
    set_as_admin,
)
from gitential2.core.emails import send_email_to_user
from gitential2.license import check_license as check_license_
from gitential2.legacy_import import import_legacy_database
from gitential2.legacy_import import import_legacy_workspace


def protocol_from_clone_url(clone_url: str) -> GitProtocol:
    if clone_url.startswith(("git@", "ssh")):
        return GitProtocol.ssh
    else:
        return GitProtocol.https


@click.group()
@click.pass_context
def cli(ctx):
    settings = load_settings()
    initialize_logging(settings)

    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings


@click.command()
@click.argument("repo_id", type=int)
@click.argument("clone_url")
@click.pass_context
def extract_git_metrics(ctx, repo_id, clone_url):
    repository = RepositoryInDB(id=repo_id, clone_url=clone_url, protocol=protocol_from_clone_url(clone_url))
    output = DataCollector()
    extract_incremental(repository, output=output, settings=ctx.obj["settings"])


@click.command()
@click.option("--host", "-h", "host", default="127.0.0.1")
@click.option("--port", "-p", "port", type=int, default=7999)
@click.option("--reload/--no-reload", default=False)
def public_api(host, port, reload):
    uvicorn.run("gitential2.public_api.main:app", host=host, port=port, log_level="info", reload=reload)


@click.command()
@click.pass_context
def initialize_database(ctx):
    g = init_context_from_settings(ctx.obj["settings"])
    workspaces = g.backend.workspaces.all()
    for w in workspaces:
        g.backend.initialize_workspace(w.id)


@click.command(name="refresh-repository")
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.option("--repository", "-r", "repository_id", type=int)
@click.option("--force", "-f", "force_rebuild", type=bool, is_flag=True)
@click.pass_context
def refresh_repository_(ctx, workspace_id, repository_id, force_rebuild):
    g = init_context_from_settings(ctx.obj["settings"])
    g.backend.initialize_workspace(workspace_id)
    refresh_repository(g, workspace_id, repository_id=repository_id, force_rebuild=force_rebuild)


@click.command()
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.option("--repository", "-r", "repository_id", type=int)
@click.pass_context
def wip(ctx, workspace_id, repository_id):
    g = init_context_from_settings(ctx.obj["settings"])
    g.backend.initialize_workspace(workspace_id)
    df = g.backend.extracted_patches.get_repo_df(workspace_id, repository_id)
    print(df, df.dtypes)


@click.command(name="recalculate-repository-values")
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.option("--repository", "-r", "repository_id", type=int)
@click.pass_context
def recalculate_repository_values_(ctx, workspace_id, repository_id):
    g = init_context_from_settings(ctx.obj["settings"])
    recalculate_repository_values(g, workspace_id=workspace_id, repository_id=repository_id)


@click.command(name="refresh-repository-pull-requests")
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.option("--repository", "-r", "repository_id", type=int)
@click.pass_context
def refresh_repository_pull_requests_(ctx, workspace_id, repository_id):
    g = init_context_from_settings(ctx.obj["settings"])
    refresh_repository_pull_requests(g, workspace_id=workspace_id, repository_id=repository_id)


@click.command()
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.pass_context
def get_stats(ctx, workspace_id):
    stdin_text = click.get_text_stream("stdin").read()
    stats_request = StatsRequest.parse_raw(stdin_text)
    g = init_context_from_settings(ctx.obj["settings"])
    result = collect_stats(g, workspace_id, stats_request)
    print(result)


@click.command()
@click.option("--license-file-path", "-l", "license_file_path", type=str)
def check_license(license_file_path):
    license_, is_valid = check_license_(license_file_path)
    if is_valid:
        print("License is valid", license_)
    else:
        print("License is invalid or expired", license_)


@click.command(name="send-email-to-user")
@click.option("--template-name", "-t", "template_name", type=str)
@click.option("--user-id", "-u", "user_id", type=int)
@click.pass_context
def send_email_to_user_(ctx, template_name, user_id):
    g = init_context_from_settings(ctx.obj["settings"])
    user = get_user(g, user_id=user_id)
    send_email_to_user(g, user, template_name)


@click.command(name="list-users")
@click.pass_context
def list_users_(ctx):
    g = init_context_from_settings(ctx.obj["settings"])
    users = list(list_users(g))
    for u in users:
        pprint(u.dict(skip_defaults=True, exclude_none=True))


@click.command(name="set-as-admin")
@click.option("--user-id", "-u", "user_id", type=int)
@click.option("--unset", "-s", "unset", type=bool, is_flag=True)
@click.pass_context
def set_as_admin_(ctx, user_id, unset):
    g = init_context_from_settings(ctx.obj["settings"])
    set_as_admin(g, user_id, is_admin=not unset)


@click.command(name="import-legacy-db")
@click.option("--users", "-u", "users_file", type=str)
@click.option("--secrets", "-s", "secrets_file", type=str)
@click.option("--accounts", "-a", "accounts_file", type=str)
@click.option("--collaborators", "-c", "collaborators_file", type=str)
@click.pass_context
def import_legacy_db_(ctx, users_file, secrets_file, accounts_file, collaborators_file):
    def _load_list(filename):
        try:
            return json.loads(open(filename, "r").read())
        except Exception as e:  # pylint: disable=broad-except
            print(e)
            return []

    legacy_users = _load_list(users_file)
    legacy_secrects = _load_list(secrets_file)
    legacy_accounts = _load_list(accounts_file)
    legacy_collaborators = _load_list(collaborators_file)

    g = init_context_from_settings(ctx.obj["settings"])
    import_legacy_database(g, legacy_users, legacy_secrects, legacy_accounts, legacy_collaborators)


@click.command(name="import-legacy-workspace")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.option("--projectrepos", "-pr", "projectrepos", type=str)
@click.option("--teamauthors", "-ta", "teamauthors", type=str)
@click.option("--aliases", "-al", "aliases", type=str)
@click.option("--account", "-a", "account", type=str)
@click.pass_context
def import_legacy_workspace_(
    ctx, workspace_id, projectrepos, teamauthors, account, aliases
):  # pylint: disable=unused-argument
    def _load_list(filename):  # pylint: disable=unused-variable
        try:
            return json.loads(open(filename, "r").read())
        except Exception as e:  # pylint: disable=broad-except
            print(e)
            return []

    project_repos = _load_list(projectrepos)
    account = _load_list(account)
    team_authors = _load_list(teamauthors)
    aliases = _load_list(aliases)
    g = init_context_from_settings(ctx.obj["settings"])
    import_legacy_workspace(
        g,
        workspace_id=1,
        legacy_teams_authors=team_authors,
        legacy_aliases=aliases,
        legacy_projects_repos=project_repos,
    )


cli.add_command(import_legacy_db_)
cli.add_command(import_legacy_workspace_)

cli.add_command(extract_git_metrics)
cli.add_command(public_api)
cli.add_command(initialize_database)
cli.add_command(refresh_repository_)
cli.add_command(refresh_repository_pull_requests_)
cli.add_command(get_stats)
cli.add_command(recalculate_repository_values_)
cli.add_command(check_license)
cli.add_command(wip)
cli.add_command(send_email_to_user_)
cli.add_command(list_users_)
cli.add_command(set_as_admin_)
