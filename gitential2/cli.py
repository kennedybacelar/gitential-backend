import json
import os
import csv
from pprint import pprint

import click
import uvicorn
from fastapi.encoders import jsonable_encoder
from structlog import get_logger
from gitential2.datatypes.credentials import CredentialType
from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector
from gitential2.datatypes.repositories import RepositoryInDB, GitProtocol, RepositoryUpdate
from gitential2.settings import load_settings
from gitential2.logging import initialize_logging
from gitential2.core.context import init_context_from_settings
from gitential2.core.projects import schedule_project_refresh
from gitential2.core.refresh import refresh_repository, refresh_repository_pull_requests
from gitential2.core.repositories import list_repositories
from gitential2.core.users import get_user, list_users, set_as_admin
from gitential2.core.calculations import recalculate_repository_values
from gitential2.core.emails import send_email_to_user
from gitential2.license import check_license as check_license_
from gitential2.legacy_import import import_legacy_database
from gitential2.legacy_import import import_legacy_workspace
from gitential2.core.tasks import configure_celery
from gitential2.core.context import GitentialContext
from gitential2.core.stats import calculate_workspace_usage_statistics, calculate_user_statistics
from gitential2.core.subscription import set_as_professional

logger = get_logger(__name__)


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
    logger.info("refreshing PRs", workspace_id=workspace_id, repo_id=repository_id)

    refresh_repository_pull_requests(g, workspace_id=workspace_id, repository_id=repository_id)


@click.command(name="refresh-workspace-pull-requests")
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.pass_context
def refresh_workspace_pull_requests_(ctx, workspace_id):
    g = init_context_from_settings(ctx.obj["settings"])
    for repo in list_repositories(g, workspace_id):
        if repo.protocol == GitProtocol.https and repo.integration_name is not None:
            logger.info("refreshing PRs", workspace_id=workspace_id, repo_name=repo.name, repo_id=repo.id)
            refresh_repository_pull_requests(g, workspace_id=workspace_id, repository_id=repo.id)


# @click.command()
# @click.option("--workspace", "-w", "workspace_id", type=int)
# @click.pass_context
# def get_stats(ctx, workspace_id):
#     stdin_text = click.get_text_stream("stdin").read()
#     stats_request = StatsRequest.parse_raw(stdin_text)
#     g = init_context_from_settings(ctx.obj["settings"])
#     result = collect_stats(g, workspace_id, stats_request)
#     print(result)


@click.command()
@click.option("--workspace", "-w", "workspace_id", type=int)
@click.pass_context
def workspace_usage_stats(ctx, workspace_id):
    g = init_context_from_settings(ctx.obj["settings"])
    result = calculate_workspace_usage_statistics(g, workspace_id)
    print(json.dumps(jsonable_encoder(result)))


@click.command()
@click.option("--user", "-u", "user_id", type=int)
@click.pass_context
def user_usage_stats(ctx, user_id):
    g = init_context_from_settings(ctx.obj["settings"])
    result = calculate_user_statistics(g, user_id)
    print(json.dumps(jsonable_encoder(result), indent=2))


@click.command()
@click.pass_context
def usage_stats(ctx):
    g = init_context_from_settings(ctx.obj["settings"])
    result = []
    for user in g.backend.users.all():
        result.append(calculate_user_statistics(g, user_id=user.id))
    print(json.dumps(jsonable_encoder(result), indent=2))


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
    legacy_users = _load_list(users_file)
    legacy_secrects = _load_list(secrets_file)
    legacy_accounts = _load_list(accounts_file)
    legacy_collaborators = _load_list(collaborators_file)

    g = init_context_from_settings(ctx.obj["settings"])
    import_legacy_database(g, legacy_users, legacy_secrects, legacy_accounts, legacy_collaborators)


@click.command(name="import-legacy-workspace-bulk")
@click.option("--folder", "-f", "folder", type=str)
@click.pass_context
def import_legacy_workspace_bulk(ctx, folder):  # pylint: disable=unused-argument,too-many-arguments,unused-variable
    dirs = os.listdir(os.getcwd() + "/" + folder)
    g = init_context_from_settings(ctx.obj["settings"])
    for directory in dirs:
        workspace_id = int(directory.split("_")[1])
        path = folder + "/" + directory + "/"
        aliases_ = _load_list(path + "alias.json")
        authors_ = _load_list(path + "author.json")
        # account_ = _load_list(account)
        projects_ = _load_list(path + "project.json")
        project_repos_ = _load_list(path + "project_repo.json")
        account_repos_ = _load_list(path + "repo.json")
        team_authors_ = _load_list(path + "teams_author.json")
        teams_ = _load_list(path + "teams.json")

        import_legacy_workspace(
            g,
            workspace_id,
            legacy_projects_repos=project_repos_,
            legacy_aliases=aliases_,
            legacy_teams=teams_,
            legacy_teams_authors=team_authors_,
            legacy_authors=authors_,
            legacy_account_repos=account_repos_,
            legacy_projects=projects_,
        )  # pylint: disable=too-many-arguments


@click.command(name="import-legacy-workspace")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.option("--projectrepos", "-pr", "projectrepos", type=str)
@click.option("--teamauthors", "-ta", "teamauthors", type=str)
@click.option("--accountrepos", "-ar", "accountrepos", type=str)
@click.option("--authors", "-au", "authors", type=str)
@click.option("--teams", "-t", "teams", type=str)
@click.option("--projects", "-p", "projects", type=str)
@click.option("--aliases", "-al", "aliases", type=str)
@click.option("--account", "-ac", "account", type=str)
@click.pass_context
def import_legacy_workspace_(
    ctx, workspace_id, projectrepos, teamauthors, account, aliases, teams, authors, accountrepos, projects
):  # pylint: disable=unused-argument,too-many-arguments,unused-variable
    teams_ = _load_list(teams)
    authors_ = _load_list(authors)
    project_repos_ = _load_list(projectrepos)
    # account_ = _load_list(account)
    account_repos_ = _load_list(accountrepos)
    team_authors_ = _load_list(teamauthors)
    aliases_ = _load_list(aliases)
    projects_ = _load_list(projects)
    g = init_context_from_settings(ctx.obj["settings"])
    import_legacy_workspace(
        g,
        workspace_id=1,
        legacy_projects_repos=project_repos_,
        legacy_aliases=aliases_,
        legacy_teams=teams_,
        legacy_teams_authors=team_authors_,
        legacy_authors=authors_,
        legacy_account_repos=account_repos_,
        legacy_projects=projects_,
    )  # pylint: disable=too-many-arguments


def _load_list(filename):  # pylint: disable=unused-variable
    try:
        return json.loads(open(os.getcwd() + "/" + filename, "r").read())
    except Exception as e:  # pylint: disable=broad-except
        print(e)
        return []


@click.command(name="schedule-project-refresh")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.option("--project-id", "-p", "project_id", type=int)
@click.option("--force", "-f", "force_rebuild", type=bool, is_flag=True)
@click.pass_context
def schedule_project_refresh_(ctx, workspace_id, project_id, force_rebuild):
    g = init_context_from_settings(ctx.obj["settings"])
    configure_celery(g.settings)
    schedule_project_refresh(g, workspace_id, project_id, force_rebuild)


@click.command(name="list-projects")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.pass_context
def list_projects_(ctx, workspace_id):
    g = init_context_from_settings(ctx.obj["settings"])
    for project in g.backend.projects.all(workspace_id):
        print(project.dict(exclude={"extra"}))


@click.command(name="list-used-repos")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.pass_context
def list_used_repos_(ctx, workspace_id):
    g = init_context_from_settings(ctx.obj["settings"])
    for repo in list_repositories(g, workspace_id):
        print(repo.dict(exclude={"extra"}))


@click.command(name="refresh-all-workspaces")
@click.option("--force", "-f", "force_rebuild", type=bool, is_flag=True)
@click.pass_context
def refresh_all_workspaces_(ctx, force_rebuild):
    g = init_context_from_settings(ctx.obj["settings"])
    configure_celery(g.settings)

    workpsaces = g.backend.workspaces.all()
    for ws in workpsaces:
        logger.info("refreshing workspace", workspace_id=ws.id, workspace_name=ws.name)
        _refresh_workspace(g, ws.id, force_rebuild)


@click.command(name="set-as-professional")
@click.option("--user-id", "-u", "user_id", type=int)
@click.option("--number-of-developers", "-d", "number_of_developers", type=int)
@click.pass_context
def set_as_professional_(ctx, user_id: int, number_of_developers: int):
    g = init_context_from_settings(ctx.obj["settings"])
    subscription = set_as_professional(g, user_id, number_of_developers)
    print(subscription)


@click.command(name="refresh-workspace")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.option("--force", "-f", "force_rebuild", type=bool, is_flag=True)
@click.pass_context
def refresh_workspace_(ctx, workspace_id, force_rebuild):
    g = init_context_from_settings(ctx.obj["settings"])
    configure_celery(g.settings)

    _refresh_workspace(g, workspace_id, force_rebuild)


def _refresh_workspace(g: GitentialContext, workspace_id: int, force_rebuild):
    for project in g.backend.projects.all(workspace_id):
        logger.info("refreshing project", workspace_id=workspace_id, project_id=project.id, project_name=project.name)
        schedule_project_refresh(g, workspace_id, project.id, force_rebuild)


def _load_fix_file():
    ret = {}
    with open(
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "credential_fix.csv"), "r"
    ) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ret[(row["account_id"], row["clone_url"])] = row
    return ret


def _all_credentials_by_owner_and_name(g: GitentialContext):
    ret = {}
    for credential in g.backend.credentials.all():
        if credential.type == CredentialType.keypair:
            if (credential.owner_id, credential.name) in ret:
                logger.warning("Credential with the same name", owner_id=credential.owner_id, name=credential.name)
            ret[(credential.owner_id, credential.name)] = credential
    return ret


@click.command(name="fix-ssh-repo-credentials")
@click.pass_context
def fix_ssh_repo_credentials(ctx):

    g = init_context_from_settings(ctx.obj["settings"])

    fixes = _load_fix_file()

    all_workspaces = list(g.backend.workspaces.all())
    all_keypairs = _all_credentials_by_owner_and_name(g)

    logger.info("all_fixes", fixes=fixes.keys())
    logger.info("all_keypairs", all_credentials=all_keypairs.keys())

    for workspace in all_workspaces:
        logger.info("Fixing ssh repositories in workspace", workspace_id=workspace.id)
        repositories = g.backend.repositories.all(workspace.id)
        for repository in repositories:
            if repository.protocol == GitProtocol.https:
                continue
            elif (str(workspace.id), repository.clone_url) in fixes and repository.credential_id is None:
                fix = fixes[(str(workspace.id), repository.clone_url)]

                try:
                    credential = all_keypairs[(workspace.created_by, fix["name"])]
                except KeyError:
                    logger.error("Missing credential ", repository=repository, fix=fix, workspace_id=workspace.id)
                    continue

                logger.info(
                    "Updating repository credential_id",
                    workspace_id=workspace.id,
                    clone_url=repository.clone_url,
                    repo_id=repository.id,
                    credential_id=credential.id,
                    credential_name=credential.name,
                )
                repo_update = RepositoryUpdate(**repository.dict())
                repo_update.credential_id = credential.id
                g.backend.repositories.update(workspace.id, repository.id, repo_update)
            else:
                logger.warning("Missing fix ", workspace_id=workspace.id, repository=repository)


cli.add_command(import_legacy_db_)
cli.add_command(import_legacy_workspace_)
cli.add_command(import_legacy_workspace_bulk)
cli.add_command(extract_git_metrics)
cli.add_command(public_api)
cli.add_command(initialize_database)
cli.add_command(refresh_repository_)
cli.add_command(refresh_repository_pull_requests_)
cli.add_command(refresh_workspace_pull_requests_)
# cli.add_command(get_stats)
cli.add_command(recalculate_repository_values_)
cli.add_command(check_license)
cli.add_command(wip)
cli.add_command(send_email_to_user_)
cli.add_command(list_users_)
cli.add_command(set_as_admin_)
cli.add_command(schedule_project_refresh_)
cli.add_command(list_projects_)
cli.add_command(list_used_repos_)
cli.add_command(refresh_all_workspaces_)
cli.add_command(refresh_workspace_)
cli.add_command(fix_ssh_repo_credentials)
cli.add_command(workspace_usage_stats)
cli.add_command(user_usage_stats)
cli.add_command(set_as_professional_)
cli.add_command(usage_stats)
