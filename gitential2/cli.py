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
)
from gitential2.license import check_license as check_license_


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
@click.pass_context
def refresh_repository_(ctx, workspace_id, repository_id):
    g = init_context_from_settings(ctx.obj["settings"])
    g.backend.initialize_workspace(workspace_id)
    refresh_repository(g, workspace_id, repository_id=repository_id, force_rebuild=False)


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


cli.add_command(extract_git_metrics)
cli.add_command(public_api)
cli.add_command(initialize_database)
cli.add_command(refresh_repository_)
cli.add_command(refresh_repository_pull_requests_)
cli.add_command(get_stats)
cli.add_command(recalculate_repository_values_)
cli.add_command(check_license)
cli.add_command(wip)
