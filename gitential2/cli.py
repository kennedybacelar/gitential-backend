import click
import uvicorn

from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector
from gitential2.datatypes.repositories import GitRepository
from gitential2.settings import load_settings
from gitential2.logging import initialize_logging
from gitential2.core import Gitential


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
    repository = GitRepository(id=repo_id, clone_url=clone_url)
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
    g = Gitential.from_config(ctx.obj["settings"])
    workspaces = g.backend.workspaces.all()
    for w in workspaces:
        g.backend.initialize_workspace(w.id)


cli.add_command(extract_git_metrics)
cli.add_command(public_api)
cli.add_command(initialize_database)
