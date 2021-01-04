import click
import uvicorn
from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector
from gitential2.datatypes import GitRepository
from gitential2.settings import GitentialSettings
from gitential2.logging import initialize_logging


@click.group()
@click.pass_context
def cli(ctx):
    settings = GitentialSettings()
    initialize_logging(settings)

    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings


@click.command()
@click.argument("repo_id", type=int)
@click.argument("clone_url")
@click.pass_context
def extract_git_metrics(ctx, repo_id, clone_url):
    print("!!!!", ctx.obj["settings"].log_level)
    repository = GitRepository(repo_id=repo_id, clone_url=clone_url)
    output = DataCollector()
    extract_incremental(repository, output=output, settings=ctx.obj["settings"])


@click.command()
@click.option("--host", "-h", "host", default="127.0.0.1")
@click.option("--port", "-p", "port", type=int, default=8080)
def public_api(host, port):
    uvicorn.run("gitential2.public_api.main:app", host=host, port=port, log_level="info")


cli.add_command(extract_git_metrics)
cli.add_command(public_api)