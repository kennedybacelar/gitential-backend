import click
import uvicorn
from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector


@click.group()
def cli():
    pass


@click.command()
@click.argument("clone_url")
def extract_git_metrics(clone_url):
    output = DataCollector()
    extract_incremental(clone_url=clone_url, output=output)


@click.command()
@click.option("--host", "-h", "host", default="127.0.0.1")
@click.option("--port", "-p", "port", type=int, default=8080)
def public_api(host, port):
    uvicorn.run("gitential2.public_api.main:app", host=host, port=port, log_level="info")


cli.add_command(extract_git_metrics)
cli.add_command(public_api)