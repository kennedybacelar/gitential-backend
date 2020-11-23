import click

from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector


@click.group()
def cli():
    pass


@click.command()
# @click.option("-o", "--output", default="json-stdout")
@click.argument("clone_url")
def extract_git_metrics(clone_url):
    output = DataCollector()
    extract_incremental(clone_url=clone_url, output=output)


cli.add_command(extract_git_metrics)