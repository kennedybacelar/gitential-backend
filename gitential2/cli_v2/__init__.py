import typer
from .export import app as export_app

app = typer.Typer()
app.add_typer(export_app, name="export")


def main():
    app()
