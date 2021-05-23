from typing import Optional
import typer
import uvicorn
from gitential2.settings import load_settings
from gitential2.logging import initialize_logging

from .export import app as export_app
from .usage_stats import app as usage_stats_app
from .refresh import app as refresh_app
from .tasks import app as tasks_app
from .status import app as status_app

app = typer.Typer()
app.add_typer(export_app, name="export")
app.add_typer(usage_stats_app, name="usage-stats")
app.add_typer(refresh_app, name="refresh")
app.add_typer(tasks_app, name="tasks")
app.add_typer(status_app, name="status")


@app.command("public-api")
def public_api(
    host: str = typer.Option("0.0.0.0", "--host", "-h"),
    port: int = typer.Option(7999, "--port", "-p"),
    reload: bool = False,
):
    uvicorn.run("gitential2.public_api.main:app", host=host, port=port, log_level="info", reload=reload)


def main(prog_name: Optional[str] = None):
    initialize_logging(load_settings())
    app(prog_name=prog_name)
