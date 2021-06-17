from typing import Optional
import typer
from structlog import get_logger

from .common import get_context, OutputFormat, print_results

app = typer.Typer()

logger = get_logger(__name__)


@app.command("list")
def list_scheduled_emails(
    format_: OutputFormat = typer.Option(OutputFormat.tabulate, "--format"),
    fields: Optional[str] = None,
):
    g = get_context()
    results = list(g.backend.email_log.all())
    print_results(results, format_=format_, fields=fields)
