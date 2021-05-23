import json
import typer
from structlog import get_logger
from fastapi.encoders import jsonable_encoder

from gitential2.core.stats import calculate_workspace_usage_statistics, calculate_user_statistics
from .common import get_context

app = typer.Typer()
logger = get_logger(__name__)


@app.command("workspace")
def workspace_usage_stats(workspace_id: int):
    g = get_context()

    result = calculate_workspace_usage_statistics(g, workspace_id)
    print(json.dumps(jsonable_encoder(result)))


@app.command("user")
def user_usage_stats(user_id: int):
    g = get_context()
    result = calculate_user_statistics(g, user_id)
    print(json.dumps(jsonable_encoder(result), indent=2))


@app.command("global")
def usage_stats():
    g = get_context()
    result = []
    for user in g.backend.users.all():
        result.append(calculate_user_statistics(g, user_id=user.id))
    print(json.dumps(jsonable_encoder(result), indent=2))
