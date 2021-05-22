from pathlib import Path
import typer
from gitential2.settings import load_settings
from gitential2.core.context import init_context_from_settings, GitentialContext


def get_context() -> GitentialContext:
    return init_context_from_settings(load_settings())


def validate_directory_exists(d: Path):
    if not d.exists():
        raise typer.Exit(1)
    if not d.is_dir():
        raise typer.Exit(2)
