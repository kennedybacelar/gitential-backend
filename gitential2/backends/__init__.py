from typing import Optional
from gitential2.settings import GitentialSettings, BackendType
from .common import GitentialBackend
from .in_memory import InMemGitentialBackend
from .sql import SQLGitentialBackend


def init_backend(settings: GitentialSettings) -> Optional[GitentialBackend]:
    if settings.backend == BackendType.in_memory:
        print("Creating in memory backend")
        return InMemGitentialBackend(settings)
    elif settings.backend == BackendType.sql:
        print("Creating SQL backend")
        return SQLGitentialBackend(settings)
    else:
        return None
