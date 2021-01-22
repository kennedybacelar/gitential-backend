from abc import ABC, abstractmethod
from gitential2.settings import GitentialSettings


class GitentialBackend(ABC):
    def __init__(self, settings: GitentialSettings):
        self._app_settings = settings
