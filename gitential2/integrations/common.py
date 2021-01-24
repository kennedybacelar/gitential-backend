# from abc import ABC, abstractmethod
# from typing import List
# from gitential2.settings import GitentialSettings, RepositorySourceSettings
# from gitential2.datatypes.repositories import GitRepository

# # from gitential2.datatypes.credentials import Credential
# class Credential:
#     pass


# class RepositorySource(ABC):
#     def __init__(self, name: str, app_settings: GitentialSettings):
#         self.name = name
#         self._settings = app_settings.repository_sources[name]
#         self._app_settings = app_settings

#     @abstractmethod
#     def authentication_url(self, frontend_url) -> str:
#         pass

#     @abstractmethod
#     def get_available_private_repositories(self, credentials: Credential) -> List[GitRepository]:
#         pass

#     @abstractmethod
#     def get_available_public_repositories(self, query: str, credentials: Credential) -> List[GitRepository]:
#         pass
