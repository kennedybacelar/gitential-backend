from typing import List


class Workspace:
    pass


class WorkspaceBackend:
    pass


class Credential:
    pass


class Project:
    pass


class Repository:
    pass


class RepositorySource:
    pass


class WorkspaceManager:
    def __init__(self, workspace: Workspace, backend: WorkspaceBackend):
        self.workspace = workspace
        self.backend = backend

    def list_repository_sources(self) -> List[RepositorySource]:
        pass

    def list_projects(self) -> List[Project]:
        return []

    def list_available_repositories(self) -> List[Repository]:
        return []

    def get_project(self, project_id: int) -> Project:
        pass

    def create_project(self, project: Project) -> Project:
        pass

    def update_project(self, project: Project) -> Project:
        pass

    def delete_project(self, project_id: int) -> bool:
        return False

    def list_project_repositories(self, project_id: int) -> List[Repository]:
        return []

    def analyze_project(self, project_id: int) -> bool:
        return True
