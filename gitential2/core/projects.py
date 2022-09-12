from typing import List, Optional
from structlog import get_logger
from gitential2.datatypes.projects import (
    ProjectInDB,
    ProjectCreate,
    ProjectUpdate,
    ProjectCreateWithRepositories,
    ProjectUpdateWithRepositories,
)
from gitential2.datatypes.repositories import RepositoryCreate, RepositoryUpdate  # , RepositoryStatus
from gitential2.datatypes.sprints import Sprint

from .refresh_v2 import refresh_project
from .context import GitentialContext
from ..datatypes.its_projects import ITSProjectUpdate, ITSProjectCreate, ITSProjectInDB

logger = get_logger(__name__)


def list_projects(g: GitentialContext, workspace_id: int) -> List[ProjectInDB]:
    return list(g.backend.projects.all(workspace_id=workspace_id))


def create_project_without_repos(g: GitentialContext, workspace_id: int, project_create: ProjectCreate) -> ProjectInDB:
    return g.backend.projects.create(workspace_id, project_create)


def update_project_without_repos(
    g: GitentialContext, workspace_id: int, project_id: int, project_update: ProjectUpdate
) -> ProjectInDB:
    return g.backend.projects.update(workspace_id=workspace_id, id_=project_id, obj=project_update)


def create_project(
    g: GitentialContext, workspace_id: int, project_create: ProjectCreateWithRepositories
) -> ProjectInDB:
    project = create_project_without_repos(
        g, workspace_id, ProjectCreate(**project_create.dict(exclude={"repos", "its_projects"}))
    )

    _update_project_repos(g, workspace_id=workspace_id, project=project, repos=project_create.repos)
    _update_project_its_projects(
        g, workspace_id=workspace_id, project=project, its_projects=project_create.its_projects
    )
    refresh_project(g, workspace_id=workspace_id, project_id=project.id)

    return project


def get_project(g: GitentialContext, workspace_id: int, project_id: int) -> ProjectInDB:
    return g.backend.projects.get_or_error(workspace_id=workspace_id, id_=project_id)


def update_project(
    g: GitentialContext,
    workspace_id: int,
    project_id: int,
    project_update: ProjectUpdateWithRepositories,
) -> ProjectInDB:
    project = update_project_without_repos(
        g, workspace_id, project_id, ProjectUpdate(**project_update.dict(exclude={"repos", "its_projects"}))
    )

    _update_project_repos(g, workspace_id=workspace_id, project=project, repos=project_update.repos)
    _update_project_its_projects(
        g, workspace_id=workspace_id, project=project, its_projects=project_update.its_projects
    )
    refresh_project(g, workspace_id, project_id)

    return project


def delete_project(g: GitentialContext, workspace_id: int, project_id: int) -> bool:
    g.backend.project_repositories.update_project_repositories(workspace_id, project_id, [])
    g.backend.project_its_projects.update_its_projects(workspace_id, project_id, [])
    g.backend.projects.delete(workspace_id, project_id)
    return True


def _update_project_repos(g: GitentialContext, workspace_id: int, project: ProjectInDB, repos: List[RepositoryCreate]):
    repositories = [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id=workspace_id, obj=r) for r in repos
    ]
    return g.backend.project_repositories.update_project_repositories(
        workspace_id=workspace_id, project_id=project.id, repo_ids=[r.id for r in repositories]
    )


def _update_project_its_projects(
    g: GitentialContext,
    workspace_id: int,
    project: ProjectInDB,
    its_projects: Optional[List[ITSProjectCreate]],
):
    its_p: List[ITSProjectInDB] = (
        [g.backend.its_projects.create_or_update_by_api_url(workspace_id=workspace_id, obj=r) for r in its_projects]
        if its_projects is not None
        else []
    )
    return g.backend.project_its_projects.update_its_projects(
        workspace_id=workspace_id, project_id=project.id, itsp_ids=[r.id for r in its_p]
    )


def update_sprint_by_project_id(g: GitentialContext, workspace_id: int, project_id: int, sprint: Sprint) -> bool:
    return g.backend.projects.update_sprint_by_project_id(
        workspace_id=workspace_id, project_id=project_id, sprint=sprint
    )


def add_repos_to_project(
    g: GitentialContext, workspace_id: int, project_id: int, repos_to_add: List[RepositoryCreate]
) -> List[int]:
    repositories = [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id=workspace_id, obj=r) for r in repos_to_add
    ]
    return g.backend.project_repositories.add_project_repositories(
        workspace_id=workspace_id, project_id=project_id, repo_ids_to_add=[r.id for r in repositories]
    )


def remove_repos_to_project(
    g: GitentialContext, workspace_id: int, project_id: int, repos_to_remove: List[RepositoryUpdate]
) -> List[int]:
    repositories = [
        g.backend.repositories.create_or_update_by_clone_url(workspace_id=workspace_id, obj=r) for r in repos_to_remove
    ]
    return g.backend.project_repositories.remove_project_repositories(
        workspace_id=workspace_id, project_id=project_id, repo_ids_to_remove=[r.id for r in repositories]
    )


def add_its_projects_to_project(
    g: GitentialContext, workspace_id: int, project_id: int, its_projects_to_add: List[ITSProjectCreate]
) -> List[int]:
    its_projects = [
        g.backend.its_projects.create_or_update_by_api_url(workspace_id=workspace_id, obj=r)
        for r in its_projects_to_add
    ]
    return g.backend.project_its_projects.add_its_projects(
        workspace_id=workspace_id, project_id=project_id, itsp_ids_to_add=[itsp.id for itsp in its_projects]
    )


def remove_its_projects_from_project(
    g: GitentialContext, workspace_id: int, project_id: int, its_projects_to_remove: List[ITSProjectUpdate]
) -> List[int]:
    its_projects = [
        g.backend.its_projects.create_or_update_by_api_url(workspace_id=workspace_id, obj=r)
        for r in its_projects_to_remove
    ]
    return g.backend.project_its_projects.remove_its_projects(
        workspace_id=workspace_id, project_id=project_id, itsp_ids_to_remove=[itsp.id for itsp in its_projects]
    )
