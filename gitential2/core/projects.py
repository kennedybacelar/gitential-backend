from typing import List
from structlog import get_logger
from gitential2.datatypes.projects import (
    ProjectInDB,
    ProjectCreate,
    ProjectUpdate,
    ProjectCreateWithRepositories,
    ProjectUpdateWithRepositories,
)
from gitential2.datatypes.repositories import RepositoryCreate


from .context import GitentialContext
from .statuses import get_repository_status, persist_repository_status, has_repository_status

logger = get_logger(__name__)


def list_projects(g: GitentialContext, workspace_id: int) -> List[ProjectInDB]:
    return list(g.backend.projects.all(workspace_id=workspace_id))


def create_project(
    g: GitentialContext, workspace_id: int, project_create: ProjectCreateWithRepositories
) -> ProjectInDB:
    project = g.backend.projects.create(workspace_id, ProjectCreate(**project_create.dict(exclude={"repos"})))

    _update_project_repos(g, workspace_id=workspace_id, project=project, repos=project_create.repos)
    schedule_project_refresh(g, workspace_id=workspace_id, project_id=project.id)
    return project


def get_project(g: GitentialContext, workspace_id: int, project_id: int) -> ProjectInDB:
    return g.backend.projects.get_or_error(workspace_id=workspace_id, id_=project_id)


def update_project(
    g: GitentialContext, workspace_id: int, project_id: int, project_update: ProjectUpdateWithRepositories
) -> ProjectInDB:
    project = g.backend.projects.update(
        workspace_id=workspace_id, id_=project_id, obj=ProjectUpdate(**project_update.dict(exclude={"repos"}))
    )
    _update_project_repos(g, workspace_id=workspace_id, project=project, repos=project_update.repos)
    schedule_project_refresh(g, workspace_id, project_id)
    return project


def delete_project(g: GitentialContext, workspace_id: int, project_id: int) -> bool:
    g.backend.project_repositories.update_project_repositories(workspace_id, project_id, [])
    g.backend.projects.delete(workspace_id, project_id)
    return True


def schedule_project_refresh(g: GitentialContext, workspace_id: int, project_id: int, force_rebuild: bool = False):
    for repo_id in g.backend.project_repositories.get_repo_ids_for_project(
        workspace_id=workspace_id, project_id=project_id
    ):
        schedule_repository_refresh(g, workspace_id=workspace_id, repository_id=repo_id, force_rebuild=force_rebuild)


def schedule_repository_refresh(
    g: GitentialContext, workspace_id: int, repository_id: int, force_rebuild: bool = False
):

    if (
        force_rebuild
        or not has_repository_status(g, workspace_id, repository_id)
        or get_repository_status(g, workspace_id, repository_id).done
    ):
        repo_status = get_repository_status(g, workspace_id, repository_id)
        persist_repository_status(g, workspace_id, repository_id, repo_status.reset())

        # pylint: disable=import-outside-toplevel,cyclic-import
        from .tasks import refresh_repository_task

        refresh_repository_task.delay(g.settings.dict(), workspace_id, repository_id, force_rebuild)
    else:
        logger.info(
            "Skipping repo refresh task, already scheduled", workspace_id=workspace_id, repository_id=repository_id
        )


def _update_project_repos(g: GitentialContext, workspace_id: int, project: ProjectInDB, repos=List[RepositoryCreate]):
    repositories = [g.backend.repositories.create_or_update(workspace_id=workspace_id, obj=r) for r in repos]
    return g.backend.project_repositories.update_project_repositories(
        workspace_id=workspace_id, project_id=project.id, repo_ids=[r.id for r in repositories]
    )
