import sys
from typing import List
from pydantic import ValidationError

from structlog import get_logger
from gitential2.core import GitentialContext

from gitential2.datatypes.repositories import RepositoryCreate
from gitential2.datatypes.projects import ProjectCreate
from gitential2.datatypes.workspaces import WorkspaceCreate
from gitential2.datatypes.project_repositories import ProjectRepositoryCreate

logger = get_logger(__name__)


def import_project_and_repos(
    g: GitentialContext,
    workspace_id: int,
    legacy_projects_repos: List[dict],
):
    def search_old_id(items, obj_id):
        for item in items:
            if item["old_id"] == obj_id:
                return item["new_obj"]
        return False

    added_repoids = []
    added_projectids = []
    g.backend.initialize_workspace(workspace_id)
    for project_repo in legacy_projects_repos:
        existing_repo_obj = search_old_id(added_repoids, project_repo["repo"]["id"])
        if not existing_repo_obj:
            existing_repo_obj = _import_repo(g, project_repo["repo"], workspace_id)
            added_repoids.append({"old_id": project_repo["repo"]["id"], "new_obj": existing_repo_obj})
        existing_project_obj = search_old_id(added_projectids, project_repo["repo"]["id"])
        if not existing_project_obj:
            existing_project_obj = _import_repo(g, project_repo["repo"], workspace_id)
            added_projectids.append({"old_id": project_repo["project"]["id"], "new_obj": existing_project_obj})
        _create_project_repo(
            g, repo_id=existing_repo_obj.id, project_id=existing_project_obj.id, workspace_id=workspace_id
        )


def get_repo_name(input_str: str):
    proto = str.split("://")[0]
    if proto == "ssh":
        return str.split(":")[1].split("/")[1].split(".")[0]
    elif proto == "https":
        return str.split("/")[-2]
    else:
        print("notimplemented repo name gather", input_str)
        sys.exit(1)


def get_namespace(input_str: str):
    proto = str.split("://")[0]
    if proto == "ssh":
        return str.split(":")[1].split("/")[0]
    elif proto == "https":
        return str.split("/")[-2]
    else:
        print("notimplemented namespace gather", input_str)
        sys.exit(1)


def get_integration_type(input_str: str):
    if "bitb" in input_str:
        return "bitbucket"
    elif "github" in input_str:
        return "github"
    else:
        print("notimplemented integration type", input_str)
        sys.exit(1)


def _import_repo(g: GitentialContext, repo: dict, workspace_id: int):
    try:
        repo_create = RepositoryCreate(
            clone_url=repo["clone_url"],
            protocol=repo["clone_url"].split("://")[0],
            name=get_repo_name(repo["clone_url"]),
            namespace=get_namespace(repo["clone_url"]),
            private=repo["private"],
            integration_type=get_integration_type(repo["clone_url"]),
            created_at=repo["created_at"],
            updated_at=repo["updated_at"],
        )
        logger.info("Importing repo", workspace_id=workspace_id)
        return g.backend.repositories.create(workspace_id, repo_create)
    except ValidationError as e:
        print(f"Failed to import repo {repo['clone_url']}", e)


def _import_project(g: GitentialContext, project: dict, workspace_id: int):
    try:
        project_create = ProjectCreate(
            name=project["name"],
            shareable=project["shareable"],
            pattern=project["pattern"] if project["pattern"] else None,
            created_at=project["created_at"],
            updated_at=project["updated_at"],
            extra=None,
        )
        logger.info("Importing project", workspace_id=workspace_id)
        return g.backend.projects.create(workspace_id, project_create)
    except ValidationError as e:
        print(f"Failed to import project {project['name']}", e)


def _create_project_repo(g: GitentialContext, project_id: int, repo_id: int, workspace_id: int):
    try:
        project_repo_create = ProjectRepositoryCreate(
            project_id=project_id,
            repo_id=repo_id,
        )
        logger.info("Importing project repo", workspace_id=workspace_id)
        g.backend.project_repositories.create(workspace_id, project_repo_create)
    except ValidationError as e:
        print(f'Failed to import project repo {e}')


def _import_workspace(g: GitentialContext, workspace: dict):
    try:
        workspace_create = WorkspaceCreate(
            name=workspace["name"],
            created_by=workspace["owner"]["id"],
            created_at=workspace["created_at"],
            updated_at=workspace["updated_at"],
        )
        logger.info("Importing workspace", workspace_id=workspace["id"])
        return g.backend.workspaces.create(workspace_create)
    except ValidationError as e:
        print(f"Failed to import workspace {workspace['id']}", e)
