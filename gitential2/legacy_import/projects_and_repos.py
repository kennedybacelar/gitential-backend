from typing import List
from gitential2.core import GitentialContext


def import_project_and_repos(
    g: GitentialContext,
    workspace_id: int,
    legacy_projects: List[dict],
    legacy_repositories: List[dict],
    legacy_projects_repos: List[dict],
):  # pylint: disable=unused-argument
    # We assume the workspace is already exists in the workspaces table
    # We're going to drop and recreate the ws_{workspace_id} shema

    # _recreate_schema(g, workspace_id)
    pass
