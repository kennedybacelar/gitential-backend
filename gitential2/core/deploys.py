from typing import Iterable

from gitential2.datatypes.deploys import Deploy

from .context import GitentialContext


def get_all_deploys(g: GitentialContext, workspace_id: int) -> Iterable[Deploy]:
    return g.backend.deploys.all(workspace_id)


def register_deploy(g: GitentialContext, workspace_id: int, deploy: Deploy) -> Deploy:
    _deploy = g.backend.deploys.create(workspace_id=workspace_id, obj=deploy)
    return _deploy


def delete_deploy_by_id(g: GitentialContext, workspace_id: int, deploy_id: str):
    g.backend.deploys.delete_deploy_by_id(workspace_id=workspace_id, deploy_id=deploy_id)


def delete_deploy_commits_by_deploy_id(g: GitentialContext, workspace_id: int, deploy_id: str):
    g.backend.deploy_commits.delete_deploy_commit_by_deploy_id(workspace_id=workspace_id, deploy_id=deploy_id)
    return True


def recalculate_deploy_commits(g: GitentialContext, workspace_id: int):
    all_deploys = g.backend.deploys.all(workspace_id)
    for deploy in all_deploys:
        if not g.backend.deploys.get_deploy_by_id(workspace_id=workspace_id, deploy_id=deploy.id):
            delete_deploy_commits_by_deploy_id(g=g, workspace_id=workspace_id, deploy_id=deploy.id)
            continue
        _create_or_update_deploy_commits(g=g, workspace_id=workspace_id, deploy_id=deploy.id)


def _get_repo_id_by_repo_name(g: GitentialContext, workspace_id: int, repo_name: str):
    pass


def _create_or_update_deploy_commits(g: GitentialContext, workspace_id: int, deploy_id: str):
    pass
