from typing import Iterable

from gitential2.datatypes.deploys import Deploy

from .context import GitentialContext


def get_all_deploys(g: GitentialContext, workspace_id: int) -> Iterable[Deploy]:
    return g.backend.deploys.all(workspace_id)


def register_deploy(g: GitentialContext, workspace_id: int, deploy: Deploy) -> Deploy:
    _deploy = g.backend.deploys.create(workspace_id=workspace_id, obj=deploy)
    return _deploy
