from gitential2.settings import GitentialSettings
from .abc import Gitential, WorkspaceCtrl
from .base import GitentialImpl
from .workspace_ctrl import WorkspaceCtrlImpl


def init_from_settings(settings: GitentialSettings) -> Gitential:
    return GitentialImpl.from_config(settings)
