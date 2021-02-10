from fastapi import Request, Depends
from gitential2.datatypes import UserInDB
from gitential2.core import WorkspaceCtrl


class GitentialCore:
    def __init__(self, request: Request):
        self.gitential = request.app.state.gitential

    def __getattr__(self, name):
        return getattr(self.gitential, name)


class OAuth:
    def __init__(self, request: Request):
        self.oauth = request.app.state.oauth

    def __getattr__(self, name):
        return getattr(self.oauth, name)


def current_user(request: Request):
    if "current_user" in request.session:
        return UserInDB(**request.session["current_user"])
    return None


def workspace_ctrl(workspace_id: int, gitential: GitentialCore = Depends()) -> WorkspaceCtrl:
    return gitential.get_workspace_ctrl(workspace_id=workspace_id)
