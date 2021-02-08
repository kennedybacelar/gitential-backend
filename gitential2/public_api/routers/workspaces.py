from fastapi import APIRouter, Depends
from ..dependencies import current_user, GitentialCore

router = APIRouter(tags=["workspaces"])


@router.get("/workspaces")
def workspaces(current_user=Depends(current_user), gitential: GitentialCore = Depends()):
    return gitential.get_accessible_workspaces(current_user=current_user)
    # return [
    #     {"id": 2155, "name": "gitential-user", "role": 1, "user_id": 2090, "primary": True, "created_at": 1593403466}
    # ]
