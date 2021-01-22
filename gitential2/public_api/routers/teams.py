from fastapi import APIRouter

router = APIRouter(tags=["teams"])


@router.get("/workspaces/{workspace_id}/teams")
async def teams(workspace_id: int):
    return [{"id": 246, "name": "Green Team"}, {"id": 247, "name": "Red Team"}]
