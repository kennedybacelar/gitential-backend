from fastapi import APIRouter

router = APIRouter(tags=["workspaces"])


@router.get("/workspaces")
async def workspaces():
    return [
        {"id": 2155, "name": "gitential-user", "role": 1, "user_id": 2090, "primary": True, "created_at": 1593403466}
    ]
