from fastapi import APIRouter, Depends, Request
from fastapi.exceptions import HTTPException
from gitential2.datatypes.users import UserPublic
from gitential2.core import GitentialContext, get_user
from ..dependencies import gitential_context

router = APIRouter(tags=["users"])


@router.get("/users")
def list_users():
    pass


@router.get("/users/me", response_model=UserPublic)
def get_current_users(
    request: Request,
    g: GitentialContext = Depends(gitential_context),
):
    current_user_id = request.session.get("current_user_id")
    if current_user_id:
        user_in_db = get_user(g, current_user_id)
        if user_in_db:
            return user_in_db
    raise HTTPException(404, "User not found.")
