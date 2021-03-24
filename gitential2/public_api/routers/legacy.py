import asyncio

from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import RedirectResponse
from gitential2.core import (
    GitentialContext,
    handle_authorize,
)

from ..dependencies import gitential_context, OAuth, current_user
from .auth import _get_token, _get_user_info


async def handle_failed_authorize(integration, token, user_info):

    return {"integration": integration, "token": token, "user_info": user_info, "failed": True}


router = APIRouter()


# pylint: disable=too-many-arguments
@router.get("/login", name="legacy_login")
async def legacy_login(
    source: str,
    request: Request,
    id_token: str = None,
    code: str = None,
    oauth_verifier: str = None,
    g: GitentialContext = Depends(gitential_context),
    oauth: OAuth = Depends(),
    current_user=Depends(current_user),
):
    remote = oauth.create_client(source)
    integration = g.integrations.get(source)

    if remote is None or integration is None:
        raise HTTPException(404)

    token = await _get_token(request, remote, integration, code, id_token, oauth_verifier)
    user_info = await _get_user_info(request, remote, token)

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, handle_authorize, g, integration.name, token, user_info, current_user)

    request.session["current_user_id"] = result["user"].id

    redirect_uri = request.session.get("redirect_uri")
    if redirect_uri:
        return RedirectResponse(url=redirect_uri)
    else:
        return result
