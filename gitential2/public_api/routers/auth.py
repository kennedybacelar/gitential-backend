import asyncio
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Depends, Header
from fastapi.responses import RedirectResponse
from fastapi.encoders import jsonable_encoder

from ..dependencies import GitentialCore, OAuth


async def handle_authorize(integration, token, user_info):
    if integration.name == "gitlab-internal":
        repositories = integration.list_available_private_repositories(token=token, update_token=print)
        print("!!!", repositories)
        new_token = integration.refresh_token(token)
        normalized_user_info = integration.normalize_userinfo(user_info)

    return {
        "token": token,
        "user_info": user_info,
        "new_token": new_token,
        "normalized_user_info": normalized_user_info,
    }


router = APIRouter()

# pylint: disable=too-many-arguments
@router.get("/auth/{backend}")
async def auth(
    backend: str,
    request: Request,
    id_token: str = None,
    code: str = None,
    oauth_verifier: str = None,
    gitential: GitentialCore = Depends(),
    oauth: OAuth = Depends(),
):
    remote = oauth.create_client(backend)
    integration = gitential.integrations.get(backend)
    loop = asyncio.get_running_loop()

    if remote is None:
        raise HTTPException(404)

    if code:
        token = await remote.authorize_access_token(request)
        if id_token:
            token["id_token"] = id_token
    elif id_token:
        token = {"id_token": id_token}
    elif oauth_verifier:
        # OAuth 1
        token = await remote.authorize_access_token(request)
    else:
        # handle failed
        return await handle_authorize(integration, None, None)

    if "id_token" in token:
        user_info = await remote.parse_id_token(request, token)
    else:
        remote.token = token
        user_info = await remote.userinfo(token=token)
    result = await loop.run_in_executor(None, gitential.handle_authorize, integration.name, token, user_info)

    # If no previous current_user found set this up
    if not request.session.get("current_user") and result.get("user"):
        request.session["current_user"] = jsonable_encoder(result["user"])

    redirect_uri = request.session.get("redirect_uri")
    if redirect_uri:
        return RedirectResponse(url=redirect_uri)
    else:
        return result


@router.get("/login/{backend}")
async def login(backend: str, request: Request, oauth: OAuth = Depends(), referer: Optional[str] = Header(None)):
    remote = oauth.create_client(backend)
    request.session["redirect_uri"] = referer
    if remote is None:
        raise HTTPException(404)

    redirect_uri = request.url_for("auth", backend=backend)

    return await remote.authorize_redirect(request, redirect_uri)
