import asyncio
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, Depends, Header
from fastapi.responses import RedirectResponse
from gitential2.core import GitentialContext, handle_authorize, register_user, get_user, get_current_subscription
from gitential2.datatypes.users import UserCreate

from ..dependencies import gitential_context, OAuth, current_user, verify_recaptcha_token


async def handle_failed_authorize(integration, token, user_info):
    # if integration.name == "gitlab-internal":
    #     repositories = integration.list_available_private_repositories(token=token, update_token=print)
    #     print("!!!", repositories)
    #     new_token = integration.refresh_token(token)
    #     normalized_user_info = integration.normalize_userinfo(user_info)

    return {"integration": integration, "token": token, "user_info": user_info, "failed": True}


router = APIRouter()

# pylint: disable=too-many-arguments
@router.get("/auth/{backend}")
async def auth(
    backend: str,
    request: Request,
    id_token: str = None,
    code: str = None,
    oauth_verifier: str = None,
    g: GitentialContext = Depends(gitential_context),
    oauth: OAuth = Depends(),
    current_user=Depends(current_user),
):
    remote = oauth.create_client(backend)
    # print("!!!!!!!", remote.api_base_url)
    integration = g.integrations.get(backend)
    if not integration:
        raise HTTPException(404)

    loop = asyncio.get_running_loop()

    if remote is None:
        raise HTTPException(404)

    if code:
        token = await remote.authorize_access_token(request)
        # print("van code", token)
        if id_token:
            token["id_token"] = id_token
    elif id_token:
        token = {"id_token": id_token}
    elif oauth_verifier:
        # OAuth 1
        token = await remote.authorize_access_token(request)
    else:
        # handle failed
        return await handle_failed_authorize(integration, None, None)

    if "id_token" in token:
        # print("van id_token")
        user_info = await remote.parse_id_token(request, token)
    else:
        remote.token = token
        try:
            user_info = await remote.userinfo(token=token)
        except Exception as e:
            print(remote, token)
            raise e

    result = await loop.run_in_executor(None, handle_authorize, g, integration.name, token, user_info, current_user)

    request.session["current_user_id"] = result["user"].id

    redirect_uri = request.session.get("redirect_uri")
    if redirect_uri:
        return RedirectResponse(url=redirect_uri)
    else:
        return result


@router.get("/login/{backend}")
async def login(
    backend: str,
    request: Request,
    oauth: OAuth = Depends(),
    referer: Optional[str] = Header(None),
    redirect_after: Optional[str] = None,
):
    remote = oauth.create_client(backend)
    request.session["redirect_uri"] = redirect_after or referer
    # print(request.headers.items())
    if remote is None:
        raise HTTPException(404)

    redirect_uri = request.url_for("auth", backend=backend)

    return await remote.authorize_redirect(request, redirect_uri)


@router.get("/logout")
async def logout(request: Request):
    if "current_user_id" in request.session:
        del request.session["current_user_id"]

    return {}


@router.get("/session")
def session(
    request: Request,
    g: GitentialContext = Depends(gitential_context),
):
    current_user_id = request.session.get("current_user_id")
    if current_user_id:
        user_in_db = get_user(g, current_user_id)
        if user_in_db:
            # registration_ready = license_.is_on_premises or request.session.get("registration_ready", False)
            subscription = get_current_subscription(g, user_in_db.id)
            return {
                "user_id": user_in_db.id,
                "login": user_in_db.login,
                "marketing_consent_accepted": True,
                "subscription": "professional",
                "registration_ready": user_in_db.registration_ready,
                "login_ready": user_in_db.login_ready,
            }
    else:
        return {}


@router.post("/registration")
def registration(
    registration_data: UserCreate,
    request: Request,
    g: GitentialContext = Depends(gitential_context),
    verify_recaptcha_token=Depends(verify_recaptcha_token),  # pylint: disable=unused-argument
):

    current_user_id = request.session.get("current_user_id")
    current_user = g.backend.users.get(current_user_id) if current_user_id else None

    user, _ = register_user(g, registration_data, current_user=current_user)
    request.session["current_user_id"] = user.id
    return {}