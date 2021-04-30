import asyncio
from typing import Optional
from structlog import get_logger
from fastapi import APIRouter, Request, HTTPException, Depends, Header
from fastapi.responses import RedirectResponse
from gitential2.core import (
    GitentialContext,
    handle_authorize,
    register_user,
    get_user,
    get_current_subscription,
    get_profile_picture,
)
from gitential2.datatypes.users import UserCreate
from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.exceptions import AuthenticationException
from ..dependencies import gitential_context, OAuth, current_user, verify_recaptcha_token, api_access_log

logger = get_logger(__name__)


router = APIRouter()


async def _get_token(request, remote, code, id_token, oauth_verifier):
    if code:
        token = await remote.authorize_access_token(request)
        print("van code", token)
        if id_token:
            token["id_token"] = id_token
    elif id_token:
        token = {"id_token": id_token}
    elif oauth_verifier:
        # OAuth 1
        token = await remote.authorize_access_token(request)
    else:
        # handle failed
        raise AuthenticationException("failed to get token")
    return token


async def _get_user_info(request, remote, token):
    if "id_token" in token:
        user_info = await remote.parse_id_token(request, token)
    else:
        remote.token = token
        try:
            if "token_type" in token and token["token_type"] == "jwt-bearer":  # VSTS fix
                token["token_type"] = "bearer"
            user_info = await remote.userinfo(token=token)
        except Exception as e:
            logger.exception("error getting user_info", remote=remote, token=token)
            # print(remote, token)
            raise e
    return user_info


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
    integration = g.integrations.get(backend)

    if remote is None or integration is None:
        raise HTTPException(404)

    try:

        token = await _get_token(request, remote, code, id_token, oauth_verifier)
        user_info = await _get_user_info(request, remote, token)

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, handle_authorize, g, integration.name, token, user_info, current_user)

        request.session["current_user_id"] = result["user"].id

        redirect_uri = request.session.get("redirect_uri")
        if redirect_uri:
            return RedirectResponse(url=redirect_uri)
        else:
            return result
    except Exception as e:  # pylint: disable=broad-except
        logger.exception("Error during authentication")
        raise AuthenticationException("error durin authentication") from e


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

    if remote is None:
        raise HTTPException(404)
    if request.app.state.settings.web.legacy_login:
        if backend == "vsts":
            redirect_uri = request.url_for("legacy_login")
        else:
            redirect_uri = request.url_for("legacy_login") + f"?source={backend}"
    else:
        redirect_uri = request.url_for("auth", backend=backend)

    return await remote.authorize_redirect(request, redirect_uri)


@router.get("/logout")
async def logout(request: Request):
    if "current_user_id" in request.session:
        del request.session["current_user_id"]

    return {}


@router.get("/session")
def session(
    request: Request, g: GitentialContext = Depends(gitential_context), api_access_log=Depends(api_access_log)
):  # pylint: disable=unused-argument
    current_user_id = request.session.get("current_user_id")
    if current_user_id:
        user_in_db = get_user(g, current_user_id)
        if user_in_db:
            # registration_ready = license_.is_on_premises or request.session.get("registration_ready", False)
            subscription = get_current_subscription(g, user_in_db.id)
            return {
                "user_id": user_in_db.id,
                "login": user_in_db.login,
                "marketing_consent_accepted": user_in_db.marketing_consent_accepted,
                "subscription_details": subscription,
                "subscription": SubscriptionType.professional
                if subscription.subscription_type in [SubscriptionType.trial, SubscriptionType.professional]
                else subscription.subscription_type,
                "registration_ready": user_in_db.registration_ready,
                "login_ready": user_in_db.login_ready,
                "profile_picture": get_profile_picture(g, user_in_db),
            }
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
