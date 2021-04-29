from typing import cast
import requests
from structlog import get_logger
from fastapi import Request, Depends, Header
from fastapi.exceptions import HTTPException
from gitential2.core.context import GitentialContext
from gitential2.core.users import get_user
from gitential2.core.access_log import create_access_log

logger = get_logger(__name__)


def gitential_context(request: Request) -> GitentialContext:
    return cast(GitentialContext, request.app.state.gitential)


class OAuth:
    def __init__(self, request: Request):
        self.oauth = request.app.state.oauth

    def __getattr__(self, name):
        return getattr(self.oauth, name)


def current_user(request: Request, g: GitentialContext = Depends(gitential_context)):
    if "current_user_id" in request.session:
        return get_user(g, request.session["current_user_id"])
    return None


def api_access_log(
    request: Request, current_user=Depends(current_user), g: GitentialContext = Depends(gitential_context)
):
    if current_user:
        return create_access_log(
            g,
            user_id=current_user.id,
            path=request.url.path,
            method=request.method,
            ip_address=request.client.host,
        )

    else:
        logger.info(
            "No access log, unknown user",
            path=request.url.path,
            method=request.method,
            ip_address=request.client.host,
        )
        return None


def verify_recaptcha_token(x_recaptcha_token: str = Header(...), g: GitentialContext = Depends(gitential_context)):
    recaptcha_settings = g.settings.recaptcha

    if recaptcha_settings.secret_key:
        recaptcha_url = "https://www.google.com/recaptcha/api/siteverify"
        payload = {
            "secret": recaptcha_settings.secret_key,
            "response": x_recaptcha_token,
        }
        response = requests.post(recaptcha_url, data=payload)
        result = response.json()

        if result.get("success") and result.get("score", 0.0) > 0.5:
            return x_recaptcha_token
        else:
            raise HTTPException(status_code=400, detail="reCaptcha validation failed")
    else:
        logger.debug("reCaptcha not configured, skipping token check.")
        return ""
