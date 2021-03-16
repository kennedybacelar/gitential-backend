from fastapi import APIRouter
from fastapi import Request
from gitential2.integrations import IntegrationType
from gitential2.license import check_license

router = APIRouter()


@router.get("/configuration")
def configuration(request: Request):
    license_ = check_license()

    logins = {}
    sources = []
    frontend_settings = request.app.state.settings.frontend
    recaptcha_settings = request.app.state.settings.recaptcha
    integrations_settings = request.app.state.settings.integrations
    for name, settings in integrations_settings.items():
        if settings.login:
            logins[name] = {
                "login_text": settings.login_text or f"Login with {name}",
                "signup_text": settings.signup_text or f"Sign up with {name}",
                "login_top_text": settings.login_top_text or None,
                "type": settings.type_,
                "url": request.url_for("login", backend=name),
            }
        if settings.type_ not in [IntegrationType.linkedin, IntegrationType.dummy]:
            sources.append(
                {
                    "name": name,
                    "type": settings.type_,
                    "url": request.url_for("login", backend=name),
                }
            )
    return {
        "license": license_.as_config(),
        "frontend": frontend_settings,
        "logins": logins,
        "recaptcha": {"site_key": recaptcha_settings.site_key},
        "sources": sources,
        "contact": "info@gitential.com",
        "sentry": {"dsn": "https://dc5be4ac529146d68d723b5f5be5ae2d@sentry.io/1815669"},
        "debug": "False",
    }
