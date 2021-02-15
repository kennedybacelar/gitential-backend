from fastapi import APIRouter
from fastapi import Request
from gitential2.integrations import IntegrationType

# from gitential2.integrations import construct_login_configuration

router = APIRouter()


@router.get("/session")
async def session(request: Request):
    current_user = request.session.get("current_user")
    print("current_user:", current_user)
    if current_user:
        return {
            "user_id": current_user["id"],
            "login": current_user["login"],
            # "github_id": 15088218,
            # "bitbucket_id": None,
            # "vsts_id": None,
            "private": True,
            "tc_consent_accepted_at": "2020-03-20",
            "marketing_consent_accepted": True,
            "sources_connected": ["gitlab-internal"],
        }
    else:
        return {}

    # print(request.session)
    # for k, v in request.session.items():
    #     print(k, v)

    # if request.session.get("current_user"):
    #     return {
    #         "user_id": 2090,
    #         "login": "gitential-user",
    #         "github_id": 15088218,
    #         "bitbucket_id": None,
    #         "vsts_id": None,
    #         "private": True,
    #         "tc_consent_accepted_at": "2020-03-20",
    #         "marketing_consent_accepted": True,
    #     }
    # return {}


@router.get("/license-check")
async def license_check():
    return {
        "valid_until": 1640908800,
        "customer_name": "G-COMDEV",
        "installation_type": "cloud",
        "number_of_developers": 500,
    }


@router.get("/users/is-admin")
async def is_admin():
    return {"is_admin": False}


gitlab_application_id = "d043df035355fe747d2a62d35b0f6089cff4c8c45fb8ca0066459792e388230d"
gitlab_application_secret = "9bcb53d633178a83b3e46849bcec95d3d2c15d817bc48c1a1e35a84e8a4964c7"


# @router.get("/login/{source_name}")
# def login(source_name: str, code: str, state: Optional[str] = None):
#     return {"ok": True, "source_name": source_name}


@router.get("/configuration")
def configuration(request: Request):
    # logins = construct_login_configuration(request.app.state.settings, frontend_url="http://example.com")
    logins = {}
    sources = []
    integrations_settings = request.app.state.settings.integrations
    print(integrations_settings)
    for name, settings in integrations_settings.items():
        if settings.login:
            logins[name] = {
                "login_text": settings.login_text or f"Login with {name}",
                "signup_text": settings.signup_text or f"Sign up with {name}",
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
        "license": {
            "valid_until": 1640908800,
            "customer_name": "G-COMDEV",
            "installation_type": "on-prem",  # "on-prem",
            "number_of_developers": 500,
        },
        "logins": logins,
        "sources": sources,
        "contact": "info@gitential.com",
        "sentry": {"dsn": "https://dc5be4ac529146d68d723b5f5be5ae2d@sentry.io/1815669"},
        "debug": "False",
    }


# """
# {
#     "env": "prod",
#     "url": "https://gitential.com",
#     "backend": "https://api.gitential.com",
#     "socketUrl": "wss://api.gitential.com",
#     "debug": false,
#     "repo_size_limit": 1048576,
#     "stripe_key": "pk_live_G3a4sySJamlnyWYMgxwbTyI900QkPl8Poe",

#     "demo_url": "https://app.gitential.com/demo/accounts/2155",

#     "legal": {
#         "terms_date": "2020-03-20",
#         "cookies_date": "2020-03-20",
#         "eula_date": "2020-03-20",
#         "privacy_date": "2020-04-10"
#     }
# }
# """
