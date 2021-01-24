from typing import Optional
from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

from authlib.integrations.starlette_client import OAuth, OAuthError

# from loginpass.gitlab import create_gitlab_backend

from gitential2.settings import GitentialSettings, load_settings
from gitential2.integrations import init_integrations
from .routers import ping, configuration, workspaces, projects, teams, repositories, stats, auth


def create_app(settings: Optional[GitentialSettings] = None):
    app = FastAPI()
    settings = settings or load_settings()
    app.state.settings = settings
    _configure_cors(app)
    _configure_routes(app)
    _configure_session(app, settings)
    _configure_integrations(app, settings)
    _configure_oauth_authentication(app, settings)
    return app


def _configure_cors(app: FastAPI):
    origins = ["http://localhost", "http://localhost:8080", "https://5e4e00989272.ngrok.io", "http://localhost:8000"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def _configure_routes(app: FastAPI):
    app.include_router(ping.router, prefix="/v2")
    app.include_router(configuration.router, prefix="/v2")
    app.include_router(workspaces.router, prefix="/v2")
    app.include_router(projects.router, prefix="/v2")
    app.include_router(teams.router, prefix="/v2")
    app.include_router(repositories.router, prefix="/v2")
    app.include_router(stats.router, prefix="/v2")
    app.include_router(auth.router, prefix="/v2")


def _configure_session(app: FastAPI, settings: GitentialSettings):
    app.add_middleware(SessionMiddleware, secret_key=settings.secret)


def _configure_integrations(app: FastAPI, settings: GitentialSettings):
    app.state.integrations = init_integrations(settings)


def _configure_oauth_authentication(app: FastAPI, settings: GitentialSettings):
    oauth = OAuth()
    print("fooooo")
    for integration in app.state.integrations.values():
        print(integration.name)
        if integration.is_oauth:
            oauth.register(name=integration.name, **integration.oauth_register())
            print("registering", integration.name)
    app.state.oauth = oauth


# def _configure_oauth_authentication(app: FastAPI, settings: GitentialSettings):
#     oauth = OAuth()
#     app.state.oauth = oauth

#     gitlab = create_gitlab_backend("gitlab", "gitlab.ops.gitential.com")

#     router = _create_authlib_routes([gitlab], oauth, handle_authorize=handle_authorize)

#     app.include_router(router, prefix="/v2")


# def handle_authorize(remote, token, user_info, request):
#     return user_info


# def _create_authlib_routes(backends, oauth, handle_authorize):
#     router = APIRouter()

#     for b in backends:
#         register_to(oauth, b)

#     @router.get("/auth/{backend}")
#     async def auth(
#         backend: str,
#         id_token: str = None,
#         code: str = None,
#         oauth_verifier: str = None,
#         request: Request = None,
#     ):
#         remote = oauth.create_client(backend)
#         if remote is None:
#             raise HTTPException(404)

#         if code:
#             token = await remote.authorize_access_token(request)
#             if id_token:
#                 token["id_token"] = id_token
#         elif id_token:
#             token = {"id_token": id_token}
#         elif oauth_verifier:
#             # OAuth 1
#             token = await remote.authorize_access_token(request)
#         else:
#             # handle failed
#             return await handle_authorize(remote, None, None)
#         if "id_token" in token:
#             user_info = await remote.parse_id_token(request, token)
#         else:
#             remote.token = token
#             user_info = await remote.userinfo(token=token)
#         return await handle_authorize(remote, token, user_info, request)

#     @router.get("/login/{backend}")
#     async def login(backend: str, request: Request):
#         remote = oauth.create_client(backend)
#         if remote is None:
#             raise HTTPException(404)

#         redirect_uri = request.url_for("auth", backend=backend)
#         conf_key = "{}_AUTHORIZE_PARAMS".format(backend.upper())
#         params = oauth.config.get(conf_key, default={}) if oauth.config else {}
#         return await remote.authorize_redirect(request, redirect_uri, **params)

#     return router


# def register_to(oauth, backend_cls):
#     from authlib.integrations.starlette_client import StarletteRemoteApp

#     class RemoteApp(backend_cls, StarletteRemoteApp):
#         OAUTH_APP_CONFIG = backend_cls.OAUTH_CONFIG

#     oauth.register(RemoteApp.NAME, overwrite=True, client_cls=RemoteApp)