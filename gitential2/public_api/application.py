from typing import Optional
from structlog import get_logger
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

from authlib.integrations.starlette_client import OAuth

from gitential2.settings import GitentialSettings, load_settings

from gitential2.core import Gitential

from .routers import ping, configuration, workspaces, projects, teams, repositories, stats, auth

logger = get_logger(__name__)


def create_app(settings: Optional[GitentialSettings] = None):
    app = FastAPI(title="Gitential REST API", version="2.0.0")
    settings = settings or load_settings()
    app.state.settings = settings
    _configure_cors(app)
    _configure_routes(app)
    _configure_session(app, settings)
    _configure_gitential_core(app, settings)
    _configure_oauth_authentication(app)
    _configure_error_handling(app)
    return app


def _configure_cors(app: FastAPI):
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=".*",
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
    app.add_middleware(SessionMiddleware, secret_key=settings.secret, same_site="None")


def _configure_gitential_core(app: FastAPI, settings: GitentialSettings):
    app.state.gitential = Gitential.from_config(settings)


def _configure_oauth_authentication(app: FastAPI):
    oauth = OAuth()
    for integration in app.state.gitential.integrations.values():
        if integration.is_oauth:
            oauth.register(name=integration.name, **integration.oauth_register())
            print("registering", integration.name)
    app.state.oauth = oauth


def _configure_error_handling(app: FastAPI):
    @app.exception_handler(500)
    async def custom_http_exception_handler(request, exc):
        logger.error("Internal server error", exc=exc)
        response = JSONResponse(content={"error": "Something went wrong"}, status_code=500)

        # Since the CORSMiddleware is not executed when an unhandled server exception
        # occurs, we need to manually set the CORS headers ourselves if we want the FE
        # to receive a proper JSON 500, opposed to a CORS error.
        # Setting CORS headers on server errors is a bit of a philosophical topic of
        # discussion in many frameworks, and it is currently not handled in FastAPI.
        # See dotnet core for a recent discussion, where ultimately it was
        # decided to return CORS headers on server failures:
        # https://github.com/dotnet/aspnetcore/issues/2378
        origin = request.headers.get("origin")

        if origin:
            # Have the middleware do the heavy lifting for us to parse
            # all the config, then update our response headers
            cors = CORSMiddleware(
                app=app, allow_origin_regex=".*", allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
            )

            # Logic directly from Starlette's CORSMiddleware:
            # https://github.com/encode/starlette/blob/master/starlette/middleware/cors.py#L152

            response.headers.update(cors.simple_headers)
            has_cookie = "cookie" in request.headers

            # If request includes any cookie headers, then we must respond
            # with the specific origin instead of '*'.
            if cors.allow_all_origins and has_cookie:
                response.headers["Access-Control-Allow-Origin"] = origin

            # If we only allow specific origins, then we have to mirror back
            # the Origin header in the response.
            elif not cors.allow_all_origins and cors.is_allowed_origin(origin=origin):
                response.headers["Access-Control-Allow-Origin"] = origin
                response.headers.add_vary_header("Origin")

        return response

    return custom_http_exception_handler
