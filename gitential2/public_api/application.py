from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

from authlib.integrations.starlette_client import OAuth

from gitential2.settings import GitentialSettings, load_settings

from gitential2.core import Gitential

from .routers import ping, configuration, workspaces, projects, teams, repositories, stats, auth


def create_app(settings: Optional[GitentialSettings] = None):
    app = FastAPI(title="Gitential REST API", version="2.0.0")
    settings = settings or load_settings()
    app.state.settings = settings
    _configure_cors(app)
    _configure_routes(app)
    _configure_session(app, settings)
    _configure_gitential_core(app, settings)
    _configure_oauth_authentication(app)
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
