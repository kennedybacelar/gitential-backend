from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from gitential2.settings import GitentialSettings, load_settings

from .routers import ping, configuration, workspaces, projects, teams, repositories, stats


def create_app(settings: Optional[GitentialSettings] = None):
    app = FastAPI()
    app.state.settings = settings or load_settings()
    _configure_cors(app)
    _configure_routes(app)

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
