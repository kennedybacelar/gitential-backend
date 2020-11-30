from fastapi import FastAPI
from .routers import ping


def create_app():
    app = FastAPI()
    app.include_router(ping.router)
    return app
