import pytest
from fastapi.testclient import TestClient
from gitential2.public_api.application import create_app


@pytest.fixture
def client(settings):
    return TestClient(create_app(settings=settings))


def test_read_ping(client):
    response = client.get("/v2/ping")
    assert response.status_code == 200
    assert response.json() == {"success": True, "msg": "pong"}
