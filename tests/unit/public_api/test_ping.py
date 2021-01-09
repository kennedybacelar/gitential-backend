import pytest
from fastapi.testclient import TestClient
from gitential2.public_api.application import create_app


@pytest.fixture
def client():
    return TestClient(create_app())


def test_read_ping(client):
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"success": True, "msg": "pong"}
