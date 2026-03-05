"""Tests for manager API endpoints (using FastAPI test client)."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from crane_manager.main import app


def _mock_redis():
    rc = MagicMock()
    rc.ping.return_value = True
    rc.get_index.return_value = set()
    rc.client.hgetall.return_value = {}
    rc.client.xinfo_stream.side_effect = Exception("no stream")
    return rc


@patch("crane_manager.deps.get_redis")
def test_root(mock_get_redis):
    mock_get_redis.return_value = _mock_redis()
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "crane-manager"


@patch("crane_manager.api.strategies.get_redis")
def test_list_strategies_empty(mock_get_redis):
    mock_get_redis.return_value = _mock_redis()
    client = TestClient(app)
    resp = client.get("/api/strategies/")
    assert resp.status_code == 200
    assert resp.json() == []


@patch("crane_manager.api.health.get_redis")
def test_health_check(mock_get_redis):
    mock_get_redis.return_value = _mock_redis()
    client = TestClient(app)
    resp = client.get("/api/health/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["redis"] is True
