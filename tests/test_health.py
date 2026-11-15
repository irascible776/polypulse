import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.serving.routes import get_db_session


@pytest.mark.asyncio
async def test_health_check_returns_200():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] in {"ok", "degraded"}


@pytest.mark.asyncio
async def test_serving_health_with_empty_chain_events(monkeypatch):
    class EmptyResult:
        def first(self):
            return None

    class EmptySession:
        async def execute(self, stmt):
            return EmptyResult()

    async def session_override():
        yield EmptySession()

    async def database_connected():
        return True

    monkeypatch.setattr("app.serving.routes.check_database_connection", database_connected)
    app.dependency_overrides[get_db_session] = session_override
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for path in ("/health", "/serving/health", "/system/health"):
                response = await client.get(path)
                assert response.status_code == 200
                if path != "/health":
                    assert response.json()["last_chain_pull"] == {
                        "block_number": None,
                        "pulled_at": None,
                    }
    finally:
        app.dependency_overrides.pop(get_db_session, None)


@pytest.mark.asyncio
async def test_ready_check_success_when_db_up(monkeypatch):
    async def mock_check_db_success():
        return True

    monkeypatch.setattr("app.main.check_database_connection", mock_check_db_success)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready", "database": "connected"}


@pytest.mark.asyncio
async def test_ready_check_fails_when_db_down(monkeypatch):
    async def mock_check_db_failure():
        return False

    monkeypatch.setattr("app.main.check_database_connection", mock_check_db_failure)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/ready")
        assert response.status_code == 503
        assert response.json() == {"status": "not ready", "database": "disconnected"}
