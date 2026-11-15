from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.serving.health import health_status


@pytest.mark.parametrize(
    "stale,age,mismatches,error,expected",
    [
        (False, 1, 0, None, "ok"),
        (True, 1, 0, None, "degraded"),
        (False, 1801, 0, None, "degraded"),
        (False, 1, 1, None, "degraded"),
        (False, 1, 0, "RPC error", "degraded"),
    ],
)
def test_health_degraded_rules(stale, age, mismatches, error, expected):
    now = datetime.now(timezone.utc)
    run = SimpleNamespace(
        finished_at=now - timedelta(seconds=age),
        mismatch_count=mismatches,
        in_flight_count=7,
        error=error,
    )
    assert health_status(stale, run, now) == expected


def test_missing_run_is_degraded():
    assert health_status(False, None, datetime.now(timezone.utc)) == "degraded"


@pytest.mark.asyncio
async def test_database_failure_is_down(monkeypatch):
    from app.serving import health

    def unavailable():
        raise ConnectionError("database unavailable")

    monkeypatch.setattr(health, "async_session_factory", unavailable)
    monkeypatch.setattr(health, "rpc_snapshot", lambda: {})
    result = await health.get_health()
    assert result["status"] == "down"
    assert "ConnectionError" in result["error"]
