"""A request's accounting payload must use its own database snapshot."""

import time
from types import SimpleNamespace

import pytest

from app.valuation.engine import ValuationEngine


@pytest.mark.asyncio
async def test_accounting_snapshot_bypasses_cross_transaction_cache(monkeypatch):
    engine = ValuationEngine()
    stale = object()
    current = object()
    engine._live_cache["wallet"] = (time.monotonic(), stale)
    session = SimpleNamespace(info={"accounting_snapshot": True})

    async def read_snapshot(actual_session, wallet):
        assert actual_session is session
        assert wallet == "wallet"
        return current

    monkeypatch.setattr(engine, "_value_live_wallet", read_snapshot)
    assert await engine.value_wallet(session, "WALLET") is current
