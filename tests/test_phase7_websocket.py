import asyncio

import pytest

from app.serving.websocket import ConnectionManager


@pytest.mark.asyncio
async def test_older_initial_snapshot_cannot_replace_newer_publication(monkeypatch):
    manager = ConnectionManager()
    entered, release = asyncio.Event(), asyncio.Event()

    class Socket:
        def __init__(self):
            self.messages = []

        async def accept(self):
            pass

        async def send_json(self, message):
            self.messages.append(message)

    ws = Socket()
    await manager.connect("wallet", ws)

    def freshness(revision):
        return {"accounting_revision": revision, "data_as_of": None, "ingestion_stale": False}

    async def old_state(wallet):
        entered.set()
        await release.wait()
        return {"accounting_revision": 1}, freshness(1)

    monkeypatch.setattr(manager, "_state", old_state)
    initial = asyncio.create_task(manager.snapshot("wallet", ws))
    await entered.wait()
    await manager._send(ws, "wallet", "pnl_update", {"accounting_revision": 2}, freshness(2))
    release.set()
    await initial
    assert [m["payload"]["accounting_revision"] for m in ws.messages] == [2]
    assert [m["seq"] for m in ws.messages] == [1]
    await manager.disconnect("wallet", ws)


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_refresh_failure_recovers_or_explicitly_closes(monkeypatch, permanent):
    monkeypatch.setattr("app.serving.websocket.FLUSH_DELAY_SECONDS", 0.001)
    manager = ConnectionManager()

    class Socket:
        messages = None
        closed = None

        async def accept(self):
            self.messages = []

        async def send_json(self, message):
            self.messages.append(message)

        async def close(self, code):
            self.closed = code

    ws = Socket()
    await manager.connect("wallet", ws)
    calls = 0

    async def state(wallet):
        nonlocal calls
        calls += 1
        if permanent or calls == 1:
            raise RuntimeError("temporary SQL failure")
        return {"accounting_revision": 2}, {
            "accounting_revision": 2,
            "data_as_of": None,
            "ingestion_stale": False,
        }

    monkeypatch.setattr(manager, "_state", state)
    await manager.broadcast_to_wallet("wallet", "pnl_update", {"ref": "test"})
    await asyncio.wait_for(manager._tasks["wallet"], 1)
    assert not manager._pending and not manager._tasks
    if permanent:
        assert calls == 3 and ws.closed == 1013
        assert not ws.messages and not manager.active_connections
    else:
        assert calls == 2 and ws.closed is None
        assert ws.messages[0]["payload"]["accounting_revision"] == 2
    await manager.stop()


@pytest.mark.asyncio
async def test_notification_arriving_during_read_is_not_discarded(monkeypatch):
    monkeypatch.setattr("app.serving.websocket.FLUSH_DELAY_SECONDS", 0.001)
    manager = ConnectionManager()
    entered, release = asyncio.Event(), asyncio.Event()
    messages = []

    class Socket:
        async def accept(self):
            pass

        async def send_json(self, message):
            messages.append(message)

    await manager.connect("wallet", Socket())
    calls = 0

    async def state(wallet):
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await release.wait()
        return {}, {"accounting_revision": calls, "data_as_of": None, "ingestion_stale": False}

    monkeypatch.setattr(manager, "_state", state)
    await manager.broadcast_to_wallet("wallet", "pnl_update", {"ref": "first"})
    await asyncio.wait_for(entered.wait(), 1)
    await manager.broadcast_to_wallet("wallet", "pnl_update", {"ref": "second"})
    release.set()
    await asyncio.wait_for(manager._tasks["wallet"], 1)
    assert [m["payload"]["ref"] for m in messages] == ["first", "second"]
    assert not manager._pending and not manager._tasks


@pytest.mark.asyncio
async def test_stop_during_refresh_cannot_respawn_retry(monkeypatch):
    monkeypatch.setattr("app.serving.websocket.FLUSH_DELAY_SECONDS", 0.001)
    manager = ConnectionManager()
    entered = asyncio.Event()
    manager.active_connections["wallet"] = {object()}

    async def state(wallet):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(manager, "_state", state)
    await manager.broadcast_to_wallet("wallet", "pnl_update", {})
    await asyncio.wait_for(entered.wait(), 1)
    task = manager._tasks["wallet"]
    await manager.stop()
    assert task.cancelled()
    assert not manager._tasks and not manager._pending
