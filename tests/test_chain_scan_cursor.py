import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import delete

from app.chain import tail
from app.ingestion.writer import write_event_batch
from app.storage.database import async_session_factory
from app.storage.models import ChainScanCursor, IngestionCursor, SourceEvent, WalletTotal


@pytest.mark.asyncio
async def test_tail_scan_persists_and_resumes_both_directions(tmp_path, monkeypatch):
    wallet = "0x" + uuid4().hex + "0" * 8
    checkpoint = tmp_path / "chain_scan_checkpoint.json"
    checkpoint.write_text(json.dumps({f"{wallet}:from": 100, f"{wallet}:to": 100}))
    monkeypatch.setattr(tail, "CHECKPOINT", checkpoint)
    starts = []

    async def get_logs(self, http, params):
        starts.append(int(params["fromBlock"], 16))
        return []

    async def prefetch(self, http, logs):
        pass

    monkeypatch.setattr(tail.Scanner, "get_logs", get_logs)
    monkeypatch.setattr(tail.Scanner, "prefetch_timestamps", prefetch)
    await tail.scan_tail(wallet, 102)
    await tail.scan_tail(wallet, 103)
    assert starts == [101, 101, 103, 103]
    async with async_session_factory() as session:
        for direction in ("from", "to"):
            cursor = await session.get(ChainScanCursor, (wallet, direction))
            assert cursor.last_block == 103
        await session.execute(delete(ChainScanCursor).where(ChainScanCursor.wallet == wallet))
        await session.commit()


@pytest.mark.asyncio
async def test_new_wallet_scan_starts_from_earliest_event(tmp_path, monkeypatch):
    wallet = "0x" + uuid4().hex + "0" * 8
    monkeypatch.setattr(tail, "CHECKPOINT", tmp_path / "missing.json")
    first = AsyncMock(return_value=100)
    monkeypatch.setattr(tail, "first_block", first)
    starts = []

    async def get_logs(self, http, params):
        starts.append(int(params["fromBlock"], 16))
        return []

    async def prefetch(self, http, logs):
        pass

    monkeypatch.setattr(tail.Scanner, "get_logs", get_logs)
    monkeypatch.setattr(tail.Scanner, "prefetch_timestamps", prefetch)
    async with async_session_factory() as session:
        async with session.begin():
            await write_event_batch(
                session, wallet, "fixture", [({"type": "REWARD", "timestamp": 1700000000}, wallet)]
            )
    try:
        await tail.scan_tail(wallet, 102)
        assert starts == [100, 100]
        first.assert_awaited_once()
    finally:
        async with async_session_factory() as session:
            for model in (ChainScanCursor, IngestionCursor, SourceEvent, WalletTotal):
                await session.execute(delete(model).where(model.wallet == wallet))
            await session.commit()
