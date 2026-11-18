import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from app.accounting.engine import compute_condition_events
from app.accounting.projector import AccountingProjector
from app.config import get_settings
from app.ingestion.writer import write_event_batch
from app.storage.database import wallet_read_session, wallet_session_factory
from app.storage.models import AccountingCheckpoint, PositionLedger, WalletTotal
from app.storage.sharding import wallet_bucket
from app.worker import Worker
from tests.test_phase7_replay import economic_state, raw


@pytest.mark.asyncio
async def test_dead_database_session_cannot_publish_delayed_condition_results():
    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    entered, release = asyncio.Event(), asyncio.Event()
    backend = []

    class DelayedExecutor:
        async def compute(self, owner, work, now, timeout):
            entered.set()
            await release.wait()
            return [
                ("attempt", owner, c, compute_condition_events(owner, c, e, p, now), 1, 1, 2)
                for c, e, p in work
            ]

    async with factory() as session, session.begin():
        await write_event_batch(
            session, wallet, wallet, [(raw(c, 100), wallet + c) for c in ("a", "b")]
        )

    async def old_owner():
        async with factory() as session, session.begin():
            backend.append(await session.scalar(text("SELECT pg_backend_pid()")))
            await AccountingProjector(executor=DelayedExecutor()).catch_up_wallet(session, wallet)

    old = asyncio.create_task(old_owner())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with factory() as session:
            assert await session.scalar(
                text("SELECT pg_terminate_backend(:pid)"), {"pid": backend[0]}
            )
        async with factory() as session, session.begin():
            assert (
                await AccountingProjector().catch_up_wallet(session, wallet)
            ).events_processed == 2
        async with factory() as session:
            expected = await economic_state(session, wallet)
            revision = (await session.get(WalletTotal, wallet)).publication_revision
        release.set()
        with pytest.raises(DBAPIError):
            await old
        async with factory() as session:
            assert await economic_state(session, wallet) == expected
            assert (await session.get(WalletTotal, wallet)).publication_revision == revision
    finally:
        release.set()
        await asyncio.gather(old, return_exceptions=True)


@pytest.mark.asyncio
async def test_uncommitted_derived_rows_are_invisible_to_snapshot_reader(monkeypatch):
    import app.accounting.projector as module

    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    entered, release = asyncio.Event(), asyncio.Event()
    original = module.store_checkpoint

    async def pause(*args, **kwargs):
        await original(*args, **kwargs)
        entered.set()
        await release.wait()

    async with factory() as session, session.begin():
        await write_event_batch(session, wallet, wallet, [(raw("a", 100), wallet)])
    monkeypatch.setattr(module, "store_checkpoint", pause)

    async def publish():
        async with factory() as session, session.begin():
            await AccountingProjector().catch_up_wallet(session, wallet)

    task = asyncio.create_task(publish())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with wallet_read_session(wallet) as reader:
            total = await reader.get(WalletTotal, wallet)
            assert total.total_trade_count == total.last_applied_api_order == 0
            assert (
                await reader.scalar(
                    select(func.count())
                    .select_from(PositionLedger)
                    .where(PositionLedger.wallet == wallet)
                )
                == 0
            )
            release.set()
            await task
            await reader.refresh(total)
            assert total.total_trade_count == 0
        async with wallet_read_session(wallet) as reader:
            assert (await reader.get(WalletTotal, wallet)).total_trade_count == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_retention_falls_back_for_correction_older_than_saved_snapshots(monkeypatch):
    monkeypatch.setattr(get_settings(), "accounting_checkpoint_keep", 2)
    wallet = "0x" + uuid4().hex + "0" * 8
    async with wallet_session_factory(wallet)() as session:
        projector = AccountingProjector(condition_replay=True)
        for timestamp in (100, 200, 300):
            await write_event_batch(
                session, wallet, wallet, [(raw("a", timestamp), f"{wallet}-{timestamp}")]
            )
            await projector.catch_up_wallet(session, wallet)
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AccountingCheckpoint)
                .where(AccountingCheckpoint.wallet == wallet)
            )
            == 2
        )
        await write_event_batch(session, wallet, wallet, [(raw("a", 150), wallet + "-late")])
        result = await projector.catch_up_wallet(session, wallet)
        assert result.conditions_replayed == 0
        assert result.events_processed == 4
        before = await economic_state(session, wallet)
        await projector.recompute_wallet(session, wallet)
        assert await economic_state(session, wallet) == before
        await session.rollback()


@pytest.mark.asyncio
async def test_slow_wallet_does_not_prevent_next_bucket_member(monkeypatch):
    first = "0x" + uuid4().hex + "0" * 8
    second = first
    while second == first or wallet_bucket(second) != wallet_bucket(first):
        second = "0x" + uuid4().hex + "0" * 8
    worker = Worker()
    worker.active_wallets = AsyncMock(return_value=[first, second])
    visited = []

    async def cycle(wallet, connection):
        visited.append(wallet)
        if wallet == first:
            await asyncio.Event().wait()

    worker.wallet_cycle = cycle
    monkeypatch.setattr(get_settings(), "wallet_cycle_timeout_seconds", 0.05)
    try:
        await asyncio.wait_for(worker.poll_cycle(), 3)
        assert visited == [first, second]
        assert worker.stats["C"]["errors"] == 1
    finally:
        await worker.close()
