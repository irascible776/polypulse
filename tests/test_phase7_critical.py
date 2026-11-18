"""Critical recovery drills: run only against disposable PostgreSQL databases."""

import asyncio
import os
import time
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.exc import DBAPIError

from app.accounting.executor import ConditionExecutor
from app.accounting.projector import AccountingProjector
from app.ingestion.writer import write_event_batch
from app.storage.database import wallet_session_factory
from app.storage.models import (
    AccountingCheckpoint,
    ConditionCheckpoint,
    ConditionReplayDirty,
    Position,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    WalletTotal,
)
from tests.test_phase7_conditions import NOW, event
from tests.test_phase7_replay import economic_state, raw


async def durable_state(session, wallet):
    state = {}
    for model in (
        Position,
        PositionLedger,
        PositionLedgerReject,
        WalletTotal,
        ProjectorCursor,
        AccountingCheckpoint,
        ConditionCheckpoint,
        ConditionReplayDirty,
    ):
        rows = (
            await session.execute(select(*model.__table__.columns).where(model.wallet == wallet))
        ).all()
        state[model.__tablename__] = sorted((tuple(row) for row in rows), key=repr)
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("server_committed", [False, True])
async def test_uncertain_commit_retries_from_durable_progress(monkeypatch, server_committed):
    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    async with factory() as session, session.begin():
        await write_event_batch(session, wallet, wallet, [(raw("a", 100), wallet)])
    original = psycopg.AsyncConnection.commit
    target_pid = None
    injected = False

    async def uncertain_commit(connection):
        nonlocal injected
        if connection.info.backend_pid == target_pid and not injected:
            injected = True
            if server_committed:
                await original(connection)
            await connection.close()
            raise psycopg.OperationalError("injected commit outcome unavailable to caller")
        await original(connection)

    monkeypatch.setattr(psycopg.AsyncConnection, "commit", uncertain_commit)
    with pytest.raises(DBAPIError):
        async with factory() as session:
            target_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            await AccountingProjector().catch_up_wallet(session, wallet)
            await session.commit()
    assert injected
    async with factory() as session:
        before = await durable_state(session, wallet)
        total = await session.get(WalletTotal, wallet)
        assert total.total_trade_count == int(server_committed)
    async with factory() as session, session.begin():
        retry = await AccountingProjector().catch_up_wallet(session, wallet)
        assert retry.events_processed == (0 if server_committed else 1)
    async with factory() as session:
        if server_committed:
            assert await durable_state(session, wallet) == before
        corrected = await economic_state(session, wallet)
        assert (await session.get(WalletTotal, wallet)).total_trade_count == 1
        await AccountingProjector().recompute_wallet(session, wallet)
        assert await economic_state(session, wallet) == corrected
        await session.rollback()


@pytest.mark.asyncio
async def test_late_ingestion_waits_for_publication_then_marks_correction(monkeypatch):
    import app.accounting.projector as module

    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    entered, release, writer_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = module.store_checkpoint
    writer_pid = None

    async with factory() as session, session.begin():
        await write_event_batch(session, wallet, wallet, [(raw("a", 100), wallet + "-100")])
        await AccountingProjector().catch_up_wallet(session, wallet)
        await write_event_batch(session, wallet, wallet, [(raw("a", 200), wallet + "-200")])

    async def pause(*args, **kwargs):
        await original(*args, **kwargs)
        entered.set()
        await release.wait()

    monkeypatch.setattr(module, "store_checkpoint", pause)

    async def publish():
        async with factory() as session, session.begin():
            await AccountingProjector().catch_up_wallet(session, wallet)

    async def late_arrival():
        nonlocal writer_pid
        async with factory() as session, session.begin():
            writer_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            writer_started.set()
            await write_event_batch(session, wallet, wallet, [(raw("a", 150), wallet + "-late")])

    publisher = asyncio.create_task(publish())
    writer = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        writer = asyncio.create_task(late_arrival())
        await asyncio.wait_for(writer_started.wait(), 5)
        async with asyncio.timeout(3):
            while True:
                async with factory() as reader:
                    blocked = await reader.scalar(
                        text(
                            "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid=:pid"
                        ),
                        {"pid": writer_pid},
                    )
                if blocked:
                    break
                await asyncio.sleep(0.01)
        assert not writer.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(publisher, writer), 5)
        monkeypatch.setattr(module, "store_checkpoint", original)
        async with factory() as session, session.begin():
            assert (await session.get(WalletTotal, wallet)).recompute_needed
            assert (await session.get(ProjectorCursor, wallet)).last_applied_api_order == 2
            corrected = await AccountingProjector(condition_replay=True).catch_up_wallet(
                session, wallet
            )
            assert corrected.conditions_replayed == 1
            state = await economic_state(session, wallet)
            await AccountingProjector().recompute_wallet(session, wallet)
            assert await economic_state(session, wallet) == state
    finally:
        release.set()
        tasks = [publisher] + ([writer] if writer else [])
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "version", "state"])
async def test_incompatible_condition_snapshot_falls_back_without_changing_economics(fault):
    wallet = "0x" + uuid4().hex + "0" * 8
    async with wallet_session_factory(wallet)() as session:
        projector = AccountingProjector(condition_replay=True)
        await write_event_batch(session, wallet, wallet, [(raw("a", 100), wallet + "-buy")])
        await projector.catch_up_wallet(session, wallet)
        saved = await session.scalar(
            select(ConditionCheckpoint).where(ConditionCheckpoint.wallet == wallet)
        )
        if fault == "missing":
            await session.execute(
                delete(ConditionCheckpoint).where(ConditionCheckpoint.wallet == wallet)
            )
        elif fault == "version":
            saved.format_version = 99
        else:
            saved.positions = [dict(saved.positions[0], shares="999")]
        await write_event_batch(
            session, wallet, wallet, [(raw("a", 200, "SELL"), wallet + "-sell")]
        )
        await projector.catch_up_wallet(session, wallet)
        await write_event_batch(
            session, wallet, wallet, [(raw("a", 150, usdc_size="2"), wallet + "-late")]
        )
        result = await projector.catch_up_wallet(session, wallet)
        assert result.is_recompute and result.conditions_replayed == 0
        position = await session.get(Position, (wallet, "a", "a-token"))
        assert position.shares == 3
        assert position.avg_cost_basis == Decimal("0.5")
        assert position.realized_pnl == -position.avg_cost_basis
        corrected = await economic_state(session, wallet)
        await projector.recompute_wallet(session, wallet)
        assert await economic_state(session, wallet) == corrected
        await session.rollback()


def blocked_child(attempt, wallet, condition, events, positions, now):
    # Spawn-importable barrier proves the assigned task has entered this child.
    Path(events[0].raw["marker"]).write_text(str(os.getpid()))
    time.sleep(30)
    raise AssertionError("child should have been terminated")


@pytest.mark.asyncio
async def test_killed_child_aborts_generation_and_fresh_work_progresses(monkeypatch, tmp_path):
    import app.accounting.executor as module

    original = module._compute
    monkeypatch.setattr(module, "_compute", blocked_child)
    executor = ConditionExecutor(1)
    marker = tmp_path / "child-started"
    task = asyncio.create_task(
        executor.compute("wallet", [("c", [event(1, raw={"marker": str(marker)})], {})], NOW, 3)
    )
    try:
        async with asyncio.timeout(5):
            while not marker.exists():
                await asyncio.sleep(0.01)
        child = executor._pool._pool[0]
        assert child.pid == int(marker.read_text())
        child.kill()
        with pytest.raises((TimeoutError, RuntimeError)):
            await asyncio.wait_for(task, 5)
        assert executor._pool is None and not executor._pending
        assert not child.is_alive()
        monkeypatch.setattr(module, "_compute", original)
        results = await executor.compute("wallet", [("c", [event(1)], {})], NOW, 10)
        assert results[0][3][0].position.shares == 3
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await executor.close()
