import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError

from app.accounting.projector import AccountingProjector
from app.config import get_settings
from app.ingestion.writer import prepare_source_event_record, write_event_batch
from app.storage.database import async_session_factory
from app.storage.models import Position, PositionLedger, SourceEvent, WalletTotal


@pytest.mark.asyncio
async def test_rejects_event_for_a_different_wallet_without_advancing_cursor():
    wallet = f"target-{uuid.uuid4()}"
    event = {"type": "REWARD", "timestamp": 1700000000, "proxy_wallet": "another-wallet"}
    async with async_session_factory() as session:
        with pytest.raises(ValueError, match="does not match"):
            await write_event_batch(session, wallet, "fixture", [(event, "event")])
        from app.storage.models import IngestionCursor

        assert await session.get(IngestionCursor, (wallet, "fixture")) is None
        assert (
            await session.scalar(
                select(func.count()).select_from(SourceEvent).where(SourceEvent.wallet == wallet)
            )
            == 0
        )
        await session.rollback()


@pytest.mark.asyncio
async def test_full_rebuild_processes_multiple_replay_batches():
    wallet = f"replay-batches-{uuid.uuid4()}"
    events = [
        ({"type": "REWARD", "timestamp": 1700000000 + i}, f"{wallet}-{i}") for i in range(5001)
    ]
    async with async_session_factory() as session:
        await write_event_batch(session, wallet, "fixture", events)
        summary = await AccountingProjector().recompute_wallet(session, wallet)
        assert summary.events_processed == 5001
        assert summary.unsupported_count == 5001
        assert summary.last_applied_api_order == 5001
        await session.rollback()


@pytest.mark.asyncio
async def test_copy_batch_rolls_back_events_and_cursor(monkeypatch):
    wallet = f"copy-rollback-{uuid.uuid4()}"
    monkeypatch.setattr(get_settings(), "ingestion_copy_threshold", 1)
    event = {"type": "REWARD", "timestamp": 1700000000, "size": "1"}
    async with async_session_factory() as session:
        with pytest.raises(RuntimeError, match="interrupt"):
            async with session.begin():
                await write_event_batch(session, wallet, "fixture", [(event, wallet)])
                raise RuntimeError("interrupt")
    async with async_session_factory() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(SourceEvent).where(SourceEvent.wallet == wallet)
            )
            == 0
        )
        from app.storage.models import IngestionCursor

        assert await session.get(IngestionCursor, (wallet, "fixture")) is None


@pytest.mark.asyncio
async def test_copy_and_chunked_inserts_produce_same_events(monkeypatch):
    wallet = f"copy-{uuid.uuid4()}"
    events = [
        ({"type": "REWARD", "timestamp": 1700000000 + i, "size": Decimal("1.5")}, f"{wallet}-{i}")
        for i in range(1001)
    ]
    results = []
    for threshold in (0, 1):
        monkeypatch.setattr(get_settings(), "ingestion_copy_threshold", threshold)
        async with async_session_factory() as session:
            assert await write_event_batch(session, wallet, "fixture", events + events[:1]) == (
                1001,
                1,
            )
            rows = (
                await session.execute(
                    select(
                        SourceEvent.source_event_id,
                        SourceEvent.api_order,
                        SourceEvent.size,
                        SourceEvent.raw,
                    )
                    .where(SourceEvent.wallet == wallet)
                    .order_by(SourceEvent.api_order)
                )
            ).all()
            results.append(rows)
            await session.rollback()
    assert results[0] == results[1]


@pytest.mark.asyncio
async def test_late_event_replays_suffix_from_checkpoint_and_matches_full_rebuild():
    wallet = f"checkpoint-{uuid.uuid4()}"
    base = 1700000000

    def event(side, timestamp, cash):
        return {
            "type": "TRADE",
            "side": side,
            "timestamp": base + timestamp,
            "size": "1",
            "usdc_size": cash,
            "condition_id": "condition",
            "token_id": "token",
        }

    async with async_session_factory() as session:
        projector = AccountingProjector()
        for number, item in enumerate(
            [event("BUY", 1, "0.4"), event("BUY", 3, "0.6"), event("SELL", 5, "0.8")]
        ):
            await write_event_batch(session, wallet, "fixture", [(item, f"{wallet}-{number}")])
            await projector.catch_up_wallet(session, wallet)
        await write_event_batch(
            session, wallet, "fixture", [(event("BUY", 4, "0.7"), f"{wallet}-late")]
        )
        result = await projector.catch_up_wallet(session, wallet)
        assert result.is_recompute and result.events_processed == 2
        position = await session.get(Position, (wallet, "condition", "token"))
        total = await session.get(WalletTotal, wallet)
        expected = (
            position.shares,
            position.avg_cost_basis,
            position.realized_pnl,
            total.total_realized_pnl,
            total.total_trade_count,
        )
        await projector.recompute_wallet(session, wallet)
        position = await session.get(Position, (wallet, "condition", "token"))
        actual = (
            position.shares,
            position.avg_cost_basis,
            position.realized_pnl,
            total.total_realized_pnl,
            total.total_trade_count,
        )
        assert actual == expected
        await session.rollback()


@pytest.mark.asyncio
async def test_projector_initialization_is_safe_for_concurrent_runs():
    wallet = f"projector-concurrent-{uuid.uuid4()}"

    async def project():
        async with async_session_factory() as session:
            async with session.begin():
                return await AccountingProjector().catch_up_wallet(session, wallet)

    results = await asyncio.gather(project(), project())
    assert [result.events_processed for result in results] == [0, 0]
    async with async_session_factory() as session:
        from app.storage.models import ProjectorCursor

        await session.execute(delete(ProjectorCursor).where(ProjectorCursor.wallet == wallet))
        await session.execute(delete(WalletTotal).where(WalletTotal.wallet == wallet))
        await session.commit()


@pytest.mark.asyncio
async def test_concurrent_sources_serialize_wallet_order():
    wallet = f"concurrent-{uuid.uuid4()}"
    event = {"type": "REWARD", "timestamp": 1700000000, "size": "1"}
    entered = asyncio.Event()

    async with async_session_factory() as first:
        await write_event_batch(first, wallet, "first", [(event, f"{wallet}-1")])

        async def second_write():
            async with async_session_factory() as second:
                entered.set()
                result = await write_event_batch(second, wallet, "second", [(event, f"{wallet}-2")])
                await second.commit()
                return result

        task = asyncio.create_task(second_write())
        await entered.wait()
        await asyncio.sleep(0.1)
        assert not task.done()
        await first.commit()
        assert await asyncio.wait_for(task, 5) == (1, 0)

    async with async_session_factory() as session:
        orders = (
            (
                await session.execute(
                    select(SourceEvent.api_order)
                    .where(SourceEvent.wallet == wallet)
                    .order_by(SourceEvent.api_order)
                )
            )
            .scalars()
            .all()
        )
        assert orders == [1, 2]
        await session.execute(delete(SourceEvent).where(SourceEvent.wallet == wallet))
        from app.storage.models import IngestionCursor

        await session.execute(delete(IngestionCursor).where(IngestionCursor.wallet == wallet))
        await session.commit()


@pytest.mark.asyncio
async def test_large_batch_deduplicates_across_chunks():
    wallet = f"chunk-test-{uuid.uuid4()}"
    event = {"type": "REWARD", "timestamp": 1700000000, "size": "1"}
    events = [(event, f"event-{i}") for i in range(1001)]
    events.append(events[0])
    async with async_session_factory() as session:
        assert await write_event_batch(session, wallet, "fixture", events) == (1001, 1)
        assert await write_event_batch(session, wallet, "fixture", events) == (0, 1002)
        rows = (
            (
                await session.execute(
                    select(SourceEvent.api_order)
                    .where(SourceEvent.wallet == wallet)
                    .order_by(SourceEvent.api_order)
                )
            )
            .scalars()
            .all()
        )
        assert rows == list(range(1, 1002))
        await session.rollback()


@pytest.mark.asyncio
async def test_wallet_order_dedup_across_sources_and_collision():
    wallet = f"order-test-{uuid.uuid4()}"
    now = datetime.now(timezone.utc)
    first = {"type": "TRADE", "side": "BUY", "timestamp": int(now.timestamp()), "size": "1"}
    second = {**first, "size": "2"}
    async with async_session_factory() as session:
        assert await write_event_batch(session, wallet, "fixture", [(first, "id-1")]) == (1, 0)
        assert await write_event_batch(session, wallet, "fixture", [(first, "id-1")]) == (0, 1)
        assert await write_event_batch(session, wallet, "live", [(first, "id-1")]) == (0, 1)
        assert await write_event_batch(session, wallet, "fresh", [(second, "id-2")]) == (1, 0)
        rows = (
            (await session.execute(select(SourceEvent).where(SourceEvent.wallet == wallet)))
            .scalars()
            .all()
        )
        assert sorted((row.source_event_id, row.api_order) for row in rows) == [
            ("id-1", 1),
            ("id-2", 2),
        ]
        collision = prepare_source_event_record(second, "id-3", wallet, "third", 2, False, now)
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                session.add(SourceEvent(**collision))
                await session.flush()
        await session.rollback()


@pytest.mark.asyncio
async def test_late_event_marks_wallet_and_replays_in_event_time_order():
    wallet = f"late-test-{uuid.uuid4()}"
    base = 1700000000

    def event(side, timestamp, size, cash):
        return {
            "type": "TRADE",
            "side": side,
            "timestamp": timestamp,
            "size": str(size),
            "usdc_size": str(cash),
            "condition_id": "condition",
            "token_id": "token",
        }

    async with async_session_factory() as session:
        first = [(event("BUY", base, 10, 5), "one"), (event("SELL", base + 2, 10, 8), "three")]
        assert await write_event_batch(session, wallet, "fixture", first) == (2, 0)
        projector = AccountingProjector()
        await projector.catch_up_wallet(session, wallet)
        assert await write_event_batch(
            session, wallet, "live", [(event("BUY", base + 1, 10, 7), "two")]
        ) == (1, 0)
        total = await session.get(WalletTotal, wallet, populate_existing=True)
        assert total.recompute_needed is True
        late = await session.scalar(select(SourceEvent).where(SourceEvent.source_event_id == "two"))
        assert late.is_late is True
        summary = await projector.catch_up_wallet(session, wallet)
        assert summary.is_recompute is True
        assert summary.last_applied_api_order == 3
        position = await session.get(Position, (wallet, "condition", "token"))
        assert position.shares == 10
        assert position.avg_cost_basis == Decimal("0.6")
        assert total.recompute_needed is False
        rows = (
            await session.execute(
                select(
                    PositionLedger.source_event_id,
                    PositionLedger.event_time,
                    PositionLedger.api_order,
                )
                .where(PositionLedger.wallet == wallet)
                .order_by(PositionLedger.id)
            )
        ).all()
        assert [row.source_event_id for row in rows] == ["one", "two", "three"]
        assert [(row.event_time, row.api_order) for row in rows] == sorted(
            (row.event_time, row.api_order) for row in rows
        )
        await session.rollback()


@pytest.mark.asyncio
async def test_late_event_restores_economic_positions_and_ledger():
    wallet = f"late-restore-{uuid.uuid4()}"
    base = 1700000000

    def event(side, timestamp, size, cash):
        return {
            "type": "TRADE",
            "side": side,
            "timestamp": timestamp,
            "size": str(size),
            "usdc_size": str(cash),
            "condition_id": "condition",
            "token_id": "token",
        }

    async def snapshot(session):
        total = await session.get(WalletTotal, wallet, populate_existing=True)
        positions = (
            await session.execute(
                select(
                    Position.outcome_index,
                    Position.shares,
                    Position.avg_cost_basis,
                    Position.realized_pnl,
                    Position.last_applied_api_order,
                ).where(Position.wallet == wallet)
            )
        ).all()
        ledger = (
            await session.execute(
                select(
                    PositionLedger.source_event_id,
                    PositionLedger.event_time,
                    PositionLedger.shares_before,
                    PositionLedger.shares_delta,
                    PositionLedger.shares_after,
                    PositionLedger.avg_cost_before,
                    PositionLedger.avg_cost_after,
                    PositionLedger.cash_delta,
                    PositionLedger.realized_pnl_delta,
                    PositionLedger.api_order,
                ).where(PositionLedger.wallet == wallet)
            )
        ).all()
        return (
            total.total_realized_pnl,
            positions,
            {row.source_event_id: tuple(row[1:]) for row in ledger},
        )

    middle = event("BUY", base + 1, 10, 7)
    async with async_session_factory() as session:
        projector = AccountingProjector()
        events = [
            (event("BUY", base, 10, 5), "one"),
            (middle, "two"),
            (event("SELL", base + 2, 10, 8), "three"),
        ]
        assert await write_event_batch(session, wallet, "fixture", events) == (3, 0)
        await projector.catch_up_wallet(session, wallet)
        before_pnl, before_positions, before_ledger = await snapshot(session)
        await session.execute(
            delete(SourceEvent).where(
                SourceEvent.wallet == wallet, SourceEvent.source_event_id == "two"
            )
        )
        await projector.recompute_wallet(session, wallet)
        assert await write_event_batch(session, wallet, "live", [(middle, "two")]) == (1, 0)
        total = await session.get(WalletTotal, wallet, populate_existing=True)
        assert total.recompute_needed is True
        assert (await projector.catch_up_wallet(session, wallet)).is_recompute is True
        after_pnl, after_positions, after_ledger = await snapshot(session)
        assert after_pnl == before_pnl
        assert after_positions == before_positions
        assert before_ledger.keys() == after_ledger.keys()
        for event_id in before_ledger:
            before, after = before_ledger[event_id], after_ledger[event_id]
            assert before[:-1] == after[:-1]
            if event_id != "two":
                assert before[-1] == after[-1]
        await session.rollback()
