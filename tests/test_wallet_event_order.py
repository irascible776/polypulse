import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from app.accounting.projector import AccountingProjector
from app.ingestion.writer import prepare_source_event_record, write_event_batch
from app.storage.database import async_session_factory
from app.storage.models import Position, PositionLedger, SourceEvent, WalletTotal


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
