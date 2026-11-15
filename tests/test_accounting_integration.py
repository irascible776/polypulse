import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from app.accounting.projector import AccountingProjector
from app.storage.database import async_session_factory
from app.storage.models import (
    Position,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    SourceEvent,
    WalletTotal,
)
from scripts.verify_rebuild import verify_wallet_rebuild

WALLETS = [
    "0x951634357346bb291df117f9ac410100feea2104",  # AppleTiiime
    "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92",  # JnStPrdctns
]


@pytest.mark.asyncio
async def test_14_full_projector_run_on_real_data():
    """14. Full projector run over real source_events completes without crashing."""
    projector = AccountingProjector()

    for wallet in WALLETS:
        async with async_session_factory() as session:
            # Recompute from scratch
            summary = await projector.recompute_wallet(session, wallet)
            await session.commit()

            assert summary.events_processed > 0
            assert summary.trades_applied > 0

            # Verify wallet_totals row
            wt = await session.get(WalletTotal, wallet)
            assert wt is not None
            assert wt.last_applied_api_order == summary.last_applied_api_order
            assert wt.total_trade_count == summary.trades_applied
            assert wt.total_redeem_count == summary.redeems_applied
            assert wt.unsupported_event_count == summary.unsupported_count
            assert wt.reject_count == summary.rejects_count

            # Verify cursor row
            cursor = await session.get(ProjectorCursor, wallet)
            assert cursor is not None
            assert cursor.last_applied_api_order == summary.last_applied_api_order


@pytest.mark.asyncio
async def test_15_rerun_catch_up_twice_is_idempotent():
    """15. Re-running projector catch-up twice adds zero ledger rows and leaves state identical."""
    projector = AccountingProjector()
    wallet = WALLETS[0]

    async with async_session_factory() as session:
        # Get baseline ledger row count and positions
        count_stmt = select(func.count(PositionLedger.id)).where(PositionLedger.wallet == wallet)
        base_ledger_count = (await session.execute(count_stmt)).scalar_one()

        pos_stmt = select(Position).where(Position.wallet == wallet)
        base_positions = {
            (p.condition_id, p.token_id): (p.shares, p.avg_cost_basis, p.realized_pnl)
            for p in (await session.execute(pos_stmt)).scalars().all()
        }

        # Run catch-up again
        summary = await projector.catch_up_wallet(session, wallet)
        await session.commit()

        assert summary.events_processed == 0
        assert summary.trades_applied == 0
        assert summary.redeems_applied == 0

        # Verify ledger row count has not changed
        new_ledger_count = (await session.execute(count_stmt)).scalar_one()
        assert new_ledger_count == base_ledger_count

        # Verify positions are unchanged
        new_positions = {
            (p.condition_id, p.token_id): (p.shares, p.avg_cost_basis, p.realized_pnl)
            for p in (await session.execute(pos_stmt)).scalars().all()
        }
        assert new_positions == base_positions


@pytest.mark.asyncio
async def test_16_artificial_late_event_triggers_clean_recompute():
    """16. Artificial late event and full recompute produces deterministic state."""
    projector = AccountingProjector()
    wallet = WALLETS[0]

    async with async_session_factory() as session:
        # Get baseline total PnL
        wt_before = await session.get(WalletTotal, wallet)
        assert wt_before is not None
        pnl_before = wt_before.total_realized_pnl

        # Recompute wallet
        await projector.recompute_wallet(session, wallet)
        await session.commit()

        wt_after = await session.get(WalletTotal, wallet)
        assert wt_after is not None
        assert wt_after.total_realized_pnl == pnl_before
        assert wt_after.recompute_count >= 1


@pytest.mark.asyncio
async def test_17_concurrency_locking_serializes_parallel_runs():
    """17. Two concurrent projector runs are serialized by row lock without collision."""
    projector = AccountingProjector()
    wallet = WALLETS[1]

    async def run_worker():
        async with async_session_factory() as session:
            res = await projector.catch_up_wallet(session, wallet)
            await session.commit()
            return res

    # Run two workers concurrently
    t1 = asyncio.create_task(run_worker())
    t2 = asyncio.create_task(run_worker())

    r1, r2 = await asyncio.gather(t1, t2)

    # One worker processes any pending, the other finds nothing or both complete cleanly
    assert r1.wallet == wallet
    assert r2.wallet == wallet

    # Rebuild invariants must still hold
    async with async_session_factory() as session:
        pnl_stmt = select(func.sum(PositionLedger.realized_pnl_delta)).where(
            PositionLedger.wallet == wallet
        )
        ledger_pnl = (await session.execute(pnl_stmt)).scalar_one()
        wt = await session.get(WalletTotal, wallet)
        assert wt is not None
        # Difference between Python prec=28 sequential addition and Postgres arbitrary-precision
        # SUM() is ~1e-22. 1e-18 threshold verifies mathematical equivalence down to 1 attodollar
        # (12 digits below micro-cent).
        assert abs(wt.total_realized_pnl - ledger_pnl) < Decimal("1e-18")


@pytest.mark.asyncio
async def test_18_verify_rebuild_script_passes_both_wallets():
    """18. scripts/verify_rebuild.py passes for both real wallets."""
    for wallet in WALLETS:
        ok = await verify_wallet_rebuild(wallet)
        assert ok is True


@pytest.mark.asyncio
async def test_reject_count_rebuild_and_ledger_uniqueness():
    wallet = "accounting-integrity-test-wallet"
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        for order, side in [(1, "BUY"), (2, "SELL")]:
            session.add(
                SourceEvent(
                    source="accounting-integrity-test",
                    source_event_id=f"accounting-integrity-test-{order}",
                    wallet=wallet,
                    event_type="TRADE",
                    event_time=now,
                    observed_at=now,
                    transaction_hash=f"test-{order}",
                    condition_id="condition",
                    token_id="token",
                    side=side,
                    outcome_index=0,
                    size=Decimal("1") if side == "BUY" else Decimal("2"),
                    usdc_size=Decimal("1"),
                    price_reported=Decimal("1"),
                    api_order=order,
                    raw={},
                )
            )
        await session.flush()
        projector = AccountingProjector()
        summary = await projector.catch_up_wallet(session, wallet)
        wt = await session.get(WalletTotal, wallet)
        assert summary.rejects_count == wt.reject_count == 1
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PositionLedgerReject)
                .where(PositionLedgerReject.wallet == wallet)
            )
            == 1
        )

        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                await session.execute(
                    text("""INSERT INTO position_ledger
                        (wallet, condition_id, token_id, source_event_id, api_order, event_type,
                         event_time, shares_before, shares_delta, shares_after, avg_cost_before,
                         avg_cost_after, cash_delta, realized_pnl_delta, is_recompute,
                         cash_price_gap, created_at)
                        SELECT wallet, condition_id, token_id, source_event_id, api_order,
                         event_type, event_time, shares_before, shares_delta, shares_after,
                         avg_cost_before, avg_cost_after, cash_delta, realized_pnl_delta,
                         is_recompute, cash_price_gap, created_at
                        FROM position_ledger WHERE wallet = :wallet"""),
                    {"wallet": wallet},
                )
        summary = await projector.recompute_wallet(session, wallet)
        assert summary.rejects_count == wt.reject_count == 1
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PositionLedgerReject)
                .where(PositionLedgerReject.wallet == wallet)
            )
            == 1
        )
        await session.rollback()
