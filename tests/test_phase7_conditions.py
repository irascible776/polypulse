"""Phase 7 regression cases. Prepared during the runtime-testing hold."""

import asyncio
import threading
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from app.accounting.engine import compute_condition_events
from app.accounting.executor import ConditionExecutor
from app.accounting.types import AccountingEvent

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def event(order, condition="c", **changes):
    base = AccountingEvent(
        source_event_id=str(order),
        api_order=order,
        event_type="TRADE",
        event_time=NOW,
        wallet="wallet",
        condition_id=condition,
        token_id="yes",
        outcome_index=0,
        side="BUY",
        size=Decimal(3),
        usdc_size=Decimal(1),
        price_reported=Decimal(0),
        cash_price_gap=False,
        raw={},
    )
    return replace(base, **changes)


def test_condition_redeem_observes_prior_events_and_preserves_input():
    positions = {}
    events = [event(1), event(2, event_type="REDEEM", token_id=None, usdc_size=Decimal(3))]
    effects = compute_condition_events("wallet", "c", events, positions, NOW)
    assert positions == {}
    assert events[1].token_id is None
    assert effects[1].event.token_id == "yes"
    assert effects[1].position.shares == 0
    assert effects[1].result.success.realized_pnl_delta == Decimal(2)


def test_condition_ambiguous_redemption_does_not_change_positions():
    effects = compute_condition_events(
        "wallet",
        "c",
        [
            event(1),
            event(2, token_id="no", outcome_index=1),
            event(3, event_type="REDEEM", token_id=None, outcome_index=None),
        ],
        {},
        NOW,
    )
    assert effects[-1].result.reject.reject_reason == "AMBIGUOUS_REDEEM_TOKEN"
    assert effects[-1].position is None


@pytest.mark.parametrize("changes", [{"wallet": "other"}, {"condition_id": "other"}])
def test_misrouted_input_fails(changes):
    with pytest.raises(ValueError, match="another wallet or condition"):
        compute_condition_events("wallet", "c", [event(1, **changes)], {}, NOW)


@pytest.mark.asyncio
async def test_process_results_match_serial_and_can_reuse_pool():
    executor = ConditionExecutor(2)
    work = [
        (condition, [event(1, condition), event(2, condition, side="SELL")], {})
        for condition in ("a", "b", "c")
    ]
    try:
        for _ in range(2):
            results = await executor.compute("wallet", work, NOW, 30)
            assert [r[3] for r in results] == [
                compute_condition_events("wallet", condition, events, positions, NOW)
                for condition, events, positions in work
            ]
            assert all(r[4] > 0 and r[5] <= r[6] for r in results)
            assert not executor._pending
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_timeout_discards_generation_and_next_call_starts_fresh():
    executor = ConditionExecutor(1)
    work = [("c", [event(1)], {})]
    try:
        with pytest.raises(TimeoutError):
            await executor.compute("wallet", work, NOW, 0)
        assert executor._pool is None
        result = await executor.compute("wallet", work, NOW, 30)
        assert result[0][3][0].position.shares == 3
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_cancelled_cleanup_remains_owned_until_children_are_reaped():
    entered, release, joined = threading.Event(), threading.Event(), threading.Event()

    class Pool:
        def terminate(self):
            entered.set()
            assert release.wait(5)

        def join(self):
            joined.set()

    executor = ConditionExecutor(1)
    pool = Pool()
    executor._pool = pool
    discard = asyncio.create_task(executor._discard(pool))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        discard.cancel()
        with pytest.raises(asyncio.CancelledError):
            await discard
        close = asyncio.create_task(executor.close())
        await asyncio.sleep(0)
        assert not close.done()
        release.set()
        await asyncio.wait_for(close, 2)
        assert joined.is_set()
        assert executor._cleanup_task.done()
    finally:
        release.set()
        await executor.close()
