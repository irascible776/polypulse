from datetime import datetime, timezone
from decimal import Decimal

from app.accounting.engine import apply_event
from app.accounting.types import AccountingEvent, PositionState


def _make_event(
    event_type: str,
    side: str | None = None,
    size: str = "0",
    usdc_size: str = "0",
    price: str = "0",
    wallet: str = "0xwallet123",
    condition_id: str = "0xcond456",
    token_id: str = "0xtoken789",
    outcome_index: int | None = 1,
    source_event_id: str = "evt1",
    api_order: int = 1,
) -> AccountingEvent:
    return AccountingEvent(
        source_event_id=source_event_id,
        api_order=api_order,
        event_type=event_type,
        event_time=datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
        wallet=wallet,
        condition_id=condition_id,
        token_id=token_id,
        outcome_index=outcome_index,
        side=side,
        size=Decimal(size),
        usdc_size=Decimal(usdc_size),
        price_reported=Decimal(price),
        cash_price_gap=False,
        raw={},
    )


def test_1_single_buy():
    # 1. Single BUY, then query avg cost and shares
    ev = _make_event("TRADE", side="BUY", size="100.5", usdc_size="50.25", price="0.5")
    res = apply_event(None, ev)

    assert res.success is not None
    suc = res.success
    assert suc.shares_after == Decimal("100.5")
    assert suc.avg_cost_after == Decimal("0.5")
    assert suc.cash_delta == Decimal("-50.25")
    assert suc.realized_pnl_delta == Decimal("0")


def test_2_two_buys_weighted_average():
    # 2. Two BUYs at different prices -> correct weighted average
    # Buy 1: 100 shares for 40 USDC (0.40/sh)
    ev1 = _make_event("TRADE", side="BUY", size="100", usdc_size="40", price="0.4")
    res1 = apply_event(None, ev1)
    assert res1.success is not None

    st1 = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=res1.success.shares_after,
        avg_cost_basis=res1.success.avg_cost_after,
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )

    # Buy 2: 100 shares for 60 USDC (0.60/sh)
    ev2 = _make_event("TRADE", side="BUY", size="100", usdc_size="60", price="0.6", api_order=2)
    res2 = apply_event(st1, ev2)
    assert res2.success is not None

    suc2 = res2.success
    assert suc2.shares_after == Decimal("200")
    # Weighted avg: (40 + 60) / 200 = 0.50
    assert suc2.avg_cost_after == Decimal("0.5")
    assert suc2.realized_pnl_delta == Decimal("0")
    assert suc2.cash_delta == Decimal("-60")


def test_3_buy_then_partial_sell():
    # 3. BUY then partial SELL -> avg cost of remainder unchanged; PnL matches
    # Starting position: 200 shares at avg_cost 0.50 (total cost 100 USDC)
    st = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=Decimal("200"),
        avg_cost_basis=Decimal("0.5"),
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )

    # Sell 50 shares for 40 USDC (0.80/sh)
    ev = _make_event("TRADE", side="SELL", size="50", usdc_size="40", price="0.8")
    res = apply_event(st, ev)
    assert res.success is not None

    suc = res.success
    assert suc.shares_after == Decimal("150")
    assert suc.avg_cost_after == Decimal("0.5")  # Cost basis of remainder is unchanged!
    # Realized PnL: proceeds (40) - cost (50 * 0.5 = 25) = 15 USDC
    assert suc.realized_pnl_delta == Decimal("15")
    assert suc.cash_delta == Decimal("40")


def test_4_buy_then_full_sell():
    # 4. BUY then full SELL -> shares and average cost reset; prior cost is retained
    st = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=Decimal("150"),
        avg_cost_basis=Decimal("0.5"),
        realized_pnl=Decimal("15"),
        last_applied_api_order=2,
        updated_at=datetime.now(timezone.utc),
    )

    # Sell all 150 shares for 105 USDC (0.70/sh)
    ev = _make_event("TRADE", side="SELL", size="150", usdc_size="105", price="0.7")
    res = apply_event(st, ev)
    assert res.success is not None

    suc = res.success
    assert suc.shares_after == Decimal("0")
    assert suc.avg_cost_before == Decimal("0.5")
    assert suc.avg_cost_after == Decimal("0")
    # Cost = 150 * 0.5 = 75 USDC. PnL = 105 - 75 = 30 USDC
    assert suc.realized_pnl_delta == Decimal("30")


def test_5_redeem_with_zero_payout_full_loss():
    # 5. REDEEM with usdc_size = 0 -> realized PnL = -shares*avg_cost (full loss), shares -> 0
    st = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=Decimal("100"),
        avg_cost_basis=Decimal("0.4"),
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )

    ev = _make_event("REDEEM", size="100", usdc_size="0")
    res = apply_event(st, ev)
    assert res.success is not None

    suc = res.success
    assert suc.shares_after == Decimal("0")
    assert suc.avg_cost_before == Decimal("0.4")
    assert suc.avg_cost_after == Decimal("0")
    # PnL = 0 - (100 * 0.4) = -40 USDC
    assert suc.realized_pnl_delta == Decimal("-40")
    assert suc.cash_delta == Decimal("0")


def test_6_redeem_with_nonzero_payout():
    # 6. REDEEM with nonzero payout -> matches hand computation
    st = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=Decimal("200"),
        avg_cost_basis=Decimal("0.45"),
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )

    # Market resolves to 1.00; proceeds = 200 USDC
    ev = _make_event("REDEEM", size="200", usdc_size="200")
    res = apply_event(st, ev)
    assert res.success is not None

    suc = res.success
    assert suc.shares_after == Decimal("0")
    assert suc.avg_cost_before == Decimal("0.45")
    assert suc.avg_cost_after == Decimal("0")
    # PnL = 200 - (200 * 0.45 = 90) = 110 USDC
    assert suc.realized_pnl_delta == Decimal("110")
    assert suc.cash_delta == Decimal("200")


def test_7_oversell_and_over_redeem_rejected():
    # 7. SELL/REDEEM exceeding shares held -> rejected, not negative, ledger-rejects row written
    st = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=Decimal("50"),
        avg_cost_basis=Decimal("0.5"),
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )

    # Attempt to sell 60 shares (only hold 50)
    ev_sell = _make_event("TRADE", side="SELL", size="60", usdc_size="48")
    res_sell = apply_event(st, ev_sell)
    assert res_sell.reject is not None
    assert res_sell.reject.reject_reason == "SELLING_MORE_THAN_HELD"

    # Attempt to redeem when holding 0 shares
    st_empty = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )
    ev_redeem = _make_event("REDEEM", size="50", usdc_size="50")
    res_redeem = apply_event(st_empty, ev_redeem)
    assert res_redeem.reject is not None
    assert res_redeem.reject.reject_reason == "REDEEM_WITH_ZERO_SHARES"


def test_8_unsupported_events_do_not_mutate():
    # 8. Unsupported/unknown event type -> zero effect on shares/PnL, no crash
    for utype in ("MAKER_REBATE", "TAKER_REBATE", "REFERRAL_REWARD", "YIELD", "MERGE", "FOO_BAR"):
        ev = _make_event(utype, size="100", usdc_size="10")
        res = apply_event(None, ev)
        assert res.is_unsupported is True
        assert res.success is None
        assert res.reject is None


def test_9_decimal_precision_repeating_division():
    # 9. Decimal precision: repeating-decimal division stays exact, zero float contamination
    # Buy 3 shares for 1 USDC -> avg_cost = 1/3
    ev1 = _make_event("TRADE", side="BUY", size="3", usdc_size="1", price="0.3333333333333333")
    res1 = apply_event(None, ev1)
    assert res1.success is not None

    st1 = PositionState(
        wallet="0xwallet123",
        condition_id="0xcond456",
        token_id="0xtoken789",
        outcome_index=1,
        shares=res1.success.shares_after,
        avg_cost_basis=res1.success.avg_cost_after,
        realized_pnl=Decimal("0"),
        last_applied_api_order=1,
        updated_at=datetime.now(timezone.utc),
    )

    # Buy 3 more shares for 2 USDC -> total shares = 6, total cost = 3 USDC -> avg_cost = 0.5
    ev2 = _make_event("TRADE", side="BUY", size="3", usdc_size="2")
    res2 = apply_event(st1, ev2)
    assert res2.success is not None
    assert res2.success.shares_after == Decimal("6")
    assert res2.success.avg_cost_after == Decimal("0.5")

    # Strict type assertion: no floats anywhere
    assert isinstance(res1.success.shares_after, Decimal)
    assert isinstance(res1.success.avg_cost_after, Decimal)
    assert isinstance(res1.success.cash_delta, Decimal)
    assert isinstance(res1.success.realized_pnl_delta, Decimal)
    assert not isinstance(res1.success.avg_cost_after, float)
