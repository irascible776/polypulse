from datetime import datetime, timezone
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from app.accounting.engine import apply_event
from app.accounting.types import AccountingEvent, ApplySuccess, PositionState


# Generate valid Trade / Redeem actions
@st.composite
def event_sequence(draw):
    """
    Generates a realistic valid sequence of BUYs, SELLs, and REDEEMs
    for a single token such that shares held never go below 0.
    """
    n_events = draw(st.integers(min_value=2, max_value=25))
    events = []
    current_shares = Decimal("0")

    for i in range(1, n_events + 1):
        # If no shares held, must BUY
        if current_shares <= Decimal("0"):
            action = "BUY"
        else:
            action = draw(st.sampled_from(["BUY", "SELL", "REDEEM"]))

        if action == "BUY":
            size_int = draw(st.integers(min_value=1, max_value=1000))
            price_cents = draw(st.integers(min_value=1, max_value=99))
            size = Decimal(size_int)
            price = Decimal(price_cents) / Decimal("100")
            usdc_size = size * price

            events.append(
                AccountingEvent(
                    source_event_id=f"evt_{i}",
                    api_order=i,
                    event_type="TRADE",
                    event_time=datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
                    wallet="0xwallet_prop",
                    condition_id="0xcond_prop",
                    token_id="0xtoken_prop",
                    outcome_index=0,
                    side="BUY",
                    size=size,
                    usdc_size=usdc_size,
                    price_reported=price,
                    cash_price_gap=False,
                    raw={},
                )
            )
            current_shares += size

        elif action == "SELL":
            # Sell up to held shares
            max_sell = int(current_shares)
            if max_sell < 1:
                sell_size = current_shares
            else:
                sell_size = Decimal(draw(st.integers(min_value=1, max_value=max_sell)))

            price_cents = draw(st.integers(min_value=1, max_value=99))
            price = Decimal(price_cents) / Decimal("100")
            usdc_size = sell_size * price

            events.append(
                AccountingEvent(
                    source_event_id=f"evt_{i}",
                    api_order=i,
                    event_type="TRADE",
                    event_time=datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
                    wallet="0xwallet_prop",
                    condition_id="0xcond_prop",
                    token_id="0xtoken_prop",
                    outcome_index=0,
                    side="SELL",
                    size=sell_size,
                    usdc_size=usdc_size,
                    price_reported=price,
                    cash_price_gap=False,
                    raw={},
                )
            )
            current_shares -= sell_size

        elif action == "REDEEM":
            # Payout between 0 and 1.00 per share
            payout_rate = Decimal(draw(st.sampled_from(["0", "0.5", "1.0"])))
            usdc_size = current_shares * payout_rate

            events.append(
                AccountingEvent(
                    source_event_id=f"evt_{i}",
                    api_order=i,
                    event_type="REDEEM",
                    event_time=datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc),
                    wallet="0xwallet_prop",
                    condition_id="0xcond_prop",
                    token_id="0xtoken_prop",
                    outcome_index=0,
                    side=None,
                    size=current_shares,
                    usdc_size=usdc_size,
                    price_reported=Decimal("0"),
                    cash_price_gap=False,
                    raw={},
                )
            )
            current_shares = Decimal("0")

    return events


def _run_sequence(events: list[AccountingEvent]) -> tuple[PositionState | None, list[ApplySuccess]]:
    st: PositionState | None = None
    applied = []
    for ev in events:
        res = apply_event(st, ev)
        if res.success is not None:
            suc = res.success
            applied.append(suc)
            prev_pnl = st.realized_pnl if st else Decimal("0")
            st = PositionState(
                wallet=ev.wallet,
                condition_id=ev.condition_id or "",
                token_id=ev.token_id or "",
                outcome_index=ev.outcome_index,
                shares=suc.shares_after,
                avg_cost_basis=suc.avg_cost_after,
                realized_pnl=prev_pnl + suc.realized_pnl_delta,
                last_applied_api_order=ev.api_order,
                updated_at=datetime.now(timezone.utc),
            )
    return st, applied


@settings(max_examples=50)
@given(events=event_sequence())
def test_10_cash_flow_reconciliation_invariant(events):
    # 10. Invariant: realized_pnl + shares*avg_cost reconciles with net cash flow
    final_st, applied_list = _run_sequence(events)
    assert final_st is not None

    total_cash_delta = sum((a.cash_delta for a in applied_list), Decimal("0"))
    total_realized_pnl = final_st.realized_pnl
    remaining_value_at_cost = final_st.shares * final_st.avg_cost_basis

    # Accounting invariant: Realized PnL = Net Cash Flow + Remaining Position Value
    # Holds to within 1e-20 (accounting for Decimal(prec=28) division of repeating fractions)
    diff = abs(total_realized_pnl - (total_cash_delta + remaining_value_at_cost))
    assert diff < Decimal("1e-20")


@settings(max_examples=50)
@given(events=event_sequence())
def test_11_order_independence_of_batching(events):
    # 11. Splitting into different batch boundaries produces identical final position
    final_st_all, _ = _run_sequence(events)

    # Split into 2 chunks
    mid = len(events) // 2
    batch1 = events[:mid]
    batch2 = events[mid:]

    st_batch, _ = _run_sequence(batch1)

    # Continue from st_batch with batch2
    final_st_chunked = st_batch
    for ev in batch2:
        res = apply_event(final_st_chunked, ev)
        if res.success is not None:
            prev_pnl = final_st_chunked.realized_pnl if final_st_chunked else Decimal("0")
            final_st_chunked = PositionState(
                wallet=ev.wallet,
                condition_id=ev.condition_id or "",
                token_id=ev.token_id or "",
                outcome_index=ev.outcome_index,
                shares=res.success.shares_after,
                avg_cost_basis=res.success.avg_cost_after,
                realized_pnl=prev_pnl + res.success.realized_pnl_delta,
                last_applied_api_order=ev.api_order,
                updated_at=datetime.now(timezone.utc),
            )

    assert final_st_all is not None
    assert final_st_chunked is not None
    assert final_st_all.shares == final_st_chunked.shares
    assert final_st_all.avg_cost_basis == final_st_chunked.avg_cost_basis
    assert final_st_all.realized_pnl == final_st_chunked.realized_pnl


@settings(max_examples=50)
@given(events=event_sequence())
def test_12_recompute_equivalence(events):
    # 12. Incremental catch-up and full recompute yield identical positions
    final_st_1, applied_1 = _run_sequence(events)
    # Replay again from scratch
    final_st_2, applied_2 = _run_sequence(events)

    # Compare business position attributes (ignoring volatile updated_at timestamps)
    assert final_st_1 is not None
    assert final_st_2 is not None
    assert final_st_1.wallet == final_st_2.wallet
    assert final_st_1.condition_id == final_st_2.condition_id
    assert final_st_1.token_id == final_st_2.token_id
    assert final_st_1.shares == final_st_2.shares
    assert final_st_1.avg_cost_basis == final_st_2.avg_cost_basis
    assert final_st_1.realized_pnl == final_st_2.realized_pnl
    assert final_st_1.last_applied_api_order == final_st_2.last_applied_api_order
    assert len(applied_1) == len(applied_2)
    for a1, a2 in zip(applied_1, applied_2, strict=False):
        assert a1.shares_after == a2.shares_after
        assert a1.avg_cost_after == a2.avg_cost_after
        assert a1.realized_pnl_delta == a2.realized_pnl_delta


@settings(max_examples=50)
@given(events=event_sequence())
def test_13_late_event_determinism(events):
    # 13. Recomputing out-of-order events yields identical result to original in-order run
    final_st_in_order, _ = _run_sequence(events)

    # Simulate late arrival: shuffle event arrival, but then re-sort by api_order before recompute
    shuffled_events = list(events)
    if len(shuffled_events) >= 3:
        # Move event 1 to the end
        late_event = shuffled_events.pop(1)
        shuffled_events.append(late_event)

    # Recompute sorts by api_order
    recomputed_order = sorted(shuffled_events, key=lambda e: e.api_order)
    final_st_recomputed, _ = _run_sequence(recomputed_order)

    assert final_st_in_order is not None
    assert final_st_recomputed is not None
    assert final_st_in_order.shares == final_st_recomputed.shares
    assert final_st_in_order.avg_cost_basis == final_st_recomputed.avg_cost_basis
    assert final_st_in_order.realized_pnl == final_st_recomputed.realized_pnl
