import decimal
from decimal import Decimal

from app.accounting.types import (
    AccountingEvent,
    ApplyReject,
    ApplyResult,
    ApplySuccess,
    PositionState,
)

# Enforce 28 significant digits precision across accounting calculations
decimal.getcontext().prec = 28

ZERO = Decimal("0")


def apply_event(
    state: PositionState | None,
    event: AccountingEvent,
) -> ApplyResult:
    """
    Pure state transition function: apply_event(prior_state, event) -> ApplyResult.

    Given the current position state for a token (or None if no prior position exists),
    and an incoming accounting event, computes the new shares, average cost basis,
    cash movement, and realized PnL delta according to Phase 3 accounting rules.

    Pure function: no I/O, no DB access, no global side-effects.
    """
    event_type = event.event_type.upper()

    # Non-mutating event types per Decision Note 002
    if event_type not in ("TRADE", "REDEEM"):
        return ApplyResult(is_unsupported=True)

    # Base position metrics before applying event
    wallet = event.wallet
    condition_id = event.condition_id or (state.condition_id if state else "")
    token_id = event.token_id or (state.token_id if state else "")

    shares_before = state.shares if state is not None else ZERO
    avg_cost_before = state.avg_cost_basis if state is not None else ZERO

    size = event.size
    usdc_size = event.usdc_size

    if event_type == "TRADE":
        side = (event.side or "").upper()

        if side == "BUY":
            shares_delta = size
            shares_after = shares_before + size

            if shares_after == ZERO:
                avg_cost_after = ZERO
            elif shares_before == ZERO:
                avg_cost_after = usdc_size / size if size > ZERO else ZERO
            else:
                total_cost = (avg_cost_before * shares_before) + usdc_size
                avg_cost_after = total_cost / shares_after

            cash_delta = -usdc_size
            realized_pnl_delta = ZERO

            return ApplyResult(
                success=ApplySuccess(
                    wallet=wallet,
                    condition_id=condition_id,
                    token_id=token_id,
                    shares_before=shares_before,
                    shares_delta=shares_delta,
                    shares_after=shares_after,
                    avg_cost_before=avg_cost_before,
                    avg_cost_after=avg_cost_after,
                    cash_delta=cash_delta,
                    realized_pnl_delta=realized_pnl_delta,
                    cash_price_gap=event.cash_price_gap,
                    is_mutating=True,
                )
            )

        elif side == "SELL":
            if size > shares_before:
                return ApplyResult(
                    reject=ApplyReject(
                        wallet=wallet,
                        condition_id=condition_id,
                        token_id=token_id,
                        size=size,
                        usdc_size=usdc_size,
                        reject_reason="SELLING_MORE_THAN_HELD",
                    )
                )

            shares_delta = -size
            shares_after = shares_before - size

            # Remaining shares keep their average; a closed position resets to zero.
            avg_cost_after = avg_cost_before if shares_after != ZERO else ZERO

            cost_of_shares_sold = size * avg_cost_before
            realized_pnl_delta = usdc_size - cost_of_shares_sold
            cash_delta = usdc_size

            return ApplyResult(
                success=ApplySuccess(
                    wallet=wallet,
                    condition_id=condition_id,
                    token_id=token_id,
                    shares_before=shares_before,
                    shares_delta=shares_delta,
                    shares_after=shares_after,
                    avg_cost_before=avg_cost_before,
                    avg_cost_after=avg_cost_after,
                    cash_delta=cash_delta,
                    realized_pnl_delta=realized_pnl_delta,
                    cash_price_gap=event.cash_price_gap,
                    is_mutating=True,
                )
            )

        else:
            return ApplyResult(
                reject=ApplyReject(
                    wallet=wallet,
                    condition_id=condition_id,
                    token_id=token_id,
                    size=size,
                    usdc_size=usdc_size,
                    reject_reason=f"UNKNOWN_TRADE_SIDE_{side}",
                )
            )

    elif event_type == "REDEEM":
        # A REDEEM closes out all held shares for this token
        if shares_before == ZERO:
            return ApplyResult(
                reject=ApplyReject(
                    wallet=wallet,
                    condition_id=condition_id,
                    token_id=token_id,
                    size=size,
                    usdc_size=usdc_size,
                    reject_reason="REDEEM_WITH_ZERO_SHARES",
                )
            )

        shares_delta = -shares_before
        shares_after = ZERO

        # Realized PnL is cash received minus total remaining cost basis
        cost_basis_total = shares_before * avg_cost_before
        realized_pnl_delta = usdc_size - cost_basis_total
        avg_cost_after = ZERO
        cash_delta = usdc_size

        return ApplyResult(
            success=ApplySuccess(
                wallet=wallet,
                condition_id=condition_id,
                token_id=token_id,
                shares_before=shares_before,
                shares_delta=shares_delta,
                shares_after=shares_after,
                avg_cost_before=avg_cost_before,
                avg_cost_after=avg_cost_after,
                cash_delta=cash_delta,
                realized_pnl_delta=realized_pnl_delta,
                cash_price_gap=event.cash_price_gap,
                is_mutating=True,
            )
        )

    return ApplyResult(is_unsupported=True)
