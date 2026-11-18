import decimal
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from app.accounting.types import (
    AccountingEvent,
    ApplyReject,
    ApplyResult,
    ApplySuccess,
    ConditionEffect,
    PositionState,
)

# Enforce 28 significant digits precision across accounting calculations
decimal.getcontext().prec = 28

ZERO = Decimal("0")


def resolve_redeem_token(
    condition_id: str,
    outcome_index: int | None,
    positions: dict[tuple[str, str], PositionState],
) -> tuple[str | None, str | None]:
    """Resolve a tokenless redemption from the condition's open positions."""
    matching_tokens = [
        token_id
        for (position_condition, token_id), state in positions.items()
        if position_condition == condition_id
        and (outcome_index is None or state.outcome_index == outcome_index)
        and state.shares > ZERO
    ]
    if len(matching_tokens) == 1:
        return matching_tokens[0], None
    if matching_tokens:
        return None, "AMBIGUOUS_REDEEM_TOKEN"
    return None, "UNMATCHED_REDEEM_TOKEN"


def advance_position(
    prior: PositionState | None,
    event: AccountingEvent,
    result: ApplySuccess,
    updated_at: datetime,
) -> PositionState:
    """Build the next position state without database access."""
    prior_realized = prior.realized_pnl if prior else ZERO
    outcome_index = event.outcome_index
    if outcome_index is None and prior:
        outcome_index = prior.outcome_index
    return PositionState(
        wallet=event.wallet,
        condition_id=event.condition_id or "",
        token_id=event.token_id,
        outcome_index=outcome_index,
        shares=result.shares_after,
        avg_cost_basis=result.avg_cost_after,
        realized_pnl=prior_realized + result.realized_pnl_delta,
        last_applied_api_order=event.api_order,
        updated_at=updated_at,
    )


def compute_position_event(
    event: AccountingEvent,
    positions: dict[tuple[str, str], PositionState],
    updated_at: datetime,
) -> tuple[AccountingEvent, ApplyResult, PositionState | None]:
    """Compute one event from position state without accessing the database."""
    condition_id = event.condition_id or ""
    token_id = event.token_id
    if not token_id and event.event_type.upper() == "REDEEM":
        token_id, reason = resolve_redeem_token(condition_id, event.outcome_index, positions)
        if reason is not None:
            return (
                event,
                ApplyResult(
                    reject=ApplyReject(
                        wallet=event.wallet,
                        condition_id=event.condition_id,
                        token_id=event.token_id,
                        size=event.size,
                        usdc_size=event.usdc_size,
                        reject_reason=reason,
                    )
                ),
                None,
            )
    resolved = replace(event, condition_id=condition_id, token_id=token_id)
    prior = positions.get((condition_id, token_id))
    result = apply_event(prior, resolved)
    state = (
        advance_position(prior, resolved, result.success, updated_at) if result.success else None
    )
    return resolved, result, state


def compute_condition_events(
    wallet: str,
    condition_id: str,
    events: list[AccountingEvent],
    positions: dict[tuple[str, str], PositionState],
    updated_at: datetime,
) -> list[ConditionEffect]:
    """Compute ordered effects for one condition without database access."""
    if any(
        event.wallet != wallet or (event.condition_id or "") != condition_id for event in events
    ):
        raise ValueError("Condition work contains an event for another wallet or condition")
    if any(
        position.wallet != wallet
        or position.condition_id != condition_id
        or key != (position.condition_id, position.token_id)
        for key, position in positions.items()
    ):
        raise ValueError("Condition work contains a position for another wallet or condition")
    state = dict(positions)
    effects = []
    for event in events:
        resolved, result, position = compute_position_event(event, state, updated_at)
        if position is not None:
            state[(resolved.condition_id or "", resolved.token_id)] = position
        effects.append(ConditionEffect(resolved, result, position))
    return effects


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
