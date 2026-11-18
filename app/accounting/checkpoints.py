"""Correction metadata; callers hold the wallet total row lock until commit."""

import json

from sqlalchemy import delete, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert

from app.config import get_settings
from app.storage.models import AccountingCheckpoint, ConditionCheckpoint, ConditionReplayDirty


async def configure_transaction(session):
    settings = get_settings()
    await session.execute(
        text(
            "SELECT set_config('lock_timeout', :lock, true), "
            "set_config('statement_timeout', :statement, true)"
        ),
        {
            "lock": f"{settings.accounting_lock_timeout_seconds}s",
            "statement": f"{settings.accounting_statement_timeout_seconds}s",
        },
    )


async def notify_publication(session, wallet):
    # PostgreSQL delivers this only if the accounting transaction commits.
    await session.execute(
        text("SELECT pg_notify('polypulse_events', :payload)"),
        {"payload": json.dumps({"type": "pnl_update", "wallet": wallet, "ref": "projector"})},
    )


async def save_checkpoint(session, wallet, total, boundary, positions):
    records = [
        {
            "condition_id": state.condition_id,
            "token_id": state.token_id,
            "outcome_index": state.outcome_index,
            "shares": str(state.shares),
            "avg_cost_basis": str(state.avg_cost_basis),
            "realized_pnl": str(state.realized_pnl),
            "last_applied_api_order": state.last_applied_api_order,
            "updated_at": state.updated_at.isoformat(),
        }
        for state in positions
    ]
    session.add(
        AccountingCheckpoint(
            wallet=wallet,
            boundary_time=boundary[0],
            boundary_order=boundary[1],
            last_api_order=total.last_applied_api_order,
            positions=records,
            totals={
                "total_realized_pnl": str(total.total_realized_pnl),
                "total_trade_count": total.total_trade_count,
                "total_redeem_count": total.total_redeem_count,
                "unsupported_event_count": total.unsupported_event_count,
                "reject_count": total.reject_count,
            },
        )
    )
    grouped = {}
    for record in records:
        grouped.setdefault(record["condition_id"], []).append(record)
    for condition, state in grouped.items():
        session.add(
            ConditionCheckpoint(
                wallet=wallet,
                condition_id=condition,
                boundary_time=boundary[0],
                boundary_order=boundary[1],
                format_version=1,
                positions=state,
            )
        )
    await session.flush()
    cutoff = (
        await session.execute(
            select(AccountingCheckpoint.boundary_time, AccountingCheckpoint.boundary_order)
            .where(AccountingCheckpoint.wallet == wallet)
            .order_by(
                AccountingCheckpoint.boundary_time.desc(),
                AccountingCheckpoint.boundary_order.desc(),
            )
            .offset(get_settings().accounting_checkpoint_keep - 1)
            .limit(1)
        )
    ).one_or_none()
    if cutoff is not None:
        for model in (AccountingCheckpoint, ConditionCheckpoint):
            await session.execute(
                delete(model).where(
                    model.wallet == wallet,
                    tuple_(model.boundary_time, model.boundary_order) < tuple(cutoff),
                )
            )


async def mark_condition_dirty(session, total, condition_id, boundary):
    stmt = insert(ConditionReplayDirty).values(
        wallet=total.wallet,
        condition_id=condition_id or "",
        boundary_time=boundary[0],
        boundary_order=boundary[1],
    )
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=["wallet", "condition_id"],
            set_={
                "boundary_time": stmt.excluded.boundary_time,
                "boundary_order": stmt.excluded.boundary_order,
            },
            where=tuple_(stmt.excluded.boundary_time, stmt.excluded.boundary_order)
            < tuple_(ConditionReplayDirty.boundary_time, ConditionReplayDirty.boundary_order),
        )
    )
    if not total.recompute_needed:
        total.recompute_from_time, total.recompute_from_order = boundary
    elif total.recompute_from_time is not None and boundary < (
        total.recompute_from_time,
        total.recompute_from_order,
    ):
        total.recompute_from_time, total.recompute_from_order = boundary
    # A pre-existing unbounded correction must stay unbounded.
    total.recompute_needed = True
