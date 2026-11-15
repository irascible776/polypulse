import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.ingestion.dedup import dedup_key, pair_enrichment
from app.storage.models import IngestionCursor, PositionLedger, SourceEvent, WalletTotal

logger = logging.getLogger("ingestion_writer")


async def supersede_trade_pairs(
    session: AsyncSession, wallet: str, key: str, monitor_events: list | None = None
) -> dict[int, int]:
    """Pair observations within one trade key, retaining already paired targets."""
    rows = (
        (
            await session.execute(
                select(SourceEvent)
                .where(
                    SourceEvent.wallet == wallet,
                    SourceEvent.event_type == "TRADE",
                    SourceEvent.dedup_key == key,
                )
                .order_by(SourceEvent.api_order)
            )
        )
        .scalars()
        .all()
    )
    pairs = pair_enrichment(
        [
            {
                "id": r.id,
                "api_order": r.api_order,
                "outcome_index": r.outcome_index,
                "raw": r.raw,
                "superseded_by": r.superseded_by,
            }
            for r in rows
        ]
    )
    if pairs:
        for row in rows:
            if row.id in pairs:
                row.superseded_by = pairs[row.id]
        projected = await session.scalar(
            select(func.count())
            .select_from(PositionLedger)
            .where(
                PositionLedger.wallet == wallet,
                PositionLedger.source_event_id.in_(
                    [r.source_event_id for r in rows if r.id in pairs]
                ),
            )
        )
        if projected:
            await session.execute(
                update(WalletTotal)
                .where(WalletTotal.wallet == wallet)
                .values(recompute_needed=True)
            )
        await session.flush()
        if monitor_events is not None:
            for row in rows:
                if row.id in pairs:
                    monitor_events.append(
                        {
                            "wallet": wallet,
                            "token_id": row.token_id,
                            "dedup_key": key,
                            "source_event_id": row.source_event_id,
                            "superseded_id": row.id,
                            "survivor_id": pairs[row.id],
                        }
                    )
        logger.info(
            json.dumps(
                {
                    "event": "supersede",
                    "wallet": wallet,
                    "count": len(pairs),
                    "transaction": "pending",
                }
            )
        )
    return pairs


async def get_or_create_cursor_locked(
    session: AsyncSession,
    wallet: str,
    source: str,
) -> IngestionCursor:
    """
    Acquire a per-wallet row lock on ingestion_cursors using SELECT ... FOR UPDATE.
    If the cursor does not exist, insert an initial row and lock it.
    """
    w = wallet.lower()
    stmt = (
        select(IngestionCursor)
        .where(IngestionCursor.wallet == w, IngestionCursor.source == source)
        .with_for_update()
    )
    res = await session.execute(stmt)
    cursor = res.scalar_one_or_none()

    if cursor is None:
        # Create initial cursor
        cursor = IngestionCursor(
            wallet=w,
            source=source,
            backfill_state="not_started",
            last_event_time=None,
            last_cursor=None,
            last_success_at=None,
            last_error=None,
            consecutive_failures=0,
            last_api_order=0,
        )
        session.add(cursor)
        await session.flush()
        # Re-select with lock
        res = await session.execute(stmt)
        cursor = res.scalar_one()

    return cursor


def prepare_source_event_record(
    raw: dict[str, Any],
    source_event_id: str,
    wallet: str,
    source: str,
    api_order: int,
    is_late: bool,
    observed_at: datetime,
) -> dict[str, Any]:
    """Transform a raw event dictionary into a dict suitable for source_events insert."""
    raw_type = str(raw.get("type") or "UNKNOWN").strip().upper()
    ts = int(raw.get("timestamp") or 0)
    event_time = datetime.fromtimestamp(ts, tz=timezone.utc)

    size = Decimal(str(raw.get("size") if raw.get("size") is not None else "0"))
    usdc_size = Decimal(str(raw.get("usdc_size") if raw.get("usdc_size") is not None else "0"))
    price_reported = Decimal(str(raw.get("price") if raw.get("price") is not None else "0"))

    effective_price = (usdc_size / size) if size > Decimal("0") else None

    # cash_price_gap flag: set when gap between size * price and usdc_size exceeds 0.01 for trades
    if raw_type == "TRADE":
        gap = abs((size * price_reported) - usdc_size)
        cash_price_gap = gap > Decimal("0.01")
    else:
        cash_price_gap = False

    outcome_raw = raw.get("outcome_index")
    outcome_index = None
    if outcome_raw is not None:
        try:
            outcome_index = int(outcome_raw)
        except (ValueError, TypeError):
            outcome_index = None

    cond_id = raw.get("condition_id")
    cond_id_str = str(cond_id).strip().lower() if cond_id else None

    tok_id = raw.get("token_id")
    tok_id_str = str(tok_id).strip() if tok_id else None

    side = raw.get("side")
    side_str = str(side).strip().upper() if side else None

    tx_hash = str(raw.get("transaction_hash") or "").strip().lower()

    return {
        "source": source,
        "source_event_id": source_event_id,
        "wallet": wallet.lower(),
        "event_type": raw_type,
        "event_time": event_time,
        "observed_at": observed_at,
        "transaction_hash": tx_hash,
        "condition_id": cond_id_str,
        "token_id": tok_id_str,
        "side": side_str,
        "outcome_index": outcome_index,
        "size": size,
        "usdc_size": usdc_size,
        "price_reported": price_reported,
        "effective_price": effective_price,
        "cash_price_gap": cash_price_gap,
        "api_order": api_order,
        "is_late": is_late,
        "raw": raw,
    }


async def write_event_batch(
    session: AsyncSession,
    wallet: str,
    source: str,
    events_with_ids: list[tuple[dict[str, Any], str]],
    next_cursor: str | None = None,
    is_backfill_complete: bool = False,
    now: datetime | None = None,
    on_new_event: Any | None = None,
    update_last_success: bool = True,
    monitor_events: list | None = None,
) -> tuple[int, int]:
    """
    Persist a batch of completed events with atomic per-wallet write locking.
    Uses INSERT ... ON CONFLICT (wallet, source_event_id) DO NOTHING RETURNING id.
    Updates ingestion_cursors in the SAME transaction.

    Assumes one writer per wallet; the wallet/order unique constraint makes
    concurrent cross-source allocation collisions fail loudly.

    Returns (inserted_count, duplicate_count).
    """
    w = wallet.lower()
    observed_now = now or datetime.now(timezone.utc)

    # 1. Acquire per-wallet row lock on ingestion_cursors
    cursor = await get_or_create_cursor_locked(session, w, source)

    if not events_with_ids:
        # No events to write, but update cursor state if needed
        if next_cursor is not None:
            cursor.last_cursor = next_cursor
        if is_backfill_complete:
            cursor.backfill_state = "complete"
        if update_last_success:
            cursor.last_success_at = observed_now
        cursor.consecutive_failures = 0
        cursor.last_error = None
        await session.flush()
        return 0, 0

    # 2. Check which source_event_ids already exist in source_events
    all_ids = [eid for _, eid in events_with_ids]
    existing_stmt = select(SourceEvent.source_event_id).where(
        SourceEvent.wallet == w,
        SourceEvent.source_event_id.in_(all_ids),
    )
    existing_res = await session.execute(existing_stmt)
    existing_set = set(existing_res.scalars().all())

    # 3. Assign monotonic api_order to new events and prepare insert rows
    max_order, wallet_latest_time = (
        await session.execute(
            select(
                func.coalesce(func.max(SourceEvent.api_order), 0), func.max(SourceEvent.event_time)
            ).where(SourceEvent.wallet == w)
        )
    ).one()
    current_order = max(cursor.last_api_order, max_order)
    last_known_event_time = max(
        (value for value in (cursor.last_event_time, wallet_latest_time) if value is not None),
        default=None,
    )
    max_event_time_in_batch = last_known_event_time

    insert_rows = []
    for raw_ev, eid in events_with_ids:
        # Determine if late
        ts = int(raw_ev.get("timestamp") or 0)
        ev_time = datetime.fromtimestamp(ts, tz=timezone.utc)
        is_late = bool(last_known_event_time is not None and ev_time < last_known_event_time)

        if max_event_time_in_batch is None or ev_time > max_event_time_in_batch:
            max_event_time_in_batch = ev_time

        if eid in existing_set:
            continue
        existing_set.add(eid)
        current_order += 1
        api_ord = current_order
        if on_new_event is not None:
            lag = max(0.0, (observed_now - ev_time).total_seconds())
            on_new_event(lag, is_late)

        row = prepare_source_event_record(
            raw=raw_ev,
            source_event_id=eid,
            wallet=w,
            source=source,
            api_order=api_ord,
            is_late=is_late,
            observed_at=observed_now,
        )
        row["dedup_key"] = dedup_key(row)
        insert_rows.append(row)

    # 4. Execute batch insert with ON CONFLICT DO NOTHING RETURNING id
    insert_stmt = (
        insert(SourceEvent)
        .values(insert_rows)
        .on_conflict_do_nothing(index_elements=["wallet", "source_event_id"])
        .returning(SourceEvent.source_event_id)
    )
    result = await session.execute(insert_stmt) if insert_rows else None
    inserted_ids = result.scalars().all() if result is not None else []
    inserted_count = len(inserted_ids)
    duplicate_count = len(events_with_ids) - inserted_count
    inserted_set = set(inserted_ids)
    for key in {
        row["dedup_key"]
        for row in insert_rows
        if row["event_type"] == "TRADE" and row["source_event_id"] in inserted_set
    }:
        await supersede_trade_pairs(session, w, key, monitor_events)
    if any(row["is_late"] and row["source_event_id"] in inserted_set for row in insert_rows):
        await session.execute(
            update(WalletTotal).where(WalletTotal.wallet == w).values(recompute_needed=True)
        )

    # 5. Update ingestion_cursors in the SAME transaction
    cursor.last_api_order = current_order
    if max_event_time_in_batch is not None:
        cursor.last_event_time = max_event_time_in_batch
    if next_cursor is not None:
        cursor.last_cursor = next_cursor
    if is_backfill_complete:
        cursor.backfill_state = "complete"
    else:
        if cursor.backfill_state == "not_started":
            cursor.backfill_state = "in_progress"

    if update_last_success:
        cursor.last_success_at = observed_now
    cursor.consecutive_failures = 0
    cursor.last_error = None

    await session.flush()
    return inserted_count, duplicate_count
