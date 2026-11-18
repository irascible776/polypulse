import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg.types.json import Jsonb
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.accounting.checkpoints import configure_transaction, mark_condition_dirty
from app.config import get_settings
from app.ingestion.dedup import dedup_key, pair_enrichment
from app.storage.models import IngestionCursor, ProjectorCursor, SourceEvent

logger = logging.getLogger("ingestion_writer")

INSERT_BATCH_SIZE = 500


async def _copy_insert_events(session: AsyncSession, rows: list[dict]) -> list[str]:
    columns = list(rows[0])
    column_sql = ", ".join(columns)
    await session.execute(
        text(
            f"CREATE TEMP TABLE IF NOT EXISTS source_event_stage ON COMMIT DROP AS "
            f"SELECT {column_sql} FROM source_events WHERE false"
        )
    )
    await session.execute(text("TRUNCATE source_event_stage"))
    connection = await session.connection()
    raw = await connection.get_raw_connection()
    async with raw.driver_connection.cursor() as cursor:
        async with cursor.copy(f"COPY source_event_stage ({column_sql}) FROM STDIN") as copy:
            for row in rows:
                await copy.write_row(
                    tuple(
                        Jsonb(row[column]) if column == "raw" else row[column] for column in columns
                    )
                )
    result = await session.execute(
        text(
            f"INSERT INTO source_events ({column_sql}) SELECT {column_sql} "
            "FROM source_event_stage ORDER BY api_order "
            "ON CONFLICT (wallet, source_event_id) DO NOTHING RETURNING source_event_id"
        )
    )
    return list(result.scalars())


async def supersede_trade_pairs(
    session: AsyncSession, wallet: str, key: str, monitor_events: list | None = None
) -> dict[int, int]:
    """Pair observations within one trade key, retaining already paired targets."""
    return await _supersede_trade_keys(session, wallet, {key}, monitor_events)


async def _supersede_trade_keys(
    session: AsyncSession, wallet: str, keys: set[str], monitor_events: list | None
) -> dict[int, int]:
    if not keys:
        return {}
    from app.accounting.projector import AccountingProjector

    total = await AccountingProjector().get_or_create_wallet_total(session, wallet)
    rows = (
        (
            await session.execute(
                select(SourceEvent)
                .where(
                    SourceEvent.wallet == wallet,
                    SourceEvent.event_type == "TRADE",
                    SourceEvent.dedup_key.in_(keys),
                )
                .order_by(SourceEvent.api_order)
                .execution_options(populate_existing=True)
            )
        )
        .scalars()
        .all()
    )
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row.dedup_key, []).append(
            {
                "id": row.id,
                "api_order": row.api_order,
                "outcome_index": row.outcome_index,
                "raw": row.raw,
                "superseded_by": row.superseded_by,
            }
        )
    pairs = {}
    for group in grouped.values():
        pairs.update(pair_enrichment(group))
    if pairs:
        for row in rows:
            if row.id in pairs:
                row.superseded_by = pairs[row.id]
        cursor = await session.get(ProjectorCursor, wallet, populate_existing=True)
        if cursor and any(
            row.id in pairs and row.api_order <= cursor.last_applied_api_order for row in rows
        ):
            affected_ids = set(pairs) | set(pairs.values())
            for row in rows:
                if row.id in affected_ids:
                    await mark_condition_dirty(
                        session, total, row.condition_id, (row.event_time, row.api_order)
                    )
        await session.flush()
        if monitor_events is not None:
            for row in rows:
                if row.id in pairs:
                    monitor_events.append(
                        {
                            "wallet": wallet,
                            "token_id": row.token_id,
                            "dedup_key": row.dedup_key,
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
    await configure_transaction(session)
    # Cursor rows are source-specific, but event order is shared across sources.
    # Serialize cursor creation and order allocation across the entire wallet.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:wallet, 1))"), {"wallet": w}
    )
    stmt = (
        select(IngestionCursor)
        .where(IngestionCursor.wallet == w, IngestionCursor.source == source)
        .with_for_update()
        .execution_options(populate_existing=True)
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

    Serializes all sources for a wallet for the duration of the transaction.

    Returns (inserted_count, duplicate_count).
    """
    w = wallet.lower()
    for raw_event, _ in events_with_ids:
        for field in ("proxy_wallet", "wallet"):
            claimed = raw_event.get(field)
            if claimed and str(claimed).strip().lower() != w:
                raise ValueError("Source event wallet does not match the target wallet")
    observed_now = now or datetime.now(timezone.utc)

    # 1. Acquire per-wallet row lock on ingestion_cursors
    cursor = await get_or_create_cursor_locked(session, w, source)
    # The projector and writer must agree on whether a correction has already
    # been applied before either transaction changes wallet accounting state.
    from app.accounting.projector import AccountingProjector

    wallet_total = await AccountingProjector().get_or_create_wallet_total(session, w)

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
    existing_set = set()
    for offset in range(0, len(all_ids), INSERT_BATCH_SIZE):
        existing_stmt = select(SourceEvent.source_event_id).where(
            SourceEvent.wallet == w,
            SourceEvent.source_event_id.in_(all_ids[offset : offset + INSERT_BATCH_SIZE]),
        )
        existing_res = await session.execute(existing_stmt)
        existing_set.update(existing_res.scalars().all())

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
        is_late = bool(max_event_time_in_batch is not None and ev_time < max_event_time_in_batch)

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
    threshold = get_settings().ingestion_copy_threshold
    if insert_rows and threshold and len(insert_rows) >= threshold:
        inserted_ids = await _copy_insert_events(session, insert_rows)
    else:
        inserted_ids = []
        for offset in range(0, len(insert_rows), INSERT_BATCH_SIZE):
            insert_stmt = (
                insert(SourceEvent)
                .values(insert_rows[offset : offset + INSERT_BATCH_SIZE])
                .on_conflict_do_nothing(index_elements=["wallet", "source_event_id"])
                .returning(SourceEvent.source_event_id)
            )
            result = await session.execute(insert_stmt)
            inserted_ids.extend(result.scalars().all())
    inserted_count = len(inserted_ids)
    duplicate_count = len(events_with_ids) - inserted_count
    inserted_set = set(inserted_ids)
    trade_keys = list(
        {
            row["dedup_key"]
            for row in insert_rows
            if row["event_type"] == "TRADE" and row["source_event_id"] in inserted_set
        }
    )
    for offset in range(0, len(trade_keys), INSERT_BATCH_SIZE):
        keys = trade_keys[offset : offset + INSERT_BATCH_SIZE]
        await _supersede_trade_keys(session, w, set(keys), monitor_events)
    dirty_conditions = {}
    for row in insert_rows:
        if row["is_late"] and row["source_event_id"] in inserted_set:
            condition = row["condition_id"]
            boundary = (row["event_time"], row["api_order"])
            dirty_conditions[condition] = min(dirty_conditions.get(condition, boundary), boundary)
    for condition, boundary in dirty_conditions.items():
        await mark_condition_dirty(session, wallet_total, condition, boundary)

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
