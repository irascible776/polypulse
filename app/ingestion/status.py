import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.serving.time_format import utc_timestamp
from app.storage.models import IngestionCursor, SourceEvent, TrackedWallet

logger = logging.getLogger("ingestion_status")

# Standard recognized Polymarket activity types
KNOWN_EVENT_TYPES = {
    "TRADE",
    "REDEEM",
    "MAKER_REBATE",
    "TAKER_REBATE",
    "REFERRAL_REWARD",
    "YIELD",
    "MERGE",
    "SPLIT",
    "CONVERSION",
    "MIGRATION",
    "REWARD",
    "DEPOSIT",
    "WITHDRAWAL",
    "DIRECTIONAL_SPLIT",
    "DIRECTIONAL_MERGE",
    "THRESHOLD_COMPOSE",
    "THRESHOLD_DECOMPOSE",
}


async def get_wallet_ingestion_status(
    session: AsyncSession,
    wallet: str,
    lag_p50_override: float | None = None,
    lag_p95_override: float | None = None,
) -> dict[str, Any]:
    """
    Retrieve comprehensive ingestion status for a single wallet.
    Returns:
    - backfill_state
    - last_success_at
    - events_stored
    - counts_per_event_type
    - unsupported_type_count
    - consecutive_failures
    - detection_lag_p50 (seconds)
    - detection_lag_p95 (seconds)
    """
    w = wallet.lower()

    # 1. Fetch cursor row
    cursor_stmt = select(IngestionCursor).where(
        IngestionCursor.wallet == w, IngestionCursor.source == "data_api"
    )
    cursor_res = await session.execute(cursor_stmt)
    cursor = cursor_res.scalars().first()

    backfill_state = cursor.backfill_state if cursor else "not_started"
    last_success_at = utc_timestamp(cursor.last_success_at) if cursor else None
    consecutive_failures = cursor.consecutive_failures if cursor else 0

    # 2. Aggregated event counts per type
    type_counts_stmt = (
        select(SourceEvent.event_type, func.count(SourceEvent.id))
        .where(SourceEvent.wallet == w)
        .group_by(SourceEvent.event_type)
    )
    type_res = await session.execute(type_counts_stmt)
    type_rows = type_res.all()

    counts_per_event_type: dict[str, int] = {}
    unsupported_type_count = 0
    total_events = 0

    for ev_type, count in type_rows:
        counts_per_event_type[ev_type] = count
        total_events += count
        if ev_type not in KNOWN_EVENT_TYPES:
            unsupported_type_count += count

    # 3. Detection lag percentiles over the last 1,000 events
    p50 = lag_p50_override
    p95 = lag_p95_override

    if p50 is None or p95 is None:
        # Compute from database over last 1,000 inserted events
        lag_subq = (
            select(
                func.extract("epoch", SourceEvent.observed_at - SourceEvent.event_time).label(
                    "lag_seconds"
                )
            )
            .where(SourceEvent.wallet == w)
            .order_by(SourceEvent.id.desc())
            .limit(1000)
            .subquery()
        )
        lag_stmt = select(lag_subq.c.lag_seconds)
        lag_res = await session.execute(lag_stmt)
        lags = sorted([max(0.0, float(v)) for v in lag_res.scalars().all() if v is not None])

        if lags:
            p50 = lags[len(lags) // 2]
            p95 = lags[min(int(len(lags) * 0.95), len(lags) - 1)]
        else:
            p50 = 0.0
            p95 = 0.0

    return {
        "wallet": w,
        "backfill_state": backfill_state,
        "last_success_at": last_success_at,
        **ingestion_freshness(cursor.last_success_at if cursor else None),
        "events_stored": total_events,
        "counts_per_event_type": counts_per_event_type,
        "unsupported_type_count": unsupported_type_count,
        "consecutive_failures": consecutive_failures,
        "detection_lag_p50": round(float(p50 or 0.0), 3),
        "detection_lag_p95": round(float(p95 or 0.0), 3),
    }


def ingestion_freshness(last_success_at: datetime | None) -> dict[str, Any]:
    """Use successful polling time, including when no events were produced."""
    if last_success_at is None:
        return {"data_as_of": None, "ingestion_lag_seconds": None, "ingestion_stale": True}
    lag = max(0.0, (datetime.now(timezone.utc) - last_success_at).total_seconds())
    return {
        "data_as_of": utc_timestamp(last_success_at),
        "ingestion_lag_seconds": lag,
        "ingestion_stale": lag > get_settings().ingestion_stale_seconds,
    }


async def get_all_ingestion_statuses(
    session: AsyncSession,
    lag_trackers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Retrieve ingestion status for all tracked wallets."""
    wallets_stmt = select(TrackedWallet.address)
    res = await session.execute(wallets_stmt)
    addresses = res.scalars().all()

    statuses = {}
    for addr in addresses:
        tracker = (lag_trackers or {}).get(addr.lower())
        lag_p50 = tracker.lag_p50 if tracker else None
        lag_p95 = tracker.lag_p95 if tracker else None
        statuses[addr.lower()] = await get_wallet_ingestion_status(
            session=session,
            wallet=addr,
            lag_p50_override=lag_p50,
            lag_p95_override=lag_p95,
        )

    return {
        "wallets": statuses,
        "total_tracked": len(statuses),
    }
