"""Read-only leaderboard, wallet analytics, and system metrics endpoints for Part 2."""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import case, desc, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.ingestion.status import ingestion_freshness
from app.serving.middleware import global_latency_tracker
from app.serving.time_format import utc_timestamp
from app.serving.websocket import ws_manager
from app.storage.database import (
    async_session_factory,
    engine,
    secondary_session_factory,
    validate_shard_identity,
    wallet_read_session,
)
from app.storage.models import (
    AccountingCheckpoint,
    IngestionCursor,
    PositionLedger,
    PositionLedgerReject,
    ReconcileRun,
    SourceEvent,
    TrackedWallet,
    WalletTotal,
)
from app.storage.sharding import wallet_bucket
from app.valuation.engine import ValuationEngine

EXCLUDED_FLOW_TYPES = ("MAKER_REBATE", "TAKER_REBATE", "REFERRAL_REWARD", "REWARD", "YIELD")

STATIC_MEASURED_BENCHMARKS: dict[str, Any] = {
    "is_static_benchmark": True,
    "not_live_traffic": True,
    "source_doc": "docs/phase6/measurements.md",
    "hardware": "Local Linux · 2 physical / 4 logical CPUs · 2 local PostgreSQL 16 shards",
    "reproduce_command": "make bench",
    "runs": [
        {
            "name": "1,000,000 Synthetic Events",
            "config": "100 wallets · 2 shards · 4 workers (200k hot wallet)",
            "result": "964.28 ev/s",
            "detail": "API p95 41.40 ms · 0 duplicate rows · elapsed 1,037.05s",
        },
        {
            "name": "PostgreSQL COPY Staging vs Chunked",
            "config": "10,000 buy events · 2,000-event batch",
            "result": "2.73s vs 10.80s",
            "detail": "4.0x faster ingestion",
        },
        {
            "name": "Checkpoint Replay vs Full Replay",
            "config": "5,001 events from checkpoint vs 200,001-event full replay",
            "result": "2.17s vs 72.76s",
            "detail": "33x faster recovery · exact ledger & state parity",
        },
    ],
}

_leaderboard_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_system_metrics_cache: tuple[float, dict[str, Any]] | None = None
_LEADERBOARD_TTL_SECONDS = 20.0
_METRICS_TTL_SECONDS = 15.0


def invalidate_analytics_cache() -> None:
    global _system_metrics_cache
    _leaderboard_cache.clear()
    _system_metrics_cache = None


def _window_cutoff(window: str, now: datetime) -> datetime | None:
    if window == "7d":
        return now - timedelta(days=7)
    if window == "30d":
        return now - timedelta(days=30)
    if window == "all":
        return None
    raise ValueError("window must be one of: 7d, 30d, all")


def _dec_str(val: Decimal | None, places: str | None = None) -> str | None:
    if val is None:
        return None
    if places is not None:
        return str(val.quantize(Decimal(places)))
    return str(val)


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[min(max(idx, 0), len(sorted_vals) - 1)]


async def _load_shard_leaderboard(
    factory: async_sessionmaker[AsyncSession],
    shard_id: int,
    validate_shard: bool,
    cutoff: datetime | None,
    valuation_engine: ValuationEngine,
) -> list[dict[str, Any]]:
    async with factory() as session:
        if validate_shard:
            await validate_shard_identity(session, shard_id)

        wallets = (
            (
                await session.execute(
                    select(TrackedWallet)
                    .where(TrackedWallet.status == "active")
                    .order_by(TrackedWallet.address)
                )
            )
            .scalars()
            .all()
        )
        if not wallets:
            return []
        addresses = [w.address for w in wallets]

        totals_rows = (
            (await session.execute(select(WalletTotal).where(WalletTotal.wallet.in_(addresses))))
            .scalars()
            .all()
        )
        totals_by_wallet = {row.wallet: row for row in totals_rows}

        cursors_rows = (
            (
                await session.execute(
                    select(IngestionCursor).where(
                        IngestionCursor.wallet.in_(addresses),
                        IngestionCursor.source == "data_api",
                    )
                )
            )
            .scalars()
            .all()
        )
        cursors_by_wallet = {row.wallet: row for row in cursors_rows}

        # Single grouped volume + last_active query per shard
        vol_conds = [
            SourceEvent.wallet.in_(addresses),
            SourceEvent.superseded_by.is_(None),
        ]
        if cutoff is not None:
            vol_conds.append(SourceEvent.event_time >= cutoff)
        vol_rows = (
            await session.execute(
                select(
                    SourceEvent.wallet,
                    func.coalesce(
                        func.sum(
                            case(
                                (SourceEvent.event_type == "TRADE", SourceEvent.usdc_size),
                                else_=Decimal("0"),
                            )
                        ),
                        Decimal("0"),
                    ).label("volume_usdc"),
                    func.max(SourceEvent.event_time).label("last_active"),
                )
                .where(*vol_conds)
                .group_by(SourceEvent.wallet)
            )
        ).all()
        vol_by_wallet = {row.wallet: (row.volume_usdc, row.last_active) for row in vol_rows}

        # Single grouped closing-trade win-rate query per shard
        led_conds = [
            PositionLedger.wallet.in_(addresses),
            PositionLedger.realized_pnl_delta != Decimal("0"),
        ]
        if cutoff is not None:
            led_conds.append(PositionLedger.event_time >= cutoff)
        wr_rows = (
            await session.execute(
                select(
                    PositionLedger.wallet,
                    func.count(PositionLedger.id).label("closed_count"),
                    func.sum(
                        case((PositionLedger.realized_pnl_delta > Decimal("0"), 1), else_=0)
                    ).label("win_count"),
                    func.coalesce(func.sum(PositionLedger.realized_pnl_delta), Decimal("0")).label(
                        "window_realized_pnl"
                    ),
                )
                .where(*led_conds)
                .group_by(PositionLedger.wallet)
            )
        ).all()
        wr_by_wallet = {
            row.wallet: (
                int(row.closed_count or 0),
                int(row.win_count or 0),
                row.window_realized_pnl,
            )
            for row in wr_rows
        }

        # Latest reconcile run per wallet via window function (SQLAlchemy 2.1 clean)
        ranked_rec = (
            select(
                ReconcileRun.id.label("rec_id"),
                func.row_number()
                .over(
                    partition_by=ReconcileRun.wallet,
                    order_by=(desc(ReconcileRun.finished_at), desc(ReconcileRun.id)),
                )
                .label("rn"),
            )
            .where(ReconcileRun.wallet.in_(addresses))
            .subquery()
        )
        rec_rows = (
            (
                await session.execute(
                    select(ReconcileRun)
                    .join(ranked_rec, ReconcileRun.id == ranked_rec.c.rec_id)
                    .where(ranked_rec.c.rn == 1)
                )
            )
            .scalars()
            .all()
        )
        rec_by_wallet = {row.wallet: row for row in rec_rows}

    entries: list[dict[str, Any]] = []
    for w in wallets:
        addr = w.address
        async with wallet_read_session(addr) as w_sess:
            summary = await valuation_engine.value_wallet(w_sess, addr)
        tot = totals_by_wallet.get(addr)
        cursor = cursors_by_wallet.get(addr)
        freshness = ingestion_freshness(cursor.last_success_at if cursor else None)
        volume_usdc, last_active = vol_by_wallet.get(addr, (Decimal("0"), None))
        closed_count, win_count, window_realized = wr_by_wallet.get(addr, (0, 0, Decimal("0")))
        win_rate_pct = (
            (Decimal(win_count) / Decimal(closed_count) * Decimal("100")).quantize(Decimal("0.01"))
            if closed_count > 0
            else None
        )
        rec = rec_by_wallet.get(addr)
        reconcile_info = (
            {
                "match_count": rec.match_count,
                "mismatch_count": rec.mismatch_count,
                "in_flight_count": rec.in_flight_count,
                "unknown_count": rec.unknown_count,
                "reference_block": rec.reference_block,
                "finished_at": utc_timestamp(rec.finished_at),
            }
            if rec
            else None
        )
        summary_dict = summary.to_dict()
        entries.append(
            {
                "address": addr,
                "label": w.label,
                "status": w.status,
                "bucket_id": wallet_bucket(addr),
                "shard_id": 0 if wallet_bucket(addr) < 8 else 1,
                "tracking_start": utc_timestamp(w.tracking_start),
                "last_active": utc_timestamp(last_active),
                "realized_pnl": summary_dict["realized_pnl"],
                "window_realized_pnl": str(window_realized),
                "unrealized_priced": summary_dict["unrealized_priced"],
                "estimated_unrealized": summary_dict["estimated_unrealized"],
                "unpriced_cost_basis": summary_dict["unpriced_cost_basis"],
                "total_pnl_excl_unpriced": summary_dict["total_pnl_excl_unpriced"],
                "complete": summary_dict["complete"],
                "total_cost_basis": summary_dict["total_cost_basis"],
                "total_market_value": summary_dict["total_market_value"],
                "open_positions_count": summary_dict["open_positions_count"],
                "unpriced_count": summary_dict["unpriced_count"],
                "stale_positions_count": summary_dict["stale_positions_count"],
                "trade_count": tot.total_trade_count if tot else 0,
                "redeem_count": tot.total_redeem_count if tot else 0,
                "volume_usdc": str(volume_usdc),
                "closed_count": closed_count,
                "win_count": win_count,
                "win_rate_pct": str(win_rate_pct) if win_rate_pct is not None else None,
                "reconcile": reconcile_info,
                "ingestion_stale": freshness["ingestion_stale"],
                "data_as_of": freshness["data_as_of"],
                "valuation_current": summary_dict["complete"] and not freshness["ingestion_stale"],
                "_sort_pnl": summary.total_pnl
                if summary_dict["complete"]
                else summary.realized_pnl,
            }
        )
    return entries


async def get_wallets_leaderboard(
    valuation_engine: ValuationEngine,
    window: str = "all",
    min_volume: Decimal = Decimal("0"),
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = _window_cutoff(window, now)
    cache_key = (window, str(min_volume))
    now_mono = time.monotonic()
    cached = _leaderboard_cache.get(cache_key)
    if cached is not None and (now_mono - cached[0]) < _LEADERBOARD_TTL_SECONDS:
        return cached[1]

    sharded = secondary_session_factory is not None
    tasks = [_load_shard_leaderboard(async_session_factory, 0, sharded, cutoff, valuation_engine)]
    if secondary_session_factory is not None:
        tasks.append(
            _load_shard_leaderboard(secondary_session_factory, 1, True, cutoff, valuation_engine)
        )
    results = await asyncio.gather(*tasks, return_exceptions=True)

    wallets: list[dict[str, Any]] = []
    failed_shards: list[int] = []
    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            failed_shards.append(idx)
            continue
        wallets.extend(res)

    if min_volume > 0:
        wallets = [w for w in wallets if Decimal(w["volume_usdc"]) >= min_volume]

    wallets.sort(key=lambda w: w["_sort_pnl"], reverse=True)
    for rank, w in enumerate(wallets, start=1):
        w.pop("_sort_pnl", None)
        w["rank"] = rank

    payload = {
        "window": window,
        "min_volume": str(min_volume),
        "as_of": utc_timestamp(now),
        "instance_id": get_settings().resolve_instance_id(),
        "partial": bool(failed_shards),
        "failed_shards": failed_shards,
        "count": len(wallets),
        "wallets": wallets,
    }
    _leaderboard_cache[cache_key] = (now_mono, payload)
    return payload


def _downsample_equity(points: list[dict[str, Any]], max_points: int = 180) -> list[dict[str, Any]]:
    if len(points) <= max_points:
        return points
    keep_indices: set[int] = {0, len(points) - 1}
    # Always preserve peak equity and maximum drawdown trough
    peak_idx = max(range(len(points)), key=lambda i: Decimal(points[i]["cumulative_realized_pnl"]))
    trough_idx = min(range(len(points)), key=lambda i: Decimal(points[i]["drawdown"]))
    keep_indices.update({peak_idx, trough_idx})
    step = (len(points) - 1) / (max_points - 1)
    for k in range(max_points):
        keep_indices.add(round(k * step))
    return [points[i] for i in sorted(keep_indices)]


async def get_wallet_analytics(
    session: AsyncSession,
    wallet: str,
    window: str = "all",
) -> dict[str, Any]:
    clean_wallet = wallet.lower()
    now = datetime.now(timezone.utc)
    cutoff = _window_cutoff(window, now)

    # 1. Ledger rows for equity curve, drawdown, and entry-price buckets
    led_stmt = (
        select(
            PositionLedger.event_time,
            PositionLedger.api_order,
            PositionLedger.shares_delta,
            PositionLedger.avg_cost_before,
            PositionLedger.realized_pnl_delta,
        )
        .where(PositionLedger.wallet == clean_wallet)
        .order_by(PositionLedger.event_time.asc(), PositionLedger.api_order.asc())
    )
    if cutoff is not None:
        led_stmt = led_stmt.where(PositionLedger.event_time >= cutoff)
    led_rows = (await session.execute(led_stmt)).all()

    cum_pnl = Decimal("0")
    peak_pnl = Decimal("0")
    peak_time: datetime | None = None
    max_dd = Decimal("0")
    max_dd_pct: Decimal | None = None
    max_dd_duration_seconds: float = 0.0
    equity_points: list[dict[str, Any]] = []

    bucket_specs = [
        ("<0.20", Decimal("0"), Decimal("0.20")),
        ("0.20-0.50", Decimal("0.20"), Decimal("0.50")),
        ("0.50-0.80", Decimal("0.50"), Decimal("0.80")),
        (">0.80", Decimal("0.80"), Decimal("1.01")),
    ]
    bucket_acc: dict[str, dict[str, Any]] = {
        name: {
            "count": 0,
            "wins": 0,
            "realized_pnl": Decimal("0"),
            "roi_sum": Decimal("0"),
            "roi_count": 0,
        }
        for name, _, _ in bucket_specs
    }

    for event_time, _api_order, shares_delta, avg_cost_before, pnl_delta in led_rows:
        if pnl_delta != Decimal("0"):
            cum_pnl += pnl_delta
            if cum_pnl > peak_pnl:
                peak_pnl = cum_pnl
                peak_time = event_time
            dd = cum_pnl - peak_pnl
            if dd < max_dd:
                max_dd = dd
                if peak_pnl > 0:
                    max_dd_pct = ((dd / peak_pnl) * Decimal("100")).quantize(Decimal("0.01"))
                if peak_time is not None:
                    max_dd_duration_seconds = max(
                        max_dd_duration_seconds,
                        max(0.0, (event_time - peak_time).total_seconds()),
                    )
            equity_points.append(
                {
                    "time": utc_timestamp(event_time),
                    "cumulative_realized_pnl": str(cum_pnl),
                    "drawdown": str(dd),
                }
            )

            # Classify entry price bucket from exact avg_cost_before
            cost_price = avg_cost_before or Decimal("0")
            for name, lo, hi in bucket_specs:
                if (name == ">0.80" and cost_price >= lo) or (lo <= cost_price < hi):
                    slot = bucket_acc[name]
                    slot["count"] += 1
                    if pnl_delta > 0:
                        slot["wins"] += 1
                    slot["realized_pnl"] += pnl_delta
                    cost_closed = abs(shares_delta) * cost_price
                    if cost_closed > 0:
                        slot["roi_sum"] += (pnl_delta / cost_closed) * Decimal("100")
                        slot["roi_count"] += 1
                    break

    entry_price_buckets = []
    for name, _, _ in bucket_specs:
        slot = bucket_acc[name]
        n = slot["count"]
        win_rate = (
            (Decimal(slot["wins"]) / Decimal(n) * Decimal("100")).quantize(Decimal("0.01"))
            if n > 0
            else None
        )
        avg_pnl = (slot["realized_pnl"] / Decimal(n)).quantize(Decimal("0.01")) if n > 0 else None
        avg_roi = (
            (slot["roi_sum"] / Decimal(slot["roi_count"])).quantize(Decimal("0.01"))
            if slot["roi_count"] > 0
            else None
        )
        entry_price_buckets.append(
            {
                "bucket": name,
                "count": n,
                "low_sample": n < 10,
                "win_rate_pct": str(win_rate) if win_rate is not None else None,
                "realized_pnl": str(slot["realized_pnl"]),
                "avg_pnl": str(avg_pnl) if avg_pnl is not None else None,
                "avg_roi_pct": str(avg_roi) if avg_roi is not None else None,
            }
        )

    # 2. Volume & Excluded Non-Trade Flows (single GROUP BY query)
    se_conds = [SourceEvent.wallet == clean_wallet, SourceEvent.superseded_by.is_(None)]
    if cutoff is not None:
        se_conds.append(SourceEvent.event_time >= cutoff)
    flow_rows = (
        await session.execute(
            select(
                SourceEvent.event_type,
                func.count(SourceEvent.id).label("rows"),
                func.coalesce(func.sum(SourceEvent.usdc_size), Decimal("0")).label("usdc_total"),
            )
            .where(*se_conds)
            .group_by(SourceEvent.event_type)
        )
    ).all()

    event_counts: dict[str, int] = {}
    trade_volume_usdc = Decimal("0")
    excluded_flows: list[dict[str, Any]] = []
    excluded_total_usdc = Decimal("0")
    for ev_type, count, usdc_total in flow_rows:
        event_counts[ev_type] = int(count)
        if ev_type == "TRADE":
            trade_volume_usdc = usdc_total
        elif ev_type in EXCLUDED_FLOW_TYPES:
            excluded_total_usdc += usdc_total
            excluded_flows.append(
                {
                    "event_type": ev_type,
                    "rows": int(count),
                    "usdc_size": str(usdc_total),
                }
            )

    # 3. Accounting Audit + Rejects + Reconcile Drill-Down
    wt = await session.get(WalletTotal, clean_wallet)
    checkpoint_count = (
        await session.scalar(
            select(func.count())
            .select_from(AccountingCheckpoint)
            .where(AccountingCheckpoint.wallet == clean_wallet)
        )
        or 0
    )
    late_count = (
        await session.scalar(
            select(func.count())
            .select_from(SourceEvent)
            .where(SourceEvent.wallet == clean_wallet, SourceEvent.is_late.is_(True))
        )
        or 0
    )
    superseded_count = (
        await session.scalar(
            select(func.count())
            .select_from(SourceEvent)
            .where(SourceEvent.wallet == clean_wallet, SourceEvent.superseded_by.is_not(None))
        )
        or 0
    )
    latest_order = await session.scalar(
        select(func.max(SourceEvent.api_order)).where(
            SourceEvent.wallet == clean_wallet, SourceEvent.superseded_by.is_(None)
        )
    )
    reject_rows = (
        (
            await session.execute(
                select(PositionLedgerReject)
                .where(PositionLedgerReject.wallet == clean_wallet)
                .order_by(desc(PositionLedgerReject.id))
                .limit(25)
            )
        )
        .scalars()
        .all()
    )
    latest_reconcile = await session.scalar(
        select(ReconcileRun)
        .where(ReconcileRun.wallet == clean_wallet)
        .order_by(desc(ReconcileRun.finished_at), desc(ReconcileRun.id))
        .limit(1)
    )

    applied_order = wt.last_applied_api_order if wt else 0
    accounting_current = not (wt and wt.recompute_needed) and (
        latest_order is None or latest_order <= applied_order
    )

    return {
        "wallet": clean_wallet,
        "window": window,
        "as_of": utc_timestamp(now),
        "bucket_id": wallet_bucket(clean_wallet),
        "shard_id": 0 if wallet_bucket(clean_wallet) < 8 else 1,
        "equity_curve": _downsample_equity(equity_points),
        "drawdown_summary": {
            "max_drawdown": str(max_dd),
            "max_drawdown_pct": str(max_dd_pct) if max_dd_pct is not None else None,
            "max_drawdown_duration_seconds": round(max_dd_duration_seconds, 1),
            "points_total": len(equity_points),
        },
        "entry_price_buckets": entry_price_buckets,
        "activity": {
            "trade_volume_usdc": str(trade_volume_usdc),
            "event_counts": event_counts,
            "excluded_flows": {
                "excluded_from_pnl": True,
                "total_usdc_size": str(excluded_total_usdc),
                "by_type": sorted(excluded_flows, key=lambda x: x["event_type"]),
                "note": (
                    "Stored in source_events but excluded from trade PnL "
                    "(no upstream cash-direction proof)."
                ),
            },
        },
        "accounting_audit": {
            "publication_revision": wt.publication_revision if wt else 0,
            "accounting_current": accounting_current,
            "last_applied_api_order": applied_order,
            "recompute_count": wt.recompute_count if wt else 0,
            "last_recompute_at": utc_timestamp(wt.last_recompute_at) if wt else None,
            "recompute_needed": bool(wt.recompute_needed) if wt else False,
            "checkpoint_count": int(checkpoint_count),
            "late_event_count": int(late_count),
            "superseded_event_count": int(superseded_count),
            "unsupported_event_count": wt.unsupported_event_count if wt else 0,
            "reject_count": wt.reject_count if wt else 0,
            "arithmetic_guarantee": "Exact PostgreSQL NUMERIC / Python Decimal (1e-12 USDC tol)",
            "recent_rejects": [
                {
                    "id": r.id,
                    "event_type": r.event_type,
                    "event_time": utc_timestamp(r.event_time),
                    "condition_id": r.condition_id,
                    "token_id": r.token_id,
                    "size": str(r.size) if r.size is not None else None,
                    "usdc_size": str(r.usdc_size) if r.usdc_size is not None else None,
                    "reject_reason": r.reject_reason,
                }
                for r in reject_rows
            ],
        },
        "reconcile": (
            {
                "confirmations_buffer": 32,
                "reference_block": latest_reconcile.reference_block,
                "reference_time": utc_timestamp(latest_reconcile.reference_time),
                "finished_at": utc_timestamp(latest_reconcile.finished_at),
                "match_count": latest_reconcile.match_count,
                "mismatch_count": latest_reconcile.mismatch_count,
                "in_flight_count": latest_reconcile.in_flight_count,
                "unknown_count": latest_reconcile.unknown_count,
                "error": latest_reconcile.error,
                "details": latest_reconcile.details,
            }
            if latest_reconcile
            else None
        ),
    }


async def _load_shard_metrics(
    factory: async_sessionmaker[AsyncSession], shard_id: int, validate_shard: bool
) -> dict[str, Any]:
    async with factory() as session:
        if validate_shard:
            await validate_shard_identity(session, shard_id)

        # Rolling ingestion counts using ix_source_events_observed_at index
        counts_row = (
            await session.execute(
                text("""
                SELECT
                    COUNT(*) FILTER (WHERE observed_at >= NOW() - INTERVAL '60 seconds') AS c60,
                    COUNT(*) FILTER (WHERE observed_at >= NOW() - INTERVAL '5 minutes') AS c5m,
                    COUNT(*) AS c1h
                FROM source_events
                WHERE observed_at >= NOW() - INTERVAL '1 hour'
                """)
            )
        ).one()

        # Detection lag over most recent 1,000 events via observed_at index
        lag_rows = (
            (
                await session.execute(
                    select(
                        func.extract(
                            "epoch", SourceEvent.observed_at - SourceEvent.event_time
                        ).label("lag")
                    )
                    .order_by(desc(SourceEvent.observed_at))
                    .limit(1000)
                )
            )
            .scalars()
            .all()
        )
        lags = [max(0.0, float(v)) for v in lag_rows if v is not None]

        # Fast catalog stats for non-partitioned tables (zero table scans)
        rel_rows = (
            await session.execute(
                text("""
                SELECT
                    c.relname,
                    GREATEST(c.reltuples::bigint, 0) AS est_rows,
                    pg_total_relation_size(c.oid) AS total_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname IN (
                      'source_events', 'position_ledger', 'chain_events',
                      'market_price_hourly'
                  )
                """)
            )
        ).all()
        tables = {
            name: {"estimated_rows": int(est_rows), "total_bytes": int(total_bytes)}
            for name, est_rows, total_bytes in rel_rows
        }

        # For partitioned tables SUM child-partition reltuples (parent always = 0)
        part_counts: dict[str, int] = {}
        for parent in ("market_prices", "market_price_archive"):
            child_row = await session.execute(
                text("""
                SELECT
                    GREATEST(SUM(c.reltuples)::bigint, 0) AS est_rows,
                    COALESCE(SUM(pg_total_relation_size(c.oid)), 0) AS total_bytes
                FROM pg_inherits i
                JOIN pg_class c ON c.oid = i.inhrelid
                JOIN pg_class p ON p.oid = i.inhparent
                JOIN pg_namespace n ON n.oid = p.relnamespace
                WHERE n.nspname = 'public' AND p.relname = :parent
                """),
                {"parent": parent},
            )
            crow = child_row.one()
            tables[parent] = {
                "estimated_rows": int(crow.est_rows or 0),
                "total_bytes": int(crow.total_bytes or 0),
            }
            if shard_id == 0:
                # Count child partitions for the partition-count metric
                p_count = await session.scalar(
                    text("""
                    SELECT COUNT(*)
                    FROM pg_inherits i
                    JOIN pg_class p ON p.oid = i.inhparent
                    JOIN pg_namespace n ON n.oid = p.relnamespace
                    WHERE n.nspname = 'public' AND p.relname = :parent
                    """),
                    {"parent": parent},
                )
                part_counts[parent] = int(p_count or 0)

        return {
            "shard_id": shard_id,
            "counts": {
                "60s": int(counts_row.c60 or 0),
                "5m": int(counts_row.c5m or 0),
                "1h": int(counts_row.c1h or 0),
            },
            "lags": lags,
            "tables": tables,
            "partitions": part_counts,
        }


async def get_system_metrics() -> dict[str, Any]:
    global _system_metrics_cache
    now_mono = time.monotonic()
    if (
        _system_metrics_cache is not None
        and (now_mono - _system_metrics_cache[0]) < _METRICS_TTL_SECONDS
    ):
        return _system_metrics_cache[1]

    sharded = secondary_session_factory is not None
    tasks = [_load_shard_metrics(async_session_factory, 0, sharded)]
    if secondary_session_factory is not None:
        tasks.append(_load_shard_metrics(secondary_session_factory, 1, True))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    c60 = c5m = c1h = 0
    all_lags: list[float] = []
    shard_storage: dict[str, Any] = {}
    partitions: dict[str, int] = {}
    failed_shards: list[int] = []

    for idx, res in enumerate(results):
        if isinstance(res, Exception):
            failed_shards.append(idx)
            continue
        c60 += res["counts"]["60s"]
        c5m += res["counts"]["5m"]
        c1h += res["counts"]["1h"]
        all_lags.extend(res["lags"])
        shard_storage[str(idx)] = res["tables"]
        if res["partitions"]:
            partitions.update(res["partitions"])

    all_lags.sort()
    pool = engine.pool
    settings = get_settings()

    payload = {
        "as_of": utc_timestamp(datetime.now(timezone.utc)),
        "instance_id": settings.resolve_instance_id(),
        "partial": bool(failed_shards),
        "failed_shards": failed_shards,
        "ingestion_throughput": {
            "events_60s": c60,
            "events_5m": c5m,
            "events_1h": c1h,
            "events_per_sec_60s": round(c60 / 60.0, 3),
            "events_per_sec_5m": round(c5m / 300.0, 3),
            "events_per_sec_1h": round(c1h / 3600.0, 3),
        },
        "detection_lag_seconds": {
            "p50": round(_percentile(all_lags, 0.50), 3),
            "p95": round(_percentile(all_lags, 0.95), 3),
            "p99": round(_percentile(all_lags, 0.99), 3),
            "sample_count": len(all_lags),
        },
        "api_latency_ms": global_latency_tracker.get_stats(),
        "websocket_stats": ws_manager.get_stats(),
        "db_pool": {
            "size": pool.size(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "configured_pool_size": settings.db_pool_size,
            "configured_max_overflow": settings.db_max_overflow,
        },
        "storage": {
            "shards": shard_storage,
            "partitions": partitions,
            "price_raw_retention_days": settings.price_raw_retention_days,
        },
        "static_benchmarks": STATIC_MEASURED_BENCHMARKS,
    }
    _system_metrics_cache = (now_mono, payload)
    return payload
