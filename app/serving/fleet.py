"""Shared fleet heartbeat persistence and 16-bucket ownership inspection."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.serving.time_format import utc_timestamp
from app.storage.database import (
    async_session_factory,
    secondary_session_factory,
    validate_shard_identity,
)
from app.storage.models import ServiceHeartbeat, TrackedWallet
from app.storage.sharding import WALLET_BUCKETS, wallet_bucket

logger = logging.getLogger(__name__)


def _to_decimal(val: float | Decimal | None) -> Decimal | None:
    if val is None:
        return None
    return Decimal(str(round(float(val), 3)))


async def upsert_heartbeat(
    *,
    instance_id: str,
    role: str,
    started_at: datetime,
    shard_id: int | None = None,
    ws_connections: int = 0,
    listener_ok: bool | None = None,
    buckets_held: list[int] | None = None,
    latency_p50_ms: float | None = None,
    latency_p95_ms: float | None = None,
    latency_p99_ms: float | None = None,
    meta: dict[str, Any] | None = None,
    session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
) -> bool:
    """Best-effort isolated heartbeat upsert on Shard 0 using DB NOW()."""
    try:
        async with session_factory() as session:
            async with session.begin():
                await session.execute(text("SET LOCAL statement_timeout = '3000'"))
                stmt = (
                    insert(ServiceHeartbeat)
                    .values(
                        instance_id=instance_id,
                        role=role,
                        shard_id=shard_id,
                        started_at=started_at,
                        last_seen_at=func.now(),
                        ws_connections=ws_connections,
                        listener_ok=listener_ok,
                        buckets_held=sorted(set(buckets_held or [])),
                        latency_p50_ms=_to_decimal(latency_p50_ms),
                        latency_p95_ms=_to_decimal(latency_p95_ms),
                        latency_p99_ms=_to_decimal(latency_p99_ms),
                        meta=meta or {},
                    )
                    .on_conflict_do_update(
                        index_elements=["instance_id"],
                        set_={
                            "role": role,
                            "shard_id": shard_id,
                            "last_seen_at": func.now(),
                            "ws_connections": ws_connections,
                            "listener_ok": listener_ok,
                            "buckets_held": sorted(set(buckets_held or [])),
                            "latency_p50_ms": _to_decimal(latency_p50_ms),
                            "latency_p95_ms": _to_decimal(latency_p95_ms),
                            "latency_p99_ms": _to_decimal(latency_p99_ms),
                            "meta": meta or {},
                        },
                    )
                )
                await session.execute(stmt)
        return True
    except Exception as exc:
        logger.warning(
            "Heartbeat upsert failed for %s: %s: %s", instance_id, type(exc).__name__, exc
        )
        return False


async def delete_heartbeat(
    instance_id: str,
    session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
) -> None:
    """Best-effort cleanup of this instance's heartbeat row on graceful shutdown."""
    try:
        async with session_factory() as session:
            async with session.begin():
                await session.execute(text("SET LOCAL statement_timeout = '3000'"))
                await session.execute(
                    delete(ServiceHeartbeat).where(ServiceHeartbeat.instance_id == instance_id)
                )
    except Exception as exc:
        logger.warning(
            "Heartbeat delete failed for %s: %s: %s", instance_id, type(exc).__name__, exc
        )


async def _fetch_shard_wallets(
    factory: async_sessionmaker[AsyncSession], expected_shard: int | None
) -> list[dict[str, Any]]:
    async with factory() as session:
        if expected_shard is not None:
            await validate_shard_identity(session, expected_shard)
        rows = (
            await session.execute(
                select(TrackedWallet.address, TrackedWallet.label)
                .where(TrackedWallet.status == "active")
                .order_by(TrackedWallet.address)
            )
        ).all()
        return [{"address": addr, "label": label} for addr, label in rows]


async def get_fleet_status(
    session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
) -> dict[str, Any]:
    """Return active/stale instances and 16-bucket ownership map grounded in DB NOW()."""
    settings = get_settings()
    stale_after = max(settings.fleet_heartbeat_seconds * 3.0, 15.0)
    visible_window = max(stale_after * 10.0, 300.0)

    async with session_factory() as session:
        db_now = await session.scalar(select(func.now()))
        if db_now is None:
            db_now = datetime.now(timezone.utc)
        cutoff = db_now - timedelta(seconds=visible_window)
        rows = (
            (
                await session.execute(
                    select(ServiceHeartbeat)
                    .where(ServiceHeartbeat.last_seen_at >= cutoff)
                    .order_by(ServiceHeartbeat.role.asc(), ServiceHeartbeat.instance_id.asc())
                )
            )
            .scalars()
            .all()
        )

    wallet_tasks = [
        _fetch_shard_wallets(session_factory, 0 if secondary_session_factory is not None else None)
    ]
    if secondary_session_factory is not None:
        wallet_tasks.append(_fetch_shard_wallets(secondary_session_factory, 1))
    wallet_results = await asyncio.gather(*wallet_tasks, return_exceptions=True)

    wallets_by_bucket: dict[int, list[dict[str, Any]]] = {i: [] for i in range(WALLET_BUCKETS)}
    failed_shards: list[int] = []
    for shard_idx, res in enumerate(wallet_results):
        if isinstance(res, Exception):
            failed_shards.append(shard_idx)
            continue
        for w in res:
            wallets_by_bucket[wallet_bucket(w["address"])].append(w)

    instances: list[dict[str, Any]] = []
    bucket_owners: dict[int, list[str]] = {i: [] for i in range(WALLET_BUCKETS)}
    active_apis = 0
    active_workers = 0

    for row in rows:
        age_seconds = max(0.0, (db_now - row.last_seen_at).total_seconds())
        is_active = age_seconds <= stale_after
        if is_active:
            if row.role == "api":
                active_apis += 1
            elif row.role == "worker":
                active_workers += 1
                for b in row.buckets_held or []:
                    if isinstance(b, int) and 0 <= b < WALLET_BUCKETS:
                        bucket_owners[b].append(row.instance_id)

        instances.append(
            {
                "instance_id": row.instance_id,
                "role": row.role,
                "shard_id": row.shard_id,
                "status": "active" if is_active else "stale",
                "started_at": utc_timestamp(row.started_at),
                "last_seen_at": utc_timestamp(row.last_seen_at),
                "age_seconds": round(age_seconds, 2),
                "uptime_seconds": round(max(0.0, (db_now - row.started_at).total_seconds()), 1),
                "ws_connections": row.ws_connections,
                "listener_ok": row.listener_ok,
                "buckets_held": list(row.buckets_held or []),
                "latency_ms": {
                    "p50": float(row.latency_p50_ms) if row.latency_p50_ms is not None else None,
                    "p95": float(row.latency_p95_ms) if row.latency_p95_ms is not None else None,
                    "p99": float(row.latency_p99_ms) if row.latency_p99_ms is not None else None,
                },
                "meta": row.meta or {},
            }
        )

    buckets: list[dict[str, Any]] = []
    split_claim_buckets: list[int] = []
    for b in range(WALLET_BUCKETS):
        owners = sorted(set(bucket_owners[b]))
        split_claim = len(owners) > 1
        if split_claim:
            split_claim_buckets.append(b)
        buckets.append(
            {
                "bucket_id": b,
                "shard_id": 0 if b < 8 else 1,
                "owner": owners[0] if len(owners) == 1 else (owners[0] if owners else None),
                "owners": owners,
                "split_claim": split_claim,
                "wallet_count": len(wallets_by_bucket[b]),
                "wallets": wallets_by_bucket[b],
            }
        )

    return {
        "serving_instance_id": settings.resolve_instance_id(),
        "db_time": utc_timestamp(db_now),
        "stale_after_seconds": stale_after,
        "partial": bool(failed_shards),
        "failed_shards": failed_shards,
        "summary": {
            "active_instances": active_apis + active_workers,
            "active_apis": active_apis,
            "active_workers": active_workers,
            "split_claim_buckets": split_claim_buckets,
            "sharded": secondary_session_factory is not None,
        },
        "instances": instances,
        "buckets": buckets,
    }
