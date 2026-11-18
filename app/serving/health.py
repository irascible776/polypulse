"""Non-blocking RPC health and wallet reconciliation status."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc, select

from app.chain.rpc import PolygonRpcClient
from app.config import get_settings
from app.ingestion.status import ingestion_freshness
from app.serving.time_format import utc_timestamp
from app.storage.database import (
    async_session_factory,
    engine,
    secondary_session_factory,
    validate_shard_identity,
)
from app.storage.models import ChainEvent, IngestionCursor, ReconcileRun, TrackedWallet

_rpc: dict[str, Any] = {"block_number": None, "latency_ms": None, "error": None}
_last_probe = 0.0
_probe_task: asyncio.Task | None = None


async def _probe() -> None:
    started = time.monotonic()
    try:
        block = await asyncio.wait_for(
            PolygonRpcClient(timeout=2, max_retries=1).get_latest_block_number(), timeout=3
        )
        _rpc.update(
            block_number=block, latency_ms=round((time.monotonic() - started) * 1000, 2), error=None
        )
    except Exception as exc:
        _rpc.update(error=f"{type(exc).__name__}: {exc}")


def rpc_snapshot() -> dict[str, Any]:
    global _last_probe, _probe_task
    now = time.monotonic()
    if now - _last_probe >= 10 and (_probe_task is None or _probe_task.done()):
        _last_probe = now
        _probe_task = asyncio.create_task(_probe())
    return dict(_rpc)


def health_status(ingestion_stale: bool, run: ReconcileRun | None, now: datetime) -> str:
    if ingestion_stale or run is None or run.error or run.mismatch_count is None:
        return "degraded"
    age = (now - run.finished_at).total_seconds()
    return (
        "degraded"
        if run.mismatch_count > 0 or age > get_settings().reconcile_stale_seconds
        else "ok"
    )


async def get_health() -> dict[str, Any]:
    pool = engine.pool
    result: dict[str, Any] = {
        "status": "ok",
        "instance_id": get_settings().resolve_instance_id(),
        "db_pool": {
            "size": pool.size(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
        },
        "rpc": rpc_snapshot(),
        "latest_chain_block": None,
        "wallets": {},
    }
    factories = [async_session_factory]
    if secondary_session_factory is not None:
        factories.append(secondary_session_factory)
    available = 0
    result["shards"] = {}
    for shard, factory in enumerate(factories):
        try:
            async with factory() as session:
                if secondary_session_factory is not None:
                    await validate_shard_identity(session, shard)
                latest = await session.scalar(
                    select(ChainEvent.block_number).order_by(desc(ChainEvent.block_number)).limit(1)
                )
                if latest is not None:
                    result["latest_chain_block"] = max(
                        result["latest_chain_block"] or latest, latest
                    )
                wallets = (
                    (
                        await session.execute(
                            select(TrackedWallet).where(TrackedWallet.status == "active")
                        )
                    )
                    .scalars()
                    .all()
                )
                now = datetime.now(timezone.utc)
                for wallet in wallets:
                    cursor = await session.get(IngestionCursor, (wallet.address, "data_api"))
                    freshness = ingestion_freshness(cursor.last_success_at if cursor else None)
                    run = await session.scalar(
                        select(ReconcileRun)
                        .where(ReconcileRun.wallet == wallet.address)
                        .order_by(desc(ReconcileRun.finished_at), desc(ReconcileRun.id))
                        .limit(1)
                    )
                    entry = {**freshness, "reconcile": None}
                    if run:
                        entry["reconcile"] = {
                            "match_count": run.match_count,
                            "mismatch_count": run.mismatch_count,
                            "in_flight_count": run.in_flight_count,
                            "unknown_count": run.unknown_count,
                            "reference_block": run.reference_block,
                            "reference_time": utc_timestamp(run.reference_time),
                            "finished_at": utc_timestamp(run.finished_at),
                            "age_seconds": max(0.0, (now - run.finished_at).total_seconds()),
                            "error": run.error,
                        }
                    entry["status"] = health_status(freshness["ingestion_stale"], run, now)
                    if entry["status"] != "ok":
                        result["status"] = "degraded"
                    result["wallets"][wallet.address] = entry
                available += 1
                result["shards"][str(shard)] = {"status": "available"}
        except Exception as exc:
            result["status"] = "degraded"
            result["shards"][str(shard)] = {"status": "unavailable", "error": type(exc).__name__}
            result["error"] = f"{type(exc).__name__}: {exc}"
    if not available:
        result["status"] = "down"
    return result
