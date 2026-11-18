"""
app/main.py

Main FastAPI application entrypoint for PolyPulse.
Wires together:
- Health, readiness, and ingestion status probes
- Serving layer REST API & WebSocket endpoints
- Real-time quantitative terminal dashboard
- Latency tracking middleware (p50/p95/p99)
"""

import asyncio
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from app.config import get_settings
from app.ingestion.status import get_all_ingestion_statuses, get_wallet_ingestion_status
from app.serving.fleet import delete_heartbeat, upsert_heartbeat
from app.serving.health import get_health
from app.serving.middleware import LatencyMiddleware, global_latency_tracker
from app.serving.routes import router as serving_router
from app.serving.websocket import ws_manager
from app.storage.database import (
    async_session,
    check_database_connection,
    check_shard_ready,
    engine,
    secondary_engine,
    secondary_session_factory,
    validate_shard_identity,
    wallet_read_session,
)

settings = get_settings()

DASHBOARD_HTML_PATH = Path(__file__).parent / "serving" / "dashboard.html"
_draining = False


async def _api_heartbeat_loop(
    instance_id: str, started_at: datetime, stop_event: asyncio.Event
) -> None:
    interval = get_settings().fleet_heartbeat_seconds
    while not stop_event.is_set():
        stats = global_latency_tracker.get_stats()
        ws_stats = ws_manager.get_stats()
        await upsert_heartbeat(
            instance_id=instance_id,
            role="api",
            started_at=started_at,
            shard_id=None,
            ws_connections=ws_stats["total_active_connections"],
            listener_ok=ws_manager.is_listening,
            buckets_held=[],
            latency_p50_ms=stats.get("p50"),
            latency_p95_ms=stats.get("p95"),
            latency_p99_ms=stats.get("p99"),
            meta={
                "sharded": secondary_engine is not None,
                "wallets_subscribed": ws_stats["total_wallets_subscribed"],
            },
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager to gracefully manage resources."""
    global _draining
    if not os.environ.get("POLYPULSE_API_KEY"):
        raise RuntimeError("POLYPULSE_API_KEY environment variable is required")
    _draining = False
    started_at = datetime.now(timezone.utc)
    instance_id = get_settings().resolve_instance_id()
    stop_heartbeat = asyncio.Event()
    ws_manager.start()
    heartbeat_task = asyncio.create_task(
        _api_heartbeat_loop(instance_id, started_at, stop_heartbeat)
    )

    # Register SIGTERM handler immediately so HAProxy sees 503 on /ready
    # during the full drain window, not only after Uvicorn stops accepting.
    loop = asyncio.get_running_loop()
    drain_seconds = get_settings().shutdown_drain_seconds

    def _handle_sigterm() -> None:
        global _draining
        _draining = True

    loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)

    try:
        yield
    finally:
        if drain_seconds > 0 and not _draining:
            # Uvicorn shutdown path (not SIGTERM) — drain now
            _draining = True
            await asyncio.sleep(drain_seconds)
        elif _draining and drain_seconds > 0:
            # SIGTERM arrived before yield exited; drain remaining budget
            await asyncio.sleep(drain_seconds)
        loop.remove_signal_handler(signal.SIGTERM)
        stop_heartbeat.set()
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        await delete_heartbeat(instance_id)
        await ws_manager.stop()
        _draining = False
    # Cleanup DB connection pool on shutdown
    await engine.dispose()
    if secondary_engine is not None:
        await secondary_engine.dispose()


app = FastAPI(
    title="PolyPulse",
    description=(
        "Real-time wallet analytics, deterministic PnL engine, "
        "and on-chain verification for Polymarket"
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Optional CORS for cross-origin dashboards (e.g. Vercel frontend)
_cors_origins = settings.parsed_cors_origins()
if _cors_origins or settings.cors_origin_regex:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_origin_regex=settings.cors_origin_regex,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "X-API-Key", "X-Request-Id"],
        expose_headers=["X-Instance-Id", "X-Response-Time-Ms", "X-Request-Id"],
    )

# Latency tracking middleware
app.add_middleware(LatencyMiddleware)

# Include Phase 5 serving layer router
app.include_router(serving_router)


@app.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> dict[str, Any]:
    return await get_health()


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard() -> str:
    """Serves the PolyPulse live terminal dashboard."""
    if DASHBOARD_HTML_PATH.exists():
        return DASHBOARD_HTML_PATH.read_text(encoding="utf-8")
    return "<h1>PolyPulse Dashboard</h1>"


@app.get("/ready")
async def readiness_check(response: Response) -> dict[str, str]:
    """
    Readiness probe: verifies that backing infrastructure (PostgreSQL) is reachable.
    Returns HTTP 200 when ready, HTTP 503 when draining or database is unreachable.
    """
    if _draining:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "not ready", "database": "draining"}
    if secondary_engine is not None:
        primary_ready, secondary_ready = await asyncio.gather(
            check_shard_ready(engine, 0), check_shard_ready(secondary_engine, 1)
        )
        if primary_ready != secondary_ready:
            response.status_code = status.HTTP_200_OK
            return {"status": "ready", "database": "partial"}
        is_connected = primary_ready and secondary_ready
    else:
        is_connected = await check_database_connection()
    if is_connected:
        response.status_code = status.HTTP_200_OK
        return {"status": "ready", "database": "connected"}

    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "not ready", "database": "disconnected"}


@app.get("/ingestion/status")
async def ingestion_status(wallet: str | None = None) -> dict[str, Any]:
    """
    Returns ingestion status per wallet: backfill_state, last_success_at,
    events stored, counts per event type, unsupported type count, consecutive_failures,
    and detection lag p50 and p95 over the last 1,000 new events.
    """
    try:
        if wallet:
            async with wallet_read_session(wallet) as session:
                return await get_wallet_ingestion_status(session, wallet)
        async with async_session() as session:
            if secondary_session_factory is not None:
                await validate_shard_identity(session, 0)
            result = await get_all_ingestion_statuses(session)
        if secondary_session_factory is not None:
            async with secondary_session_factory() as session:
                await validate_shard_identity(session, 1)
                other = await get_all_ingestion_statuses(session)
            result["wallets"].update(other["wallets"])
            result["total_tracked"] = len(result["wallets"])
        return result
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Ingestion status unavailable") from exc
