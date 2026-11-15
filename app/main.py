"""
app/main.py

Main FastAPI application entrypoint for PolyPulse.
Wires together:
- Health, readiness, and ingestion status probes
- Serving layer REST API & WebSocket endpoints
- Real-time quantitative terminal dashboard
- Latency tracking middleware (p50/p95/p99)
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator

from fastapi import FastAPI, Response, status
from fastapi.responses import HTMLResponse

from app.config import get_settings
from app.ingestion.status import get_all_ingestion_statuses, get_wallet_ingestion_status
from app.serving.health import get_health
from app.serving.middleware import LatencyMiddleware
from app.serving.routes import router as serving_router
from app.serving.websocket import ws_manager
from app.storage.database import async_session, check_database_connection, engine

settings = get_settings()

DASHBOARD_HTML_PATH = Path(__file__).parent / "serving" / "dashboard.html"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan manager to gracefully manage resources."""
    if not os.environ.get("POLYPULSE_API_KEY"):
        raise RuntimeError("POLYPULSE_API_KEY environment variable is required")
    ws_manager.start()
    try:
        yield
    finally:
        await ws_manager.stop()
    # Cleanup DB connection pool on shutdown
    await engine.dispose()


app = FastAPI(
    title="PolyPulse",
    description=(
        "Real-time wallet analytics, deterministic PnL engine, "
        "and on-chain verification for Polymarket"
    ),
    version="0.1.0",
    lifespan=lifespan,
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
    Returns HTTP 200 when ready, HTTP 503 when the database is unreachable.
    """
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
    async with async_session() as session:
        if wallet:
            return await get_wallet_ingestion_status(session, wallet)
        return await get_all_ingestion_statuses(session)
