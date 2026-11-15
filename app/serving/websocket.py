"""One PostgreSQL listener and coalesced per-wallet WebSocket delivery.

LISTEN requires a session connection; PgBouncer transaction pooling breaks it.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
from fastapi import WebSocket
from sqlalchemy.engine import make_url

from app.config import get_settings
from app.ingestion.status import ingestion_freshness
from app.serving.time_format import utc_timestamp
from app.storage.database import async_session_factory
from app.storage.models import IngestionCursor

logger = logging.getLogger(__name__)


def _money_strings(value: Any, key: str = "") -> Any:
    if isinstance(value, dict):
        return {k: _money_strings(v, k) for k, v in value.items()}
    if isinstance(value, list):
        return [_money_strings(v, key) for v in value]
    financial = key in {
        "price",
        "bid",
        "ask",
        "shares",
        "cash_delta",
        "cost_basis_total",
        "unrealized_priced",
        "estimated_unrealized",
        "total_pnl_excl_unpriced",
    } or key.endswith(("_pnl", "_pnl_delta", "_price", "_shares", "_basis", "_value", "_delta"))
    if financial and isinstance(value, float):
        raise TypeError(f"financial field {key} must not be a float")
    if financial and isinstance(value, (Decimal, int)) and not isinstance(value, bool):
        return str(value)
    return value


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, set[WebSocket]] = {}
        self.sequences: dict[WebSocket, int] = {}
        self._send_locks: dict[WebSocket, asyncio.Lock] = {}
        self._lock = asyncio.Lock()
        self._pending: dict[str, tuple[str, dict[str, Any]]] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._listener: asyncio.Task | None = None
        self._listening = asyncio.Event()

    async def connect(self, wallet: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.setdefault(wallet.lower(), set()).add(websocket)
            self.sequences[websocket] = 0
            self._send_locks[websocket] = asyncio.Lock()

    async def disconnect(self, wallet: str, websocket: WebSocket) -> None:
        async with self._lock:
            self.active_connections.get(wallet.lower(), set()).discard(websocket)
            if not self.active_connections.get(wallet.lower()):
                self.active_connections.pop(wallet.lower(), None)
            self.sequences.pop(websocket, None)
            self._send_locks.pop(websocket, None)

    async def _state(self, wallet: str) -> tuple[dict[str, Any], dict[str, Any]]:
        from app.serving.routes import valuation_engine

        async with async_session_factory() as session:
            valuation_engine.invalidate(wallet)
            summary = await valuation_engine.value_wallet(session, wallet)
            cursor = await session.get(IngestionCursor, (wallet, "data_api"))
            return summary.to_dict(), ingestion_freshness(
                cursor.last_success_at if cursor else None
            )

    async def _send(
        self, ws: WebSocket, wallet: str, event_type: str, payload: dict, freshness: dict
    ) -> None:
        lock = self._send_locks.get(ws)
        if lock is None:
            return
        async with lock:
            async with self._lock:
                if ws not in self.sequences:
                    return
                self.sequences[ws] += 1
                seq = self.sequences[ws]
            await ws.send_json(
                {
                    "type": event_type,
                    "wallet": wallet,
                    "seq": seq,
                    "server_time": utc_timestamp(datetime.now(timezone.utc)),
                    "data_as_of": freshness["data_as_of"],
                    "ingestion_stale": freshness["ingestion_stale"],
                    "payload": _money_strings(payload),
                }
            )

    async def snapshot(self, wallet: str, ws: WebSocket) -> None:
        payload, freshness = await self._state(wallet)
        await self._send(ws, wallet, "snapshot", payload, freshness)

    async def resync(self) -> None:
        async with self._lock:
            targets = [
                (wallet, ws)
                for wallet, sockets in self.active_connections.items()
                for ws in sockets
            ]
        for wallet, ws in targets:
            try:
                await self.snapshot(wallet, ws)
            except Exception as exc:
                logger.warning("WS resync failed for %s: %s: %s", wallet, type(exc).__name__, exc)
                await self.disconnect(wallet, ws)

    async def _flush(self, wallet: str) -> None:
        try:
            await asyncio.sleep(1)
            event_type, extra = self._pending.pop(wallet)
            payload, freshness = await self._state(wallet)
            payload.update(extra)
            async with self._lock:
                targets = list(self.active_connections.get(wallet, ()))
            for ws in targets:
                try:
                    await self._send(ws, wallet, event_type, payload, freshness)
                except Exception:
                    await self.disconnect(wallet, ws)
        finally:
            self._tasks.pop(wallet, None)
            if wallet in self._pending:
                self._tasks[wallet] = asyncio.create_task(self._flush(wallet))

    async def broadcast_to_wallet(
        self, wallet: str, event_type: str, payload: dict[str, Any]
    ) -> int:
        wallet = wallet.lower()
        if event_type not in {"pnl_update", "trade", "price_update"}:
            raise ValueError("unsupported wallet notification")
        if not self.active_connections.get(wallet):
            return 0
        self._pending[wallet] = (event_type, payload)
        if wallet not in self._tasks:
            self._tasks[wallet] = asyncio.create_task(self._flush(wallet))
        return len(self.active_connections.get(wallet, ()))

    async def listen(self) -> None:
        delay = 0.5
        while True:
            try:
                dsn = (
                    make_url(get_settings().database_url)
                    .set(drivername="postgresql")
                    .render_as_string(hide_password=False)
                )
                async with await psycopg.AsyncConnection.connect(
                    dsn, autocommit=True, application_name="polypulse_listener"
                ) as conn:
                    await conn.execute("LISTEN polypulse_events")
                    await self.resync()
                    self._listening.set()
                    delay = 0.5
                    async for notification in conn.notifies():
                        try:
                            item = json.loads(notification.payload)
                            if set(item) == {"type", "wallet", "ref"}:
                                await self.broadcast_to_wallet(
                                    item["wallet"], item["type"], {"ref": item["ref"]}
                                )
                        except (ValueError, TypeError, KeyError) as exc:
                            logger.warning("Invalid notification: %s: %s", type(exc).__name__, exc)
            except asyncio.CancelledError:
                self._listening.clear()
                raise
            except Exception as exc:
                self._listening.clear()
                logger.warning("LISTEN disconnected: %s: %s", type(exc).__name__, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    def start(self) -> None:
        if self._listener is None or self._listener.done():
            self._listener = asyncio.create_task(self.listen())

    async def stop(self) -> None:
        tasks = list(self._tasks.values()) + ([self._listener] if self._listener else [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._listener = None

    def get_stats(self) -> dict[str, Any]:
        return {
            "total_wallets_subscribed": len(self.active_connections),
            "total_active_connections": sum(len(v) for v in self.active_connections.values()),
        }


ws_manager = ConnectionManager()
