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

from app.config import get_settings
from app.serving.time_format import utc_timestamp
from app.storage.database import wallet_read_session

logger = logging.getLogger(__name__)
FLUSH_DELAY_SECONDS = 1
REFRESH_TIMEOUT_SECONDS = 10
REFRESH_ATTEMPTS = 3


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
        self._accounting_revisions: dict[WebSocket, int] = {}
        self._send_locks: dict[WebSocket, asyncio.Lock] = {}
        self._lock = asyncio.Lock()
        self._pending: dict[str, tuple[str, dict[str, Any]]] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._listener: asyncio.Task | None = None
        self._listening: dict[str, asyncio.Event] = {}

    async def connect(self, wallet: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.active_connections.setdefault(wallet.lower(), set()).add(websocket)
            self.sequences[websocket] = 0
            self._accounting_revisions[websocket] = -1
            self._send_locks[websocket] = asyncio.Lock()

    async def disconnect(self, wallet: str, websocket: WebSocket) -> None:
        async with self._lock:
            self.active_connections.get(wallet.lower(), set()).discard(websocket)
            if not self.active_connections.get(wallet.lower()):
                self.active_connections.pop(wallet.lower(), None)
            self.sequences.pop(websocket, None)
            self._accounting_revisions.pop(websocket, None)
            self._send_locks.pop(websocket, None)

    async def _state(self, wallet: str) -> tuple[dict[str, Any], dict[str, Any]]:
        from app.serving.routes import valuation_engine, wallet_freshness

        async with wallet_read_session(wallet) as session:
            valuation_engine.invalidate(wallet)
            summary = await valuation_engine.value_wallet(session, wallet)
            freshness = await wallet_freshness(session, wallet)
            payload = summary.to_dict()
            payload["accounting_current"] = freshness["accounting_current"]
            payload["accounting_applied_order"] = freshness["accounting_applied_order"]
            payload["accounting_revision"] = freshness["accounting_revision"]
            return payload, freshness

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
                revision = freshness.get("accounting_revision", 0)
                if revision < self._accounting_revisions[ws]:
                    return
                self._accounting_revisions[ws] = revision
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
                await self.broadcast_to_wallet(wallet, "pnl_update", {"ref": "resync"})

    async def _flush(self, wallet: str) -> None:
        failures = 0
        try:
            while wallet in self._pending:
                await asyncio.sleep(FLUSH_DELAY_SECONDS * 2**failures)
                if not self.active_connections.get(wallet):
                    self._pending.pop(wallet, None)
                    return
                pending = self._pending[wallet]
                event_type, extra = pending
                try:
                    payload, freshness = await asyncio.wait_for(
                        self._state(wallet), REFRESH_TIMEOUT_SECONDS
                    )
                except Exception as exc:
                    failures += 1
                    logger.warning("WS refresh failed for %s: %s", wallet, type(exc).__name__)
                    if failures < REFRESH_ATTEMPTS:
                        continue
                    # Do not leave an apparently healthy socket with stale data.
                    self._pending.pop(wallet, None)
                    targets = list(self.active_connections.get(wallet, ()))
                    for ws in targets:
                        await self.disconnect(wallet, ws)
                        try:
                            await asyncio.wait_for(ws.close(code=1013), REFRESH_TIMEOUT_SECONDS)
                        except Exception:
                            pass
                    return
                failures = 0
                # A newer notification may have arrived while the read awaited SQL.
                if self._pending.get(wallet) is pending:
                    self._pending.pop(wallet)
                payload.update(extra)
                targets = list(self.active_connections.get(wallet, ()))
                for ws in targets:
                    try:
                        await asyncio.wait_for(
                            self._send(ws, wallet, event_type, payload, freshness),
                            REFRESH_TIMEOUT_SECONDS,
                        )
                    except Exception:
                        await self.disconnect(wallet, ws)
                        try:
                            await asyncio.wait_for(ws.close(code=1013), REFRESH_TIMEOUT_SECONDS)
                        except Exception:
                            pass
        finally:
            self._tasks.pop(wallet, None)

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

    async def listen(self, database_url: str | None = None) -> None:
        # Use the URL as the dict key; fall back to a sentinel for the primary.
        url_key = database_url or "__primary__"
        if url_key not in self._listening:
            self._listening[url_key] = asyncio.Event()
        ev = self._listening[url_key]
        delay = 0.5
        while True:
            try:
                dsn, connect_args = get_settings().psycopg_url_and_args(database_url)
                async with await psycopg.AsyncConnection.connect(
                    dsn,
                    autocommit=True,
                    application_name="polypulse_listener",
                    **connect_args,
                ) as conn:
                    await conn.execute("LISTEN polypulse_events")
                    await self.resync()
                    ev.set()
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
                ev.clear()
                raise
            except Exception as exc:
                ev.clear()
                logger.warning("LISTEN disconnected: %s: %s", type(exc).__name__, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    @property
    def is_listening(self) -> bool:
        if not self._listening:
            return False
        return all(ev.is_set() for ev in self._listening.values())

    def start(self) -> None:
        if self._listener is None or self._listener.done():

            async def listen_all() -> None:
                urls = [get_settings().database_url]
                if get_settings().secondary_database_url:
                    urls.append(get_settings().secondary_database_url)
                await asyncio.gather(*(self.listen(url) for url in urls))

            self._listener = asyncio.create_task(listen_all())

    async def stop(self) -> None:
        self._pending.clear()
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
