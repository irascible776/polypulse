"""Scheduled market pricing, resolution checks, and serialized wallet polling.

CLOB batch APIs: https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body
and https://docs.polymarket.com/api-reference/market-data/get-last-trade-prices-request-body.
"""

import argparse
import asyncio
import json
import logging
import signal
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.accounting.projector import AccountingProjector
from app.chain.reconcile import reconcile_wallet
from app.chain.rpc import PolygonRpcClient
from app.chain.tail import scan_tail
from app.config import get_settings
from app.ingestion.data_api import DataApiSource
from app.ingestion.poller import LivePoller
from app.storage.database import async_session_factory, engine
from app.storage.models import (
    IngestionCursor,
    MarketPrice,
    MarketResolution,
    Position,
    ReconcileRun,
    TrackedWallet,
)
from app.valuation.resolution import check_resolution, save_resolution

logger = logging.getLogger("worker")
CLOB_URL = "https://clob.polymarket.com"


def log_event(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, default=str), flush=True)


class CountingSource:
    def __init__(self, source: DataApiSource) -> None:
        self.source = source
        self.fetched = 0
        self.final = False
        self.error: str | None = None

    @property
    def source_tag(self) -> str:
        return self.source.source_tag

    async def fetch_page(self, **kwargs: Any) -> Any:
        try:
            page = await self.source.fetch_page(**kwargs)
            self.fetched += len(page.events)
            if page.has_more and (not page.next_cursor or not page.events):
                raise RuntimeError("incomplete page: has_more without events and cursor")
            self.final = not page.has_more
            return page
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            raise

    async def fetch_status(self) -> Any:
        return await self.source.fetch_status()


class Worker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
        db_engine: AsyncEngine = engine,
        http: httpx.AsyncClient | None = None,
        concurrency: int = 4,
    ) -> None:
        self.sessions = session_factory
        self.engine = db_engine
        self.http = http or httpx.AsyncClient(timeout=20)
        self.owns_http = http is None
        self.limit = asyncio.Semaphore(concurrency)
        self.source = DataApiSource(global_concurrency=concurrency)
        self.stats: dict[str, Counter] = {name: Counter() for name in ("A", "B", "C", "D")}
        self.rate_headers: list[dict[str, Any]] = []
        self.urgent_resolutions: set[str] = set()
        self.resolution_wakeup = asyncio.Event()

    def queue_resolution(self, condition: str) -> None:
        self.urgent_resolutions.add(condition)
        self.resolution_wakeup.set()

    async def close(self) -> None:
        await self.source.close()
        if self.owns_http:
            await self.http.aclose()

    async def notify_wallet(self, wallet: str, event_type: str, ref: str) -> None:
        payload = json.dumps({"type": event_type, "wallet": wallet, "ref": ref})
        async with self.sessions() as session:
            async with session.begin():
                await session.execute(
                    text("select pg_notify('polypulse_events', :payload)"), {"payload": payload}
                )

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        async with self.limit:
            for attempt in range(4):
                try:
                    response = await self.http.request(method, CLOB_URL + path, **kwargs)
                    headers = {
                        k: v
                        for k, v in response.headers.items()
                        if "ratelimit" in k.lower() or k.lower() == "retry-after"
                    }
                    if headers or response.status_code == 429:
                        self.rate_headers.append(
                            {"path": path, "status": response.status_code, "headers": headers}
                        )
                    if response.status_code == 429 or response.status_code >= 500:
                        if attempt < 3:
                            await asyncio.sleep(0.5 * 2**attempt)
                            continue
                    return response
                except (httpx.NetworkError, httpx.TimeoutException):
                    if attempt == 3:
                        raise
                    await asyncio.sleep(0.5 * 2**attempt)
        raise RuntimeError("unreachable request retry state")

    async def active_wallets(self) -> list[str]:
        async with self.sessions() as session:
            return list(
                (
                    await session.execute(
                        select(TrackedWallet.address)
                        .where(TrackedWallet.status == "active")
                        .order_by(TrackedWallet.address)
                    )
                )
                .scalars()
                .all()
            )

    async def open_tokens(self, include_resolved: bool = False) -> list[tuple[str, str]]:
        async with self.sessions() as session:
            stmt = (
                select(Position.token_id, Position.condition_id)
                .join(TrackedWallet, TrackedWallet.address == Position.wallet)
                .outerjoin(MarketResolution, MarketResolution.condition_id == Position.condition_id)
                .where(TrackedWallet.status == "active", Position.shares > 0)
            )
            if not include_resolved:
                stmt = stmt.where(
                    or_(
                        MarketResolution.condition_id.is_(None),
                        MarketResolution.resolved.is_(False),
                    )
                )
            return list((await session.execute(stmt.distinct())).all())

    async def _individual_book(self, token: str) -> tuple[str, Any]:
        try:
            response = await self._request("GET", "/book", params={"token_id": token})
            if response.status_code == 404:
                self.stats["A"]["not_found"] += 1
                log_event("book_not_found", token=token, status=404)
                return token, None
            response.raise_for_status()
            return token, response.json()
        except Exception as exc:
            self.stats["A"]["errors"] += 1
            log_event("book_error", token=token, error=f"{type(exc).__name__}: {exc}")
            return token, None

    async def _individual_trade(self, token: str) -> tuple[str, Any]:
        try:
            response = await self._request("GET", "/last-trade-price", params={"token_id": token})
            if response.status_code == 404:
                return token, None
            response.raise_for_status()
            return token, response.json()
        except Exception as exc:
            self.stats["A"]["errors"] += 1
            log_event("trade_error", token=token, error=f"{type(exc).__name__}: {exc}")
            return token, None

    async def _batch(self, tokens: list[str], book: bool) -> dict[str, Any]:
        if not tokens:
            return {}
        path = "/books" if book else "/last-trades-prices"
        try:
            response = await self._request("POST", path, json=[{"token_id": t} for t in tokens])
            response.raise_for_status()
            rows = response.json()
            if not isinstance(rows, list):
                raise ValueError("batch response must be a list")
            key = "asset_id" if book else "token_id"
            found = {str(row[key]): row for row in rows if isinstance(row, dict) and key in row}
        except Exception as exc:
            log_event("batch_fallback", path=path, error=f"{type(exc).__name__}: {exc}")
            found = {}
        missing = [token for token in tokens if token not in found]
        fetch = self._individual_book if book else self._individual_trade
        found.update(dict(await asyncio.gather(*(fetch(token) for token in missing))))
        return found

    async def price_cycle(self) -> dict[str, Any]:
        started = time.perf_counter()
        stats = self.stats["A"]
        before = stats.copy()
        stats["cycles"] += 1
        valid_tokens: set[str] = set()
        pairs: list[tuple[str, str]] = []
        try:
            pairs = await self.open_tokens()
            for start in range(0, len(pairs), 100):
                chunk = pairs[start : start + 100]
                tokens = [token for token, _ in chunk]
                books, trades = await asyncio.gather(
                    self._batch(tokens, True), self._batch(tokens, False)
                )
                for token, condition in chunk:
                    try:
                        now = datetime.now(timezone.utc)
                        rows = []
                        book = books.get(token)
                        if book is None:
                            self.queue_resolution(condition)
                        if book:
                            bids = [Decimal(x["price"]) for x in book.get("bids", [])]
                            asks = [Decimal(x["price"]) for x in book.get("asks", [])]
                            if bids and asks:
                                bid, ask = max(bids), min(asks)
                                rows.append(
                                    MarketPrice(
                                        token_id=token,
                                        condition_id=condition,
                                        price=(bid + ask) / 2,
                                        source="live_quote",
                                        bid=bid,
                                        ask=ask,
                                        observed_at=now,
                                        created_at=now,
                                    )
                                )
                        trade = trades.get(token)
                        # The individual endpoint documents an empty side as
                        # the no-trade default; reject it conservatively here.
                        if (
                            trade
                            and trade.get("side") != ""
                            and trade.get("price") not in (None, "")
                        ):
                            price = Decimal(str(trade["price"]))
                            if 0 <= price <= 1:
                                rows.append(
                                    MarketPrice(
                                        token_id=token,
                                        condition_id=condition,
                                        price=price,
                                        source="last_trade",
                                        observed_at=now,
                                        created_at=now,
                                    )
                                )
                        if rows:
                            async with self.sessions() as session:
                                async with session.begin():
                                    session.add_all(rows)
                            stats.update(row.source for row in rows)
                            if any(
                                row.source == "last_trade"
                                or (
                                    row.bid is not None
                                    and row.ask is not None
                                    and Decimal(0) <= row.bid <= row.ask <= Decimal(1)
                                    and row.ask - row.bid <= get_settings().max_spread
                                )
                                for row in rows
                            ):
                                valid_tokens.add(token)
                    except Exception as exc:
                        stats["errors"] += 1
                        log_event(
                            "price_item_error", token=token, error=f"{type(exc).__name__}: {exc}"
                        )
        except Exception as exc:
            stats["errors"] += 1
            log_event("price_cycle_error", error=f"{type(exc).__name__}: {exc}")
        if pairs:
            async with self.sessions() as session:
                closed = (
                    (
                        await session.execute(
                            select(MarketResolution.condition_id).where(
                                MarketResolution.condition_id.in_({c for _, c in pairs}),
                                MarketResolution.gamma_closed.is_(True),
                                MarketResolution.resolved.is_(False),
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                for condition in closed:
                    self.queue_resolution(condition)
        if valid_tokens:
            async with self.sessions() as session:
                wallets = (
                    (
                        await session.execute(
                            select(Position.wallet)
                            .join(TrackedWallet, TrackedWallet.address == Position.wallet)
                            .where(
                                TrackedWallet.status == "active",
                                Position.shares > 0,
                                Position.token_id.in_(valid_tokens),
                            )
                            .distinct()
                        )
                    )
                    .scalars()
                    .all()
                )
            for wallet in wallets:
                await self.notify_wallet(wallet, "price_update", "price_cycle")
        result = {
            "seconds": round(time.perf_counter() - started, 3),
            **{
                k: stats[k] - before[k] for k in ("live_quote", "last_trade", "not_found", "errors")
            },
        }
        log_event("loop_a", **result)
        return result

    async def resolution_cycle(
        self, initial: bool = False, urgent_only: bool = False
    ) -> dict[str, Any]:
        started = time.perf_counter()
        stats = self.stats["B"]
        before = stats.copy()
        stats["cycles"] += 1
        try:
            urgent = self.urgent_resolutions.copy()
            self.urgent_resolutions.difference_update(urgent)
            self.resolution_wakeup.clear()
            pairs = [] if urgent_only else await self.open_tokens(include_resolved=initial)

            async def item(condition: str) -> None:
                async with self.limit:
                    try:
                        for attempt in range(4):
                            try:
                                record = await check_resolution(condition)
                                break
                            except httpx.HTTPStatusError as exc:
                                if attempt == 3 or (
                                    exc.response.status_code != 429
                                    and exc.response.status_code < 500
                                ):
                                    raise
                                await asyncio.sleep(0.5 * 2**attempt)
                        async with self.sessions() as session:
                            async with session.begin():
                                await save_resolution(session, record)
                        stats["upserted"] += 1
                    except Exception as exc:
                        stats["errors"] += 1
                        log_event(
                            "resolution_error",
                            condition=condition,
                            error=f"{type(exc).__name__}: {exc}",
                        )

            await asyncio.gather(
                *(item(c) for c in sorted({condition for _, condition in pairs} | urgent))
            )
        except Exception as exc:
            stats["errors"] += 1
            log_event("resolution_cycle_error", error=f"{type(exc).__name__}: {exc}")
        result = {
            "seconds": round(time.perf_counter() - started, 3),
            "upserted": stats["upserted"] - before["upserted"],
            "errors": stats["errors"] - before["errors"],
            "initial": initial,
        }
        log_event("loop_b", **result)
        return result

    async def wallet_cycle(self, wallet: str) -> dict[str, Any]:
        started = time.perf_counter()
        stats = self.stats["C"]
        stats["cycles"] += 1
        async with self.engine.connect() as lock_conn:
            locked = await lock_conn.scalar(
                text("select pg_try_advisory_lock(hashtextextended(:w, 0))"), {"w": wallet.lower()}
            )
            if not locked:
                stats["skipped_lock"] += 1
                result = {"wallet": wallet, "skipped_lock": True}
                log_event("loop_c", **result)
                return result
            try:
                counted = CountingSource(self.source)
                poller = LivePoller(wallet, counted, self.sessions)
                inserted = await poller._execute_poll_cycle(update_last_success=False)
                if counted.error or not counted.final:
                    raise RuntimeError(f"poll incomplete: {counted.error}")
                processed = 0
                while True:
                    async with self.sessions() as session:
                        async with session.begin():
                            summary = await AccountingProjector().catch_up_wallet(session, wallet)
                    processed += summary.events_processed
                    if summary.events_processed == 0 or summary.is_recompute:
                        break
                now = datetime.now(timezone.utc)
                async with self.sessions() as session:
                    async with session.begin():
                        cursor = await session.get(
                            IngestionCursor, (wallet.lower(), "data_api"), with_for_update=True
                        )
                        if cursor is None:
                            raise RuntimeError("missing data_api cursor after poll")
                        cursor.last_success_at = now
                if processed:
                    await self.notify_wallet(wallet, "pnl_update", "projector")
                result = {
                    "wallet": wallet,
                    "fetched": counted.fetched,
                    "skipped": counted.fetched - inserted,
                    "inserted": inserted,
                    "late": poller.metrics.total_late_events,
                    "projected": processed,
                    "last_success_at": now.isoformat(),
                    "seconds": round(time.perf_counter() - started, 3),
                }
                stats["inserted"] += inserted
                stats["fetched"] += counted.fetched
                stats["skipped"] += counted.fetched - inserted
                stats["late"] += poller.metrics.total_late_events
                log_event("loop_c", **result)
                return result
            except Exception as exc:
                stats["errors"] += 1
                result = {"wallet": wallet, "error": f"{type(exc).__name__}: {exc}"}
                log_event("loop_c_error", **result)
                return result
            finally:
                await lock_conn.execute(
                    text("select pg_advisory_unlock(hashtextextended(:w, 0))"),
                    {"w": wallet.lower()},
                )

    async def poll_cycle(self) -> None:
        try:
            wallets = await self.active_wallets()
            if not wallets:
                raise RuntimeError("no active tracked wallets")
            await asyncio.gather(*(self.wallet_cycle(wallet) for wallet in wallets))
        except Exception as exc:
            self.stats["C"]["errors"] += 1
            log_event("poll_cycle_error", error=f"{type(exc).__name__}: {exc}")

    async def reconcile_cycle(self) -> None:
        logger.info("loop_d_start timestamp=%s", datetime.now(timezone.utc).isoformat())
        stats = self.stats["D"]
        stats["cycles"] += 1
        client = PolygonRpcClient(endpoints=["https://polygon.drpc.org/"], max_retries=1)
        for wallet in await self.active_wallets():
            started = datetime.now(timezone.utc)
            block_number = None
            block_time = None
            summary = None
            error = None
            try:
                block_number = await client.get_latest_block_number() - 32
                block = await client.call("eth_getBlockByNumber", [hex(block_number), False])
                block_time = datetime.fromtimestamp(int(block["timestamp"], 16), tz=timezone.utc)
                scan = await scan_tail(wallet, block_number)
                summary = await reconcile_wallet(client, wallet, wallet, block_number, block_time)
                stats["matches"] += summary["perfect_matches"]
                stats["mismatches"] += summary["mismatches"]
                stats["unknown"] += summary["query_failures"]
                stats["in_flight"] += summary["in_flight_count"]
                log_event(
                    "loop_d",
                    wallet=wallet,
                    scan=scan,
                    reference_block=block_number,
                    matches=summary["perfect_matches"],
                    mismatches=summary["mismatches"],
                    in_flight=summary["in_flight_count"],
                    unknown=summary["query_failures"],
                )
            except Exception as exc:
                stats["errors"] += 1
                error = f"{type(exc).__name__}: {exc}"
                log_event("loop_d_error", wallet=wallet, error=error)
            details = (
                []
                if summary is None
                else [
                    {
                        key: str(value) if isinstance(value, Decimal) else value
                        for key, value in item.items()
                    }
                    for item in summary["results"]
                    if item.get("status") != "MATCH"
                ][:50]
            )
            async with self.sessions() as session:
                async with session.begin():
                    session.add(
                        ReconcileRun(
                            wallet=wallet,
                            reference_block=block_number,
                            reference_time=block_time,
                            match_count=summary["perfect_matches"] if summary else None,
                            mismatch_count=summary["mismatches"] if summary else None,
                            in_flight_count=summary["in_flight_count"] if summary else None,
                            unknown_count=summary["query_failures"] if summary else None,
                            details=details,
                            started_at=started,
                            finished_at=datetime.now(timezone.utc),
                            error=error,
                        )
                    )
        logger.info("loop_d_complete timestamp=%s", datetime.now(timezone.utc).isoformat())


async def run_worker(
    duration: float | None = None, once: bool = False, price_cycles: int = 0
) -> None:
    worker = Worker()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async def periodic(name: str, interval: float) -> None:
        initial = True
        next_full_resolution = 0.0
        while not stop.is_set():
            started = time.monotonic()
            if name == "A":
                await worker.price_cycle()
            elif name == "B":
                full = initial or started >= next_full_resolution
                await worker.resolution_cycle(initial=initial, urgent_only=not full)
                if full:
                    next_full_resolution = time.monotonic() + interval
            elif name == "C":
                await worker.poll_cycle()
            else:
                await worker.reconcile_cycle()
            initial = False
            remaining = max(0.01, interval - (time.monotonic() - started))
            if name == "B":
                remaining = max(0.01, next_full_resolution - time.monotonic())
                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(stop.wait()),
                        asyncio.create_task(worker.resolution_wakeup.wait()),
                    ],
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if done:
                    continue
            else:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass

    started = time.monotonic()
    try:
        if price_cycles:
            for _ in range(price_cycles):
                if stop.is_set():
                    break
                await worker.price_cycle()
        elif once:
            await worker.resolution_cycle(initial=True)
            await worker.price_cycle()
            await worker.poll_cycle()
        else:
            tasks = [
                asyncio.create_task(periodic(name, interval))
                for name, interval in (("A", 15), ("B", 300), ("C", 60), ("D", 600))
            ]
            try:
                if duration is None:
                    await stop.wait()
                else:
                    try:
                        await asyncio.wait_for(stop.wait(), timeout=duration)
                    except asyncio.TimeoutError:
                        stop.set()
            finally:
                stop.set()
                await asyncio.gather(*tasks)
    finally:
        log_event(
            "summary",
            seconds=round(time.monotonic() - started, 3),
            loops={name: dict(stats) for name, stats in worker.stats.items()},
            rate_headers=worker.rate_headers,
        )
        await worker.close()
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--price-cycles", type=int, default=0)
    args = parser.parse_args()
    asyncio.run(run_worker(args.duration, args.once, args.price_cycles))


if __name__ == "__main__":
    main()
