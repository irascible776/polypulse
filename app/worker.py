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
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.accounting.executor import ConditionExecutor
from app.accounting.projector import AccountingProjector
from app.chain.reconcile import reconcile_wallet
from app.chain.rpc import PolygonRpcClient
from app.chain.tail import scan_tail
from app.config import get_settings
from app.ingestion.data_api import DataApiSource
from app.ingestion.poller import LivePoller
from app.serving.fleet import delete_heartbeat, upsert_heartbeat
from app.storage.database import (
    async_session_factory,
    engine,
    secondary_engine,
    secondary_session_factory,
    validate_shard_identity,
)
from app.storage.models import (
    IngestionCursor,
    MarketPrice,
    MarketResolution,
    Position,
    ReconcileRun,
    TrackedWallet,
)
from app.storage.price_archive import archive_old_prices
from app.storage.price_partitions import ensure_price_partitions
from app.storage.sharding import wallet_bucket
from app.valuation.resolution import check_resolution, save_resolution

logger = logging.getLogger("worker")
CLOB_URL = "https://clob.polymarket.com"
BUCKET_LOCK_NAMESPACE = 1347505237


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
        concurrency: int | None = None,
        market_sessions: async_sessionmaker[AsyncSession] | None = None,
        shard_id: int | None = None,
    ) -> None:
        settings = get_settings()
        effective_concurrency = (
            concurrency if concurrency is not None else settings.worker_concurrency
        )
        self.sessions = session_factory
        self.market_sessions = market_sessions or session_factory
        self.shard_id = shard_id
        self.engine = db_engine
        self.http = http or httpx.AsyncClient(timeout=20)
        self.owns_http = http is None
        self.limit = asyncio.Semaphore(effective_concurrency)
        self.wallet_limit = asyncio.Semaphore(effective_concurrency)
        self.source = DataApiSource(global_concurrency=effective_concurrency)
        self.stats: dict[str, Counter] = {name: Counter() for name in ("A", "B", "C", "D")}
        self.rate_headers: list[dict[str, Any]] = []
        self.urgent_resolutions: set[str] = set()
        self.resolution_wakeup = asyncio.Event()
        self.held_buckets: set[int] = set()
        self.parallel_wallets = {
            value.strip().lower()
            for value in settings.accounting_parallel_wallets.split(",")
            if value.strip()
        }
        self.condition_replay_wallets = {
            value.strip().lower()
            for value in settings.accounting_condition_replay_wallets.split(",")
            if value.strip()
        }
        self.accounting_executor = (
            ConditionExecutor(settings.accounting_parallel_processes)
            if settings.accounting_parallel_processes and self.parallel_wallets
            else None
        )

    def queue_resolution(self, condition: str) -> None:
        self.urgent_resolutions.add(condition)
        self.resolution_wakeup.set()

    async def close(self) -> None:
        await self.source.close()
        if self.accounting_executor is not None:
            await self.accounting_executor.close()
        if self.owns_http:
            await self.http.aclose()

    async def notify_wallet(
        self,
        wallet: str,
        event_type: str,
        ref: str,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        payload = json.dumps({"type": event_type, "wallet": wallet, "ref": ref})
        factory = session_factory or self.sessions
        async with factory() as session:
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

    async def active_wallets(self, session_factory=None) -> list[str]:
        factory = session_factory or self.sessions
        async with factory() as session:
            wallets = list(
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
            if self.shard_id is not None:
                await validate_shard_identity(session, self.shard_id)
                if any(
                    (0 if wallet_bucket(wallet) < 8 else 1) != self.shard_id for wallet in wallets
                ):
                    raise RuntimeError("Wallet shard identity or ownership mismatch")
            return wallets

    async def open_tokens(
        self,
        include_resolved: bool = False,
        market_sessions: async_sessionmaker[AsyncSession] | None = None,
    ) -> list[tuple[str, str]]:
        if self.shard_id is not None:
            await self.active_wallets()
        async with self.sessions() as session:
            stmt = (
                select(Position.token_id, Position.condition_id)
                .join(TrackedWallet, TrackedWallet.address == Position.wallet)
                .where(TrackedWallet.status == "active", Position.shares > 0)
            )
            pairs = list((await session.execute(stmt.distinct())).all())
        if include_resolved or not pairs:
            return pairs
        mkt = market_sessions or self.market_sessions
        async with mkt() as session:
            resolved = set(
                (
                    await session.execute(
                        select(MarketResolution.condition_id).where(
                            MarketResolution.condition_id.in_(
                                {condition for _, condition in pairs}
                            ),
                            MarketResolution.resolved.is_(True),
                        )
                    )
                ).scalars()
            )
        return [(token, condition) for token, condition in pairs if condition not in resolved]

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

    async def price_cycle(
        self,
        market_sessions: async_sessionmaker[AsyncSession] | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        stats = self.stats["A"]
        before = stats.copy()
        stats["cycles"] += 1
        valid_tokens: set[str] = set()
        pairs: list[tuple[str, str]] = []
        mkt = market_sessions or self.market_sessions
        wallet_sess = (
            mkt
            if market_sessions is not None and self.sessions is self.market_sessions
            else self.sessions
        )
        try:
            try:
                async with mkt() as session:
                    async with session.begin():
                        await ensure_price_partitions(session)
                        await archive_old_prices(session, get_settings().price_raw_retention_days)
            except Exception as exc:
                stats["errors"] += 1
                log_event("price_maintenance_error", error=f"{type(exc).__name__}: {exc}")
            pairs = await self.open_tokens(market_sessions=mkt)
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
                            async with mkt() as session:
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
            async with mkt() as session:
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
            async with wallet_sess() as session:
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
                await self.notify_wallet(
                    wallet, "price_update", "price_cycle", session_factory=wallet_sess
                )
        result = {
            "seconds": round(time.perf_counter() - started, 3),
            **{
                k: stats[k] - before[k] for k in ("live_quote", "last_trade", "not_found", "errors")
            },
        }
        log_event("loop_a", **result)
        return result

    async def resolution_cycle(
        self,
        initial: bool = False,
        urgent_only: bool = False,
        market_sessions: async_sessionmaker[AsyncSession] | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        stats = self.stats["B"]
        before = stats.copy()
        stats["cycles"] += 1
        mkt = market_sessions or self.market_sessions
        db_write_lock = asyncio.Lock()
        try:
            urgent = self.urgent_resolutions.copy()
            self.urgent_resolutions.difference_update(urgent)
            self.resolution_wakeup.clear()
            pairs = (
                []
                if urgent_only
                else await self.open_tokens(include_resolved=initial, market_sessions=mkt)
            )

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
                        async with db_write_lock:
                            async with mkt() as session:
                                async with session.begin():
                                    saved = await save_resolution(session, record)
                        stats["upserted" if saved else "stale_ignored"] += 1
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
            "stale_ignored": stats["stale_ignored"] - before["stale_ignored"],
            "errors": stats["errors"] - before["errors"],
            "initial": initial,
        }
        log_event("loop_b", **result)
        return result

    async def wallet_cycle(self, wallet: str, connection=None) -> dict[str, Any]:
        started = time.perf_counter()
        stats = self.stats["C"]
        stats["cycles"] += 1
        async with AsyncExitStack() as stack:
            lock_conn = connection or await stack.enter_async_context(self.engine.connect())
            locked = await lock_conn.scalar(
                text("select pg_try_advisory_lock(hashtextextended(:w, 0))"), {"w": wallet.lower()}
            )
            if not locked:
                stats["skipped_lock"] += 1
                result = {"wallet": wallet, "skipped_lock": True}
                log_event("loop_c", **result)
                return result
            lock_driver = (await lock_conn.get_raw_connection()).driver_connection

            def require_lock_connection(conn, cursor, statement, parameters, context, executemany):
                if conn.connection.driver_connection is not lock_driver:
                    raise RuntimeError("Wallet lock connection changed during processing")

            event.listen(
                lock_conn.sync_connection, "before_cursor_execute", require_lock_connection
            )
            try:
                # The advisory lock and wallet writes must use the same database
                # connection. Losing it then aborts the active transaction as well
                # as releasing ownership to another worker.
                await lock_conn.commit()
                wallet_sessions = async_sessionmaker(
                    bind=lock_conn, class_=AsyncSession, expire_on_commit=False
                )
                counted = CountingSource(self.source)
                poller = LivePoller(wallet, counted, wallet_sessions)
                inserted = await poller._execute_poll_cycle(update_last_success=False)
                if counted.error or not counted.final:
                    raise RuntimeError(f"poll incomplete: {counted.error}")
                processed = 0
                while True:
                    async with wallet_sessions() as session:
                        async with session.begin():
                            executor = (
                                self.accounting_executor
                                if wallet.lower() in self.parallel_wallets
                                else None
                            )
                            summary = await AccountingProjector(
                                executor=executor,
                                timeout=get_settings().accounting_parallel_timeout_seconds,
                                condition_replay=(
                                    executor is not None
                                    or wallet.lower() in self.condition_replay_wallets
                                ),
                            ).catch_up_wallet(session, wallet)
                    processed += summary.events_processed
                    if summary.events_processed == 0 or summary.is_recompute:
                        break
                now = datetime.now(timezone.utc)
                async with wallet_sessions() as session:
                    async with session.begin():
                        cursor = await session.get(
                            IngestionCursor, (wallet.lower(), "data_api"), with_for_update=True
                        )
                        if cursor is None:
                            raise RuntimeError("missing data_api cursor after poll")
                        cursor.last_success_at = now
                if processed:
                    await self.notify_wallet(
                        wallet, "pnl_update", "projector", session_factory=wallet_sessions
                    )
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
                event.remove(
                    lock_conn.sync_connection, "before_cursor_execute", require_lock_connection
                )
                # Cancellation can invalidate the connection. Its session locks
                # are then already gone; never unlock on a replacement session.
                if (
                    not lock_conn.invalidated
                    and (await lock_conn.get_raw_connection()).driver_connection is lock_driver
                ):
                    await lock_conn.execute(
                        text("select pg_advisory_unlock(hashtextextended(:w, 0))"),
                        {"w": wallet.lower()},
                    )

    async def poll_cycle(self) -> None:
        try:
            wallets = await self.active_wallets()
            if not wallets:
                raise RuntimeError("no active tracked wallets")
            buckets: dict[int, list[str]] = {}
            for wallet in wallets:
                buckets.setdefault(wallet_bucket(wallet), []).append(wallet)

            async def process_bucket(bucket: int, members: list[str]) -> None:
                async with self.wallet_limit, self.engine.connect() as connection:
                    claimed = await connection.scalar(
                        text("SELECT pg_try_advisory_lock(:namespace, :bucket)"),
                        {"namespace": BUCKET_LOCK_NAMESPACE, "bucket": bucket},
                    )
                    if not claimed:
                        return
                    self.held_buckets.add(bucket)
                    bucket_driver = (await connection.get_raw_connection()).driver_connection

                    def require_bucket_connection(
                        conn, cursor, statement, parameters, context, executemany
                    ):
                        if conn.connection.driver_connection is not bucket_driver:
                            raise RuntimeError("Bucket lock connection changed during processing")

                    event.listen(
                        connection.sync_connection,
                        "before_cursor_execute",
                        require_bucket_connection,
                    )
                    try:
                        await connection.commit()
                        for member in members:
                            if (
                                connection.invalidated
                                or (await connection.get_raw_connection()).driver_connection
                                is not bucket_driver
                            ):
                                break
                            try:
                                await asyncio.wait_for(
                                    self.wallet_cycle(member, connection),
                                    timeout=get_settings().wallet_cycle_timeout_seconds,
                                )
                            except TimeoutError:
                                self.stats["C"]["errors"] += 1
                                log_event("wallet_cycle_timeout", wallet=member)
                                # Cancellation unwinds session.begin() and releases
                                # the wallet lock. A lost connection also loses
                                # the bucket lock, so stop processing this bucket.
                                if connection.invalidated:
                                    break
                                try:
                                    await connection.rollback()
                                    # rollback() can be a no-op without an active
                                    # transaction; verify the lock session is live.
                                    await connection.execute(text("SELECT 1"))
                                except Exception as exc:
                                    log_event(
                                        "bucket_connection_lost", bucket=bucket, error=str(exc)
                                    )
                                    break
                    finally:
                        self.held_buckets.discard(bucket)
                        event.remove(
                            connection.sync_connection,
                            "before_cursor_execute",
                            require_bucket_connection,
                        )
                        if (
                            not connection.invalidated
                            and (await connection.get_raw_connection()).driver_connection
                            is bucket_driver
                        ):
                            await connection.execute(
                                text("SELECT pg_advisory_unlock(:namespace, :bucket)"),
                                {"namespace": BUCKET_LOCK_NAMESPACE, "bucket": bucket},
                            )

            await asyncio.gather(
                *(process_bucket(bucket, members) for bucket, members in buckets.items())
            )
        except Exception as exc:
            self.stats["C"]["errors"] += 1
            log_event("poll_cycle_error", error=f"{type(exc).__name__}: {exc}")

    async def reconcile_cycle(self, session_factory=None) -> None:
        """Loop D: on-chain reconciliation.

        ``session_factory`` is the bound connection session factory provided by
        ``singleton()``; when given, we reuse that connection so the loop never
        opens an extra connection from the pool (critical under pool_size=2).
        Falls back to ``self.sessions`` when called outside the singleton context.
        """
        sessions = session_factory or self.sessions
        logger.info("loop_d_start timestamp=%s", datetime.now(timezone.utc).isoformat())
        stats = self.stats["D"]
        stats["cycles"] += 1
        client = PolygonRpcClient(endpoints=["https://polygon.drpc.org/"], max_retries=1)
        for wallet in await self.active_wallets(session_factory=sessions):
            started = datetime.now(timezone.utc)
            block_number = None
            block_time = None
            summary = None
            error = None
            try:
                block_number = await client.get_latest_block_number() - 32
                block = await client.call("eth_getBlockByNumber", [hex(block_number), False])
                block_time = datetime.fromtimestamp(int(block["timestamp"], 16), tz=timezone.utc)
                scan = await scan_tail(wallet, block_number, self.sessions, self.engine)
                summary = await reconcile_wallet(
                    client, wallet, wallet, block_number, block_time, self.sessions
                )
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
            async with sessions() as session:
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


async def _start_health_server(
    port: int, instance_id: str, stop_event: asyncio.Event, workers: list[Worker]
) -> asyncio.AbstractServer:
    """Minimal DB-free HTTP /health & /ready server for PaaS Web Service hosting."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=5.0)
            line = raw.decode("latin-1", errors="replace").strip()
            parts = line.split()
            path = parts[1].split("?", 1)[0] if len(parts) >= 2 else "/"
            draining = stop_event.is_set()
            if path in ("/health", "/ready", "/"):
                if path == "/ready" and draining:
                    code, reason = 503, "Service Unavailable"
                    body_dict = {"status": "draining", "instance_id": instance_id}
                else:
                    code, reason = 200, "OK"
                    held = sorted({b for w in workers for b in w.held_buckets})
                    body_dict = {
                        "status": "ok" if not draining else "draining",
                        "role": "worker",
                        "instance_id": instance_id,
                        "buckets_held": held,
                    }
            else:
                code, reason = 404, "Not Found"
                body_dict = {"detail": "Not Found"}
            body = json.dumps(body_dict).encode("utf-8")
            response = (
                f"HTTP/1.1 {code} {reason}\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"X-Instance-Id: {instance_id}\r\n"
                f"Connection: close\r\n\r\n"
            ).encode("latin-1") + body
            writer.write(response)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    return await asyncio.start_server(handle, "0.0.0.0", port)


async def _worker_heartbeat_loop(
    instance_id: str,
    started_at: datetime,
    workers: list[Worker],
    stop_event: asyncio.Event,
) -> None:
    interval = get_settings().fleet_heartbeat_seconds
    while not stop_event.is_set():
        held = sorted({b for w in workers for b in w.held_buckets})
        loop_cycles = {
            name: sum(w.stats[name]["cycles"] for w in workers) for name in ("A", "B", "C", "D")
        }
        loop_errors = {
            name: sum(w.stats[name]["errors"] for w in workers) for name in ("A", "B", "C", "D")
        }
        await upsert_heartbeat(
            instance_id=instance_id,
            role="worker",
            started_at=started_at,
            shard_id=None if len(workers) > 1 else workers[0].shard_id,
            ws_connections=0,
            listener_ok=None,
            buckets_held=held,
            meta={
                "sharded": len(workers) > 1,
                "loop_cycles": loop_cycles,
                "loop_errors": loop_errors,
            },
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def run_worker(
    duration: float | None = None,
    once: bool = False,
    price_cycles: int = 0,
    health_port: int | None = None,
) -> None:
    if secondary_engine is not None and secondary_session_factory is not None:
        workers = [
            Worker(shard_id=0),
            Worker(
                session_factory=secondary_session_factory,
                db_engine=secondary_engine,
                market_sessions=async_session_factory,
                shard_id=1,
            ),
        ]
    else:
        workers = [Worker()]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    singleton_locks = {id(worker): asyncio.Lock() for worker in workers}
    instance_id = get_settings().resolve_instance_id()
    started_at = datetime.now(timezone.utc)
    health_server: asyncio.AbstractServer | None = None
    if health_port is not None:
        health_server = await _start_health_server(health_port, instance_id, stop, workers)
        log_event("worker_health_server_started", port=health_port, instance_id=instance_id)

    async def singleton(worker: Worker, name: str, action) -> bool:
        lock_engine = engine if name in ("A", "B") else worker.engine
        key = {"A": 100, "B": 102, "D": 104}[name] + (worker.shard_id or 0)
        async with singleton_locks[id(worker)]:
            async with lock_engine.connect() as connection:
                claimed = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:namespace, :key)"),
                    {"namespace": BUCKET_LOCK_NAMESPACE, "key": key},
                )
                if not claimed:
                    await asyncio.sleep(1)
                    return False
                bound_sessions = async_sessionmaker(
                    bind=connection, class_=AsyncSession, expire_on_commit=False
                )
                try:
                    await connection.commit()
                    await action(bound_sessions)
                    return True
                finally:
                    if not connection.invalidated:
                        await connection.execute(
                            text("SELECT pg_advisory_unlock(:namespace, :key)"),
                            {"namespace": BUCKET_LOCK_NAMESPACE, "key": key},
                        )

    async def periodic(worker: Worker, name: str, interval: float) -> None:
        initial = True
        next_full_resolution = 0.0
        while not stop.is_set():
            started = time.monotonic()
            try:
                if name == "A":
                    await singleton(
                        worker,
                        name,
                        lambda bound: worker.price_cycle(market_sessions=bound),
                    )
                elif name == "B":
                    full = initial or started >= next_full_resolution
                    completed = await singleton(
                        worker,
                        name,
                        lambda bound: worker.resolution_cycle(
                            initial=initial, urgent_only=not full, market_sessions=bound
                        ),
                    )
                    if full and completed:
                        next_full_resolution = time.monotonic() + interval
                elif name == "C":
                    await worker.poll_cycle()
                else:
                    await singleton(
                        worker,
                        name,
                        lambda bound: worker.reconcile_cycle(session_factory=bound),
                    )
            except Exception as exc:
                worker.stats[name]["errors"] += 1
                log_event("worker_cycle_error", shard=worker.shard_id, loop=name, error=str(exc))
                if name == "B":
                    next_full_resolution = time.monotonic() + min(interval, 10)
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
                await asyncio.gather(
                    *(
                        singleton(
                            worker,
                            "A",
                            lambda bound, w=worker: w.price_cycle(market_sessions=bound),
                        )
                        for worker in workers
                    )
                )
        elif once:
            for worker in workers:
                await singleton(
                    worker,
                    "B",
                    lambda bound, w=worker: w.resolution_cycle(initial=True, market_sessions=bound),
                )
                await singleton(
                    worker, "A", lambda bound, w=worker: w.price_cycle(market_sessions=bound)
                )
                await worker.poll_cycle()
        else:
            tasks = [
                asyncio.create_task(periodic(worker, name, interval))
                for worker in workers
                for name, interval in (("A", 15), ("B", 300), ("C", 60), ("D", 600))
            ]
            hb_task = asyncio.create_task(
                _worker_heartbeat_loop(instance_id, started_at, workers, stop)
            )
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
                await asyncio.gather(*tasks, hb_task, return_exceptions=True)
                await delete_heartbeat(instance_id)
    finally:
        if health_server is not None:
            health_server.close()
            await health_server.wait_closed()
        for worker in workers:
            log_event(
                "summary",
                shard=worker.shard_id,
                seconds=round(time.monotonic() - started, 3),
                loops={name: dict(stats) for name, stats in worker.stats.items()},
                rate_headers=worker.rate_headers,
            )
            await worker.close()
        await engine.dispose()
        if secondary_engine is not None:
            await secondary_engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--price-cycles", type=int, default=0)
    parser.add_argument("--health-port", type=int, default=None)
    args = parser.parse_args()
    asyncio.run(run_worker(args.duration, args.once, args.price_cycles, args.health_port))


if __name__ == "__main__":
    main()
