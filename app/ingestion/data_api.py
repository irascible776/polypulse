import asyncio
import logging
import random
import time
from typing import Any

import httpx

from app.ingestion.identifiers import parse_json_with_decimal
from app.ingestion.source import EventSource, IngestionMetrics, PageResult

logger = logging.getLogger("data_api_source")


class CircuitBreakerOpenError(Exception):
    """Raised when request is attempted while the circuit breaker is OPEN."""

    pass


class InvalidCursorError(Exception):
    """Raised when Data API responds with HTTP 400 indicating an invalid cursor."""

    pass


class CircuitBreaker:
    """
    Circuit breaker with CLOSED, OPEN, and HALF_OPEN states.
    Opens after failure_threshold consecutive failures.
    Probes after recovery_timeout seconds. A successful probe closes it.
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.consecutive_failures = 0
        self.last_state_change = time.time()
        self._probe_lock = asyncio.Lock()

    def record_success(self) -> None:
        self.consecutive_failures = 0
        if self.state != "CLOSED":
            logger.info("Circuit breaker closed after successful request.")
            self.state = "CLOSED"
            self.last_state_change = time.time()

    def record_failure(self) -> None:
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.failure_threshold and self.state == "CLOSED":
            logger.warning(
                "Circuit breaker tripped to OPEN after %d consecutive failures.",
                self.consecutive_failures,
            )
            self.state = "OPEN"
            self.last_state_change = time.time()
        elif self.state == "HALF_OPEN":
            logger.warning("Probe failed; circuit breaker returning to OPEN.")
            self.state = "OPEN"
            self.last_state_change = time.time()

    def can_attempt(self) -> bool:
        now = time.time()
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if now - self.last_state_change >= self.recovery_timeout:
                logger.info("Circuit breaker recovery timeout elapsed; entering HALF_OPEN.")
                self.state = "HALF_OPEN"
                self.last_state_change = now
                return True
            return False
        if self.state == "HALF_OPEN":
            # In half-open, allow only probe attempts
            return True
        return False


class DataApiSource(EventSource):
    """
    Client for Polymarket Data API v2 (/v2/activity and /v2/status).
    Features:
    - Async HTTPX client with connection pooling and timeouts
    - Per-wallet request serialization (one request at a time per wallet)
    - Global concurrency limit via semaphore
    - Retries with exponential backoff and jitter on 5xx, timeouts
    - Strict obedience of HTTP 429 Retry-After headers
    - Circuit breaker protecting against upstream downtime
    - Metric collection for requests, retries, 429s, failures, and latencies
    """

    def __init__(
        self,
        base_url: str = "https://data-api.polymarket.com",
        timeout_seconds: float = 15.0,
        global_concurrency: int = 5,
        circuit_breaker: CircuitBreaker | None = None,
        max_retries: int = 3,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_seconds
        self.global_semaphore = asyncio.Semaphore(global_concurrency)
        self.wallet_locks: dict[str, asyncio.Lock] = {}
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.max_retries = max_retries
        self.metrics = IngestionMetrics()
        self._client = client

    @property
    def source_tag(self) -> str:
        return "data_api"

    def _get_wallet_lock(self, wallet: str) -> asyncio.Lock:
        w = wallet.lower()
        if w not in self.wallet_locks:
            self.wallet_locks[w] = asyncio.Lock()
        return self.wallet_locks[w]

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def fetch_page(
        self,
        wallet: str,
        cursor: str | None = None,
        start: int | None = None,
        limit: int = 500,
        sort_direction: str = "ASC",
    ) -> PageResult:
        """
        Fetch a page of activity events for a wallet.
        Enforces per-wallet serialization, global semaphore, circuit breaker, and retry logic.
        """
        wallet_lock = self._get_wallet_lock(wallet)

        async with wallet_lock:
            async with self.global_semaphore:
                return await self._execute_fetch_page_with_retries(
                    wallet=wallet,
                    cursor=cursor,
                    start=start,
                    limit=limit,
                    sort_direction=sort_direction,
                )

    async def _execute_fetch_page_with_retries(
        self,
        wallet: str,
        cursor: str | None,
        start: int | None,
        limit: int,
        sort_direction: str,
    ) -> PageResult:
        if not self.circuit_breaker.can_attempt():
            self.metrics.record_circuit_trip()
            raise CircuitBreakerOpenError(
                f"Circuit breaker is OPEN. Upstream Data API unavailable. "
                f"Consecutive failures: {self.circuit_breaker.consecutive_failures}"
            )

        client = await self._get_client()
        url = f"{self.base_url}/v2/activity"

        params: dict[str, str | int] = {
            "user": wallet,
            "limit": limit,
            "sort_direction": sort_direction,
        }
        if cursor:
            params["cursor"] = cursor
        elif start is not None:
            params["start"] = start

        attempt = 0
        while True:
            t0 = time.perf_counter()
            try:
                resp = await client.get(url, params=params)
                latency_ms = (time.perf_counter() - t0) * 1000.0

                if resp.status_code == 200:
                    self.metrics.record_request(latency_ms, success=True)
                    self.circuit_breaker.record_success()

                    parsed = parse_json_with_decimal(resp.text)
                    events = parsed.get("data", [])
                    pagination = parsed.get("pagination", {})
                    next_cursor = pagination.get("next_cursor")
                    has_more = bool(pagination.get("has_more", False))

                    return PageResult(
                        events=events,
                        next_cursor=next_cursor,
                        has_more=has_more,
                        status_code=200,
                        raw_response=parsed,
                    )

                if resp.status_code == 429:
                    self.metrics.record_429()
                    self.metrics.record_request(latency_ms, success=False)
                    retry_after_str = resp.headers.get("Retry-After", "5")
                    try:
                        retry_after = float(retry_after_str)
                    except ValueError:
                        retry_after = 5.0

                    logger.warning(
                        "Data API rate limit (429) hit for wallet %s. Retry-After: %.1fs",
                        wallet,
                        retry_after,
                    )
                    attempt += 1
                    if attempt > self.max_retries:
                        self.circuit_breaker.record_failure()
                        resp.raise_for_status()
                    self.metrics.record_retry()
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status_code == 400 and cursor:
                    # Invalid cursor detected
                    self.metrics.record_request(latency_ms, success=False)
                    logger.warning(
                        "Data API returned 400 Bad Request on cursor fetch: %s", resp.text
                    )
                    raise InvalidCursorError(f"Data API rejected cursor '{cursor}': {resp.text}")

                if resp.status_code >= 500:
                    self.metrics.record_request(latency_ms, success=False)
                    attempt += 1
                    if attempt > self.max_retries:
                        self.circuit_breaker.record_failure()
                        resp.raise_for_status()
                    backoff = (0.5 * (2**attempt)) + random.uniform(0.0, 0.2)
                    self.metrics.record_retry()
                    logger.warning(
                        "Data API 5xx (%d). Retrying in %.2fs (attempt %d/%d)",
                        resp.status_code,
                        backoff,
                        attempt,
                        self.max_retries,
                    )
                    await asyncio.sleep(backoff)
                    continue

                # Unhandled 4xx error
                self.metrics.record_request(latency_ms, success=False)
                resp.raise_for_status()

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                latency_ms = (time.perf_counter() - t0) * 1000.0
                self.metrics.record_request(latency_ms, success=False)
                attempt += 1
                if attempt > self.max_retries:
                    self.circuit_breaker.record_failure()
                    raise
                backoff = (0.5 * (2**attempt)) + random.uniform(0.0, 0.2)
                self.metrics.record_retry()
                logger.warning(
                    "Data API network/timeout error (%s). Retrying in %.2fs (attempt %d/%d)",
                    type(exc).__name__,
                    backoff,
                    attempt,
                    self.max_retries,
                )
                await asyncio.sleep(backoff)

    async def fetch_status(self) -> dict[str, Any] | None:
        """Fetch Polymarket Data API freshness status (/v2/status)."""
        client = await self._get_client()
        url = f"{self.base_url}/v2/status"
        try:
            resp = await client.get(url)
            if resp.status_code == 200:
                return parse_json_with_decimal(resp.text)
            logger.warning("GET /v2/status returned status %d", resp.status_code)
            return None
        except Exception as exc:
            logger.warning("Failed to fetch /v2/status: %s", exc)
            return None

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
