import asyncio

import httpx
import pytest

from app.ingestion.data_api import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    DataApiSource,
    InvalidCursorError,
)


@pytest.mark.asyncio
async def test_429_with_retry_after_is_obeyed(monkeypatch):
    """
    Test that HTTP 429 rate limit is obeyed with Retry-After header.
    """
    sleep_calls = []

    async def mock_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    monkeypatch.setattr("asyncio.sleep", mock_sleep)

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "3.5"})
        return httpx.Response(
            200,
            json={
                "data": [{"type": "TRADE", "timestamp": 1700000000, "size": 10}],
                "pagination": {"has_more": False, "next_cursor": None},
            },
        )

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = DataApiSource(client=mock_client)

    res = await source.fetch_page(wallet="0x1111111111111111111111111111111111111111")
    assert calls == 2
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == 3.5
    assert source.metrics.rate_limits_429 == 1
    assert source.metrics.retries_total == 1
    assert len(res.events) == 1
    assert res.events[0]["type"] == "TRADE"


@pytest.mark.asyncio
async def test_five_failures_open_circuit_breaker_and_probe_closes_it():
    """
    Test that 5 consecutive failures open the circuit breaker,
    subsequent calls fast-fail with CircuitBreakerOpenError,
    and after recovery timeout a successful probe closes it.
    """
    # Fast recovery timeout for testing
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=0.05)

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls <= 5:
            # 5 consecutive 500 errors
            return httpx.Response(500, json={"error": "server error"})
        # Successful probe
        return httpx.Response(
            200,
            json={
                "data": [{"type": "TRADE", "timestamp": 1700000001, "size": 20}],
                "pagination": {"has_more": False, "next_cursor": None},
            },
        )

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = DataApiSource(client=mock_client, circuit_breaker=cb, max_retries=0)

    # Trigger 5 failures
    for _ in range(5):
        with pytest.raises(httpx.HTTPStatusError):
            await source.fetch_page(wallet="0x2222222222222222222222222222222222222222")

    # Circuit breaker must now be OPEN
    assert cb.state == "OPEN"
    assert cb.consecutive_failures == 5

    # A request made immediately must fast-fail with CircuitBreakerOpenError
    with pytest.raises(CircuitBreakerOpenError):
        await source.fetch_page(wallet="0x2222222222222222222222222222222222222222")

    # Wait for recovery timeout
    await asyncio.sleep(0.06)

    # Next request probes the upstream and succeeds
    res = await source.fetch_page(wallet="0x2222222222222222222222222222222222222222")
    assert cb.state == "CLOSED"
    assert cb.consecutive_failures == 0
    assert len(res.events) == 1
    assert res.events[0]["size"] == 20


@pytest.mark.asyncio
async def test_invalid_cursor_raises_invalid_cursor_error():
    """
    Test that HTTP 400 with cursor raises InvalidCursorError rather than retrying.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"error": "invalid cursor", "code": "invalid_request", "retryable": False},
        )

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    source = DataApiSource(client=mock_client)

    with pytest.raises(InvalidCursorError) as exc_info:
        await source.fetch_page(
            wallet="0x3333333333333333333333333333333333333333",
            cursor="corrupted_garbage_token",
        )

    assert "rejected cursor" in str(exc_info.value)
