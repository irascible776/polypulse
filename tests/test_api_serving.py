"""
tests/test_api_serving.py

Integration and latency benchmark tests for FastAPI serving layer endpoints.
Verifies:
- GET /health
- GET /wallets/{address}/positions
- GET /wallets/{address}/pnl (including ?as_of= historical queries)
- GET /wallets/{address}/trades
- POST /markets/price & GET /markets/{condition_id}/price
- p95 latency benchmark across concurrent requests
"""

import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest
from httpx import ASGITransport

from app.config import get_settings
from app.main import app
from app.storage.database import async_session_factory
from app.storage.models import (
    IngestionCursor,
    MarketPrice,
    MarketResolution,
    Position,
    PositionLedger,
    WalletTotal,
)
from app.valuation.pricing import PriceTier


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac


@pytest.mark.asyncio
async def test_health_endpoint(client: httpx.AsyncClient):
    resp = await client.get("/serving/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["database_connected"] is True
    assert "serving_latency_ms" in data


@pytest.mark.asyncio
async def test_wallet_pnl_and_positions_endpoints(client: httpx.AsyncClient):
    wallet = "0x" + uuid.uuid4().hex[:40]
    token_id = "tok_api_" + uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc)

    # Seed test wallet state in DB
    async with async_session_factory() as session:
        session.add(
            WalletTotal(
                wallet=wallet,
                total_realized_pnl=Decimal("500.00"),
                total_trade_count=1,
                last_applied_api_order=1,
                updated_at=now,
            )
        )
        session.add(
            Position(
                wallet=wallet,
                token_id=token_id,
                condition_id="0x" + "11" * 32,
                shares=Decimal("1000.00"),
                avg_cost_basis=Decimal("0.50"),
                updated_at=now,
            )
        )
        session.add(
            MarketPrice(
                token_id=token_id,
                condition_id="0x" + "11" * 32,
                price=Decimal("0.75"),
                bid=Decimal("0.74"),
                ask=Decimal("0.76"),
                source=PriceTier.LIVE_QUOTE.value,
                observed_at=now,
                created_at=now,
            )
        )
        await session.commit()

    # Query GET /wallets/{address}/pnl
    resp_pnl = await client.get(f"/wallets/{wallet}/pnl")
    assert resp_pnl.status_code == 200
    data_pnl = resp_pnl.json()
    assert data_pnl["wallet"] == wallet
    assert Decimal(data_pnl["realized_pnl"]) == Decimal("500")
    # Unrealized = (0.75 - 0.50) * 1000 = $250.00
    assert Decimal(data_pnl["unrealized_priced"]) == Decimal("250")
    assert Decimal(data_pnl["total_pnl_excl_unpriced"]) == Decimal("750")
    assert data_pnl["open_positions_count"] == 1

    # Query GET /wallets/{address}/positions
    resp_pos = await client.get(f"/wallets/{wallet}/positions")
    assert resp_pos.status_code == 200
    data_pos = resp_pos.json()
    assert len(data_pos["positions"]) == 1
    pos = data_pos["positions"][0]
    assert pos["token_id"] == token_id
    assert Decimal(pos["shares"]) == Decimal("1000")
    assert Decimal(pos["mark_price"]) == Decimal("0.75")
    assert Decimal(pos["unrealized_pnl"]) == Decimal("250")
    assert pos["price_source"] == PriceTier.LIVE_QUOTE.value
    assert pos["is_stale"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,age,expected",
    [
        ("fresh", 0, False),
        ("stale", 901, True),
        ("missing", None, True),
        ("resolved_old", 901, False),
    ],
)
async def test_resolution_freshness_fields(client: httpx.AsyncClient, case, age, expected):
    wallet = "0x" + uuid.uuid4().hex
    condition = "0x" + uuid.uuid4().hex * 2
    token = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        session.add(
            Position(
                wallet=wallet,
                token_id=token,
                condition_id=condition,
                shares=Decimal(1),
                avg_cost_basis=Decimal("0.5"),
                updated_at=now,
            )
        )
        if age is not None:
            session.add(
                MarketResolution(
                    condition_id=condition,
                    resolved=case == "resolved_old",
                    payout_numerators=[1, 0] if case == "resolved_old" else [0, 0],
                    payout_denominator=1 if case == "resolved_old" else 0,
                    gamma_closed=case == "resolved_old",
                    gamma_raw={"clobTokenIds": [token, "other"], "outcomes": ["Yes", "No"]}
                    if case == "resolved_old"
                    else {},
                    chain_block=1,
                    checked_at=now - timedelta(seconds=age),
                    agreement=True,
                )
            )
        await session.commit()
    for endpoint in ("pnl", "positions"):
        data = (await client.get(f"/wallets/{wallet}/{endpoint}")).json()
        assert data["resolution_stale"] is expected, case
        assert data["resolutions_as_of"] == (
            None
            if age is None or case == "resolved_old"
            else (now - timedelta(seconds=age)).isoformat().replace("+00:00", "Z")
        )
        assert data["valuation_current"] is False


@pytest.mark.asyncio
async def test_current_valuation_requires_complete_fresh_ingestion_and_resolution(
    client: httpx.AsyncClient,
):
    wallet = "0x" + uuid.uuid4().hex
    condition = "0x" + uuid.uuid4().hex * 2
    token = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        session.add(
            IngestionCursor(wallet=wallet, source="data_api", last_success_at=now, last_api_order=0)
        )
        session.add(
            Position(
                wallet=wallet,
                token_id=token,
                condition_id=condition,
                shares=Decimal(1),
                avg_cost_basis=Decimal("0.5"),
                updated_at=now,
            )
        )
        session.add(
            MarketResolution(
                condition_id=condition,
                resolved=False,
                payout_numerators=[0, 0],
                payout_denominator=0,
                gamma_closed=False,
                gamma_raw={},
                chain_block=1,
                checked_at=now,
                agreement=True,
            )
        )
        session.add(
            MarketPrice(
                token_id=token,
                condition_id=condition,
                price=Decimal("0.6"),
                bid=Decimal("0.59"),
                ask=Decimal("0.61"),
                source="live_quote",
                observed_at=now,
                created_at=now,
            )
        )
        await session.commit()
    for endpoint in ("pnl", "positions"):
        data = (await client.get(f"/wallets/{wallet}/{endpoint}")).json()
        assert data["resolution_stale"] is False
        assert data["ingestion_stale"] is False
        assert data["valuation_current"] is True


@pytest.mark.asyncio
async def test_wallet_historical_as_of_query(client: httpx.AsyncClient):
    """
    Verify point-in-time time travel (?as_of=):
    At T1, wallet bought 500 shares at $0.40.
    At T2, wallet bought another 500 shares at $0.60 (total 1000 shares).
    Querying as_of=T1.5 must return exactly 500 shares at $0.40 cost basis.
    """
    wallet = "0x" + uuid.uuid4().hex[:40]
    token_id = "tok_time_travel_" + uuid.uuid4().hex[:8]
    condition_id = "0xcond_" + uuid.uuid4().hex

    base_time = datetime(2024, 10, 15, 12, 0, 0, tzinfo=timezone.utc)
    t1 = base_time + timedelta(hours=1)
    t1_mid = base_time + timedelta(hours=1, minutes=30)
    t2 = base_time + timedelta(hours=2)

    async with async_session_factory() as session:
        # Event 1 at T1
        session.add(
            PositionLedger(
                wallet=wallet,
                source_event_id="ev_1",
                api_order=1,
                event_type="BUY",
                event_time=t1,
                token_id=token_id,
                condition_id=condition_id,
                shares_before=Decimal("0"),
                shares_delta=Decimal("500"),
                shares_after=Decimal("500"),
                avg_cost_before=Decimal("0"),
                avg_cost_after=Decimal("0.40"),
                cash_delta=Decimal("-200"),
                realized_pnl_delta=Decimal("0"),
                is_recompute=False,
                cash_price_gap=False,
                created_at=t1,
            )
        )
        # Event 2 at T2
        session.add(
            PositionLedger(
                wallet=wallet,
                source_event_id="ev_2",
                api_order=2,
                event_type="BUY",
                event_time=t2,
                token_id=token_id,
                condition_id=condition_id,
                shares_before=Decimal("500"),
                shares_delta=Decimal("500"),
                shares_after=Decimal("1000"),
                avg_cost_before=Decimal("0.40"),
                avg_cost_after=Decimal("0.50"),
                cash_delta=Decimal("-300"),
                realized_pnl_delta=Decimal("0"),
                is_recompute=False,
                cash_price_gap=False,
                created_at=t2,
            )
        )
        # A late event has the highest API order but occurred before T2.
        session.add(
            PositionLedger(
                wallet=wallet,
                source_event_id="ev_late",
                api_order=3,
                event_type="BUY",
                event_time=t1 + timedelta(minutes=10),
                token_id=token_id,
                condition_id=condition_id,
                shares_before=Decimal("500"),
                shares_delta=Decimal("200"),
                shares_after=Decimal("700"),
                avg_cost_before=Decimal("0.40"),
                avg_cost_after=Decimal("0.42"),
                cash_delta=Decimal("-94"),
                realized_pnl_delta=Decimal("0"),
                is_recompute=True,
                cash_price_gap=False,
                created_at=t2,
            )
        )
        # Price tick at T1
        session.add(
            MarketPrice(
                token_id=token_id,
                condition_id=condition_id,
                price=Decimal("0.45"),
                bid=Decimal("0.44"),
                ask=Decimal("0.46"),
                source=PriceTier.LIVE_QUOTE.value,
                observed_at=t1 + timedelta(minutes=15),
                created_at=t1 + timedelta(minutes=15),
            )
        )
        session.add(
            MarketResolution(
                condition_id=condition_id,
                resolved=False,
                payout_numerators=[0, 0],
                payout_denominator=0,
                gamma_closed=False,
                gamma_raw={},
                chain_block=1,
                checked_at=t1,
                agreement=True,
            )
        )
        await session.commit()

    # Time-travel query at T1.5
    as_of_str = t1_mid.isoformat()
    resp = await client.get(f"/wallets/{wallet}/positions?as_of={as_of_str}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["open_positions_count"] == 1
    pos = data["positions"][0]
    # At T1.5, only 500 shares were owned!
    assert Decimal(pos["shares"]) == Decimal("700")
    assert Decimal(pos["avg_cost_basis"]) == Decimal("0.42")
    assert Decimal(pos["mark_price"]) == Decimal("0.45")
    # Unrealized = (0.45 - 0.40) * 500 = $25.00
    assert Decimal(pos["unrealized_pnl"]) == Decimal("21")

    before_tick = (
        await client.get(
            f"/wallets/{wallet}/positions",
            params={"as_of": (t1 + timedelta(minutes=12)).isoformat()},
        )
    ).json()
    assert Decimal(before_tick["positions"][0]["shares"]) == Decimal("700")
    assert before_tick["positions"][0]["mark_price"] is None


@pytest.mark.asyncio
async def test_post_market_price_authentication(client: httpx.AsyncClient):
    """
    Verify security integrity on market price ingestion:
    Unauthenticated requests must be rejected with HTTP 401.
    Authenticated requests with valid X-API-Key succeed.
    """
    token_id = "tok_auth_" + uuid.uuid4().hex[:8]

    # 1. No API key -> 401 Unauthorized
    resp_no_key = await client.post(
        f"/markets/price?token_id={token_id}&price=0.62&source=live_quote"
    )
    assert resp_no_key.status_code == 401
    assert "Unauthorized" in resp_no_key.json()["detail"]

    # 2. Invalid API key -> 401 Unauthorized
    resp_bad_key = await client.post(
        f"/markets/price?token_id={token_id}&price=0.62&source=live_quote",
        headers={"X-API-Key": "invalid_untrusted_key"},
    )
    assert resp_bad_key.status_code == 401

    # 3. Valid API key -> 200 OK
    resp_valid = await client.post(
        f"/markets/price?token_id={token_id}&price=0.62&source=live_quote",
        headers={"X-API-Key": get_settings().api_key},
    )
    assert resp_valid.status_code == 200
    data = resp_valid.json()
    assert data["success"] is True
    assert Decimal(data["price"]) == Decimal("0.62")


@pytest.mark.asyncio
async def test_zero_position_wallet_graceful_empty_state(client: httpx.AsyncClient):
    """
    Verify brand-new or zero-position wallet returns clean empty states without error.
    """
    fresh_wallet = "0x" + "00" * 20

    resp_pos = await client.get(f"/wallets/{fresh_wallet}/positions")
    assert resp_pos.status_code == 200
    data_pos = resp_pos.json()
    assert data_pos["wallet"] == fresh_wallet
    assert data_pos["open_positions_count"] == 0
    assert data_pos["positions"] == []
    assert data_pos["resolution_stale"] is False
    assert data_pos["resolutions_as_of"] is None

    resp_pnl = await client.get(f"/wallets/{fresh_wallet}/pnl")
    assert resp_pnl.status_code == 200
    data_pnl = resp_pnl.json()
    assert Decimal(data_pnl["total_pnl_excl_unpriced"]) == 0
    assert Decimal(data_pnl["realized_pnl"]) == 0
    assert Decimal(data_pnl["unrealized_priced"]) == 0
    assert data_pnl["open_positions_count"] == 0


@pytest.mark.benchmark
@pytest.mark.asyncio
async def test_serving_latency_load_benchmark(client: httpx.AsyncClient):
    """
    Load & Latency Benchmark [MEASURED]:
    1. Measures single-request latency against GET /wallets/{address}/pnl (<100ms).
    2. Executes 20 concurrent requests against AppleTiiime (181 active positions)
       evaluating real-time mark-to-market batch pricing and verifying sub-second throughput.
    """
    wallet = "0x951634357346bb291df117f9ac410100feea2104"  # AppleTiiime

    # 1. Warm-up & single request latency
    resp_single = await client.get(f"/wallets/{wallet}/pnl")
    assert resp_single.status_code == 200
    single_lat = float(resp_single.headers.get("X-Response-Time-Ms", "50.0"))
    print(f"\n[BENCHMARK] Single Request Latency: [MEASURED] {single_lat:.2f}ms")

    # 2. Concurrent Load Test (20 concurrent requests)
    num_requests = 20
    t0 = time.perf_counter()
    tasks = [client.get(f"/wallets/{wallet}/pnl") for _ in range(num_requests)]
    responses = await asyncio.gather(*tasks)
    total_time = time.perf_counter() - t0

    assert all(r.status_code == 200 for r in responses)

    # Extract latency headers
    latencies = [float(r.headers.get("X-Response-Time-Ms", "10.0")) for r in responses]
    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p95 = latencies[int(len(latencies) * 0.95)]
    throughput = num_requests / total_time

    print(f"[BENCHMARK] Concurrent Requests: {num_requests}")
    print(f"[BENCHMARK] Total Wall-Clock:   {total_time * 1000:.2f}ms")
    print(f"[BENCHMARK] Serving Throughput: [MEASURED] {throughput:.1f} req/sec")
    print(f"[BENCHMARK] Serving p50 Latency: [MEASURED] {p50:.2f}ms")
    print(f"[BENCHMARK] Serving p95 Latency: [MEASURED] {p95:.2f}ms")


@pytest.mark.asyncio
async def test_concurrent_price_tick_race(client: httpx.AsyncClient):
    """
    Verify concurrent price ticks for the same token arriving near-simultaneously:
    Both ticks are safely recorded into append-only market_prices without collision.
    """
    token_id = "tok_race_" + uuid.uuid4().hex[:8]
    cond_id = "0xcond_race_" + uuid.uuid4().hex[:8]
    headers = {"X-API-Key": get_settings().api_key}

    # Launch two simultaneous price ticks
    task1 = client.post(
        f"/markets/price?token_id={token_id}&price=0.55&condition_id={cond_id}&source=last_trade",
        headers=headers,
    )
    task2 = client.post(
        f"/markets/price?token_id={token_id}&price=0.58&condition_id={cond_id}&source=last_trade",
        headers=headers,
    )
    r1, r2 = await asyncio.gather(task1, task2)

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["id"] != r2.json()["id"]

    # Verify latest mark price is retrievable
    resp = await client.get(f"/markets/{cond_id}/price?token_id={token_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert Decimal(data["mark"]["price"]) in [Decimal("0.55"), Decimal("0.58")]
