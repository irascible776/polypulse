"""Part 2 verification: leaderboard, wallet analytics, system metrics, enriched positions/trades."""

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.serving.analytics import invalidate_analytics_cache

APPLE = "0x951634357346bb291df117f9ac410100feea2104"
UNKNOWN_WALLET = "0x000000000000000000000000000000000000dead"


def _assert_no_float_money(obj):
    money_keys = {
        "realized_pnl",
        "window_realized_pnl",
        "unrealized_priced",
        "estimated_unrealized",
        "unpriced_cost_basis",
        "total_pnl_excl_unpriced",
        "total_cost_basis",
        "total_market_value",
        "volume_usdc",
        "trade_volume_usdc",
        "cumulative_realized_pnl",
        "drawdown",
        "max_drawdown",
        "usdc_size",
        "total_usdc_size",
        "exposure_weight_pct",
        "position_roi_pct",
    }
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in money_keys and v is not None:
                assert isinstance(v, str), f"Expected Decimal string for {k}, got {type(v)}: {v}"
            _assert_no_float_money(v)
    elif isinstance(obj, list):
        for item in obj:
            _assert_no_float_money(item)


@pytest.mark.asyncio
async def test_wallets_leaderboard_endpoint():
    invalidate_analytics_cache()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        bad = await client.get("/wallets?window=99d")
        assert bad.status_code == 400

        resp = await client.get("/wallets?window=all")
        assert resp.status_code == 200
        body = resp.json()
        assert body["partial"] is False
        assert body["failed_shards"] == []
        assert body["count"] >= 1
        assert body["instance_id"]
        _assert_no_float_money(body)

        apple = next((w for w in body["wallets"] if w["address"] == APPLE), None)
        assert apple is not None
        assert 0 <= apple["bucket_id"] < 16
        assert apple["shard_id"] in (0, 1)
        assert apple["rank"] >= 1


@pytest.mark.asyncio
async def test_wallet_analytics_endpoint_seeded_and_empty():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        bad = await client.get(f"/wallets/{APPLE}/analytics?window=invalid")
        assert bad.status_code == 400

        resp = await client.get(f"/wallets/{APPLE}/analytics?window=all")
        assert resp.status_code == 200
        body = resp.json()
        assert body["wallet"] == APPLE
        assert len(body["entry_price_buckets"]) == 4
        assert body["activity"]["excluded_flows"]["excluded_from_pnl"] is True
        assert "publication_revision" in body["accounting_audit"]
        assert "checkpoint_count" in body["accounting_audit"]
        _assert_no_float_money(body)

        empty = await client.get(f"/wallets/{UNKNOWN_WALLET}/analytics?window=30d")
        assert empty.status_code == 200
        eb = empty.json()
        assert eb["equity_curve"] == []
        assert eb["drawdown_summary"]["max_drawdown"] == "0"
        assert eb["activity"]["trade_volume_usdc"] == "0"
        assert eb["accounting_audit"]["reject_count"] == 0


@pytest.mark.asyncio
async def test_system_metrics_endpoint():
    invalidate_analytics_cache()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/system/metrics")
        assert resp.status_code == 200
        body = resp.json()
        assert body["partial"] is False
        assert set(body["ingestion_throughput"]) >= {
            "events_per_sec_60s",
            "events_per_sec_5m",
            "events_per_sec_1h",
        }
        assert set(body["detection_lag_seconds"]) >= {"p50", "p95", "p99", "sample_count"}
        assert body["static_benchmarks"]["is_static_benchmark"] is True
        assert body["static_benchmarks"]["not_live_traffic"] is True
        assert "0" in body["storage"]["shards"]


@pytest.mark.asyncio
async def test_enriched_positions_and_trades_fields():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        pos_resp = await client.get(f"/wallets/{APPLE}/positions")
        assert pos_resp.status_code == 200
        pos_body = pos_resp.json()
        _assert_no_float_money(pos_body)
        if pos_body["positions"]:
            first = pos_body["positions"][0]
            assert "market_question" in first
            assert "category" in first
            assert "exposure_weight_pct" in first
            assert "position_roi_pct" in first

        tr_resp = await client.get(f"/wallets/{APPLE}/trades?limit=5")
        assert tr_resp.status_code == 200
        tr_body = tr_resp.json()
        if tr_body["trades"]:
            assert "transaction_hash" in tr_body["trades"][0]
