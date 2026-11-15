from decimal import Decimal

import httpx
import pytest
from httpx import ASGITransport
from sqlalchemy import select

from app.main import app, lifespan
from app.storage.database import async_session_factory
from app.storage.models import Position


@pytest.mark.asyncio
async def test_golden_scratch_wallet_and_money_strings():
    wallet = "0x951634357346bb291df117f9ac410100feea2104"
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        positions = (await c.get(f"/wallets/{wallet}/positions")).json()
        pnl = (await c.get(f"/wallets/{wallet}/pnl")).json()
        trades = (await c.get(f"/wallets/{wallet}/trades")).json()
        resolved = next(p for p in positions["positions"] if p["price_source"] == "resolved")
        market = (await c.get(f"/markets/{resolved['condition_id']}/price")).json()
        one = (
            await c.get(
                f"/markets/{resolved['condition_id']}/price",
                params={"token_id": resolved["token_id"]},
            )
        ).json()
        assert len(market["prices"]) == 2
        assert one["token_id"] == resolved["token_id"]
        assert one["outcome"] is not None
    live = Decimal(0)
    estimated = Decimal(0)
    async with async_session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(Position).where(Position.wallet == wallet, Position.shares > 0)
                )
            )
            .scalars()
            .all()
        )
        stored = {row.token_id: (row.shares, row.avg_cost_basis) for row in rows}
    for p in positions["positions"]:
        shares, basis = stored[p["token_id"]]
        assert shares == Decimal(p["shares"])
        assert basis == Decimal(p["avg_cost_basis"])
        if p["mark_price"] is not None:
            amount = (Decimal(p["mark_price"]) - basis) * shares
            if p["estimated"]:
                estimated += amount
            else:
                live += amount
    assert live == Decimal(pnl["unrealized_priced"])
    assert estimated == Decimal(pnl["estimated_unrealized"])
    money_keys = {
        "shares",
        "avg_cost_basis",
        "cost_basis_total",
        "mark_price",
        "market_value",
        "unrealized_pnl",
        "realized_pnl",
        "unrealized_priced",
        "estimated_unrealized",
        "unpriced_cost_basis",
        "total_pnl_excl_unpriced",
        "total_cost_basis",
        "total_market_value",
        "shares_delta",
        "shares_after",
        "avg_cost_after",
        "cash_delta",
        "realized_pnl_delta",
        "price",
        "bid",
        "ask",
    }

    def walk(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key in money_keys and item is not None:
                    assert isinstance(item, str), (key, item)
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk([positions, pnl, trades, market, one])


@pytest.mark.asyncio
async def test_environment_key_required_for_startup_and_price_route(monkeypatch):
    monkeypatch.delenv("POLYPULSE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="POLYPULSE_API_KEY"):
        async with lifespan(app):
            pass
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        result = await c.post("/markets/price?token_id=test&price=0.4")
        assert result.status_code == 401
