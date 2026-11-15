import httpx
import pytest
from sqlalchemy import desc, select

from app.main import app
from app.storage.database import async_session_factory
from app.storage.models import PositionLedger


@pytest.mark.asyncio
async def test_trades_cursor_uses_stable_api_order():
    wallet = "0x951634357346bb291df117f9ac410100feea2104"
    async with async_session_factory() as session:
        expected = (
            await session.execute(
                select(PositionLedger.id, PositionLedger.api_order)
                .where(PositionLedger.wallet == wallet)
                .order_by(desc(PositionLedger.api_order))
                .limit(10)
            )
        ).all()
    assert len(expected) == 10
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = (await client.get(f"/wallets/{wallet}/trades", params={"limit": 5})).json()
        second = (
            await client.get(
                f"/wallets/{wallet}/trades", params={"limit": 5, "cursor": first["next_cursor"]}
            )
        ).json()
        again = (await client.get(f"/wallets/{wallet}/trades", params={"limit": 5})).json()
        assert [row["id"] for row in first["trades"] + second["trades"]] == [
            row.id for row in expected
        ]
        assert first == again
        assert first["next_cursor"] == expected[4].api_order
        assert (
            await client.get(f"/wallets/{wallet}/trades", params={"limit": 0})
        ).status_code == 422
        assert (
            await client.get(f"/wallets/{wallet}/trades", params={"limit": 201})
        ).status_code == 422
