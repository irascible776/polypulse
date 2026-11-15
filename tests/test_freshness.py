from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text

from app.ingestion.status import get_wallet_ingestion_status
from app.serving.routes import get_wallet_pnl, get_wallet_positions
from app.storage.database import async_session_factory
from app.storage.models import IngestionCursor


@pytest.mark.asyncio
@pytest.mark.parametrize("age,stale", [(10, False), (600, True), (None, True)])
async def test_freshness_uses_only_data_api_cursor(age, stale):
    wallet = "0x" + uuid4().hex + "0" * 8
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        await session.execute(text("SET LOCAL TIME ZONE 'Asia/Kolkata'"))
        session.add(IngestionCursor(wallet=wallet, source="fixture_replay", last_success_at=now))
        if age is not None:
            session.add(
                IngestionCursor(
                    wallet=wallet, source="data_api", last_success_at=now - timedelta(seconds=age)
                )
            )
        await session.flush()
        session.expunge_all()  # Reload timestamps using the session's non-UTC timezone.
        status = await get_wallet_ingestion_status(session, wallet)
        positions = await get_wallet_positions(wallet, as_of=None, session=session)
        pnl = await get_wallet_pnl(wallet, as_of=None, session=session)
        for response in (status, positions, pnl):
            assert response["ingestion_stale"] is stale
            if age is None:
                assert response["data_as_of"] is None
                assert response["ingestion_lag_seconds"] is None
            else:
                assert response["data_as_of"].endswith("Z")
                assert datetime.fromisoformat(response["data_as_of"]) == now - timedelta(
                    seconds=age
                )
                assert response["ingestion_lag_seconds"] >= age
        if age is None:
            assert status["last_success_at"] is None
        else:
            assert datetime.fromisoformat(status["last_success_at"]) == now - timedelta(seconds=age)
        assert positions["prices_as_of"] is None
        assert pnl["oldest_tick_age_seconds"] is None
        await session.rollback()
