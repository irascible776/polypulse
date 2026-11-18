from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from app.storage.database import async_session_factory
from app.storage.models import MarketResolution
from app.valuation.resolution import save_resolution


@pytest.mark.asyncio
async def test_older_resolution_observation_cannot_overwrite_newer_state():
    condition = uuid4().hex
    now = datetime.now(timezone.utc)
    record = {
        "condition_id": condition,
        "resolved": True,
        "payout_numerators": [1, 0],
        "payout_denominator": Decimal("1"),
        "gamma_closed": True,
        "gamma_uma_status": "resolved",
        "gamma_raw": {},
        "chain_block": 200,
        "checked_at": now,
        "agreement": True,
    }
    async with async_session_factory() as session:
        assert await save_resolution(session, record)
        older = {**record, "checked_at": now - timedelta(seconds=1), "resolved": False}
        behind = {**record, "checked_at": now + timedelta(seconds=1), "chain_block": 199}
        assert not await save_resolution(session, older)
        assert not await save_resolution(session, behind)
        stored = await session.get(MarketResolution, condition)
        await session.refresh(stored)
        assert stored.resolved and stored.chain_block == 200 and stored.checked_at == now
        await session.rollback()
