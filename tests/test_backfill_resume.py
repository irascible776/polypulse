from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.ingestion.backfill import run_wallet_backfill
from app.ingestion.source import PageResult
from app.ingestion.writer import write_event_batch
from app.storage.database import async_session_factory
from app.storage.models import IngestionCursor, SourceEvent


@pytest.mark.asyncio
async def test_backfill_restart_replays_held_timestamp_group():
    wallet = f"backfill-resume-{uuid4()}"
    first = {"type": "REWARD", "timestamp": 1700000000, "transaction_hash": "first"}
    held = {"type": "REWARD", "timestamp": 1700000001, "transaction_hash": "held"}

    class Source:
        source_tag = "backfill-test"

        def __init__(self, fail):
            self.fail = fail
            self.starts = []

        async def fetch_page(self, cursor=None, **kwargs):
            self.starts.append(kwargs.get("start"))
            if cursor == "page-1":
                if self.fail:
                    raise RuntimeError("interrupted")
                return PageResult(events=[held], next_cursor=None, has_more=False)
            return PageResult(events=[first, held], next_cursor="page-1", has_more=True)

    # A newer event from another source must not advance the backfill resume point.
    async with async_session_factory() as session:
        async with session.begin():
            await write_event_batch(
                session,
                wallet,
                "other-source",
                [({"type": "REWARD", "timestamp": 1800000000}, f"{wallet}-other")],
            )

    with pytest.raises(RuntimeError, match="interrupted"):
        await run_wallet_backfill(wallet, Source(True), async_session_factory)
    async with async_session_factory() as session:
        cursor = await session.get(IngestionCursor, (wallet, "backfill-test"))
        assert cursor.last_cursor is None
        assert int(cursor.backfill_resume_time.timestamp()) == 1700000000

    resumed = Source(False)
    result = await run_wallet_backfill(wallet, resumed, async_session_factory)
    assert result["status"] == "complete"
    assert resumed.starts[0] == 1699999999
    async with async_session_factory() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(SourceEvent).where(SourceEvent.wallet == wallet)
            )
            == 4
        )
