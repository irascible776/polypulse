"""Measure synthetic ingestion and projection on a dedicated scratch database."""

import argparse
import asyncio
import time
import uuid

from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from app.accounting.projector import AccountingProjector
from app.config import get_settings
from app.ingestion.identifiers import EventIdAssigner
from app.ingestion.writer import write_event_batch
from app.storage.database import async_session_factory, engine
from app.storage.models import SourceEvent


async def run(count: int, batch_size: int) -> None:
    if make_url(get_settings().database_url).database != "polypulse_benchmark":
        raise SystemExit("DATABASE_URL must point to the dedicated polypulse_benchmark database")
    wallet = f"benchmark-{uuid.uuid4()}"
    assigner = EventIdAssigner(wallet)
    started = time.perf_counter()
    for start in range(0, count, batch_size):
        events = []
        for number in range(start, min(start + batch_size, count)):
            event = {
                "type": "TRADE",
                "side": "BUY",
                "timestamp": 1700000000 + number,
                "transaction_hash": f"0x{number:064x}",
                "condition_id": "benchmark-condition",
                "token_id": "benchmark-token",
                "outcome_index": 0,
                "size": "1",
                "usdc_size": "0.5",
                "price": "0.5",
                "event_slug": "benchmark",
            }
            events.append((event, assigner.get_id(event)))
        async with async_session_factory() as session:
            async with session.begin():
                inserted, duplicates = await write_event_batch(session, wallet, "benchmark", events)
                if inserted != len(events) or duplicates:
                    raise RuntimeError("Synthetic ingestion count mismatch")
    ingestion_seconds = time.perf_counter() - started
    started = time.perf_counter()
    processed = 0
    projector = AccountingProjector()
    while processed < count:
        async with async_session_factory() as session:
            async with session.begin():
                result = await projector.catch_up_wallet(session, wallet, batch_size=batch_size)
                if result.events_processed == 0:
                    raise RuntimeError("Projector stopped before processing all events")
                processed += result.events_processed
    projection_seconds = time.perf_counter() - started
    async with async_session_factory() as session:
        stored = await session.scalar(
            select(func.count()).select_from(SourceEvent).where(SourceEvent.wallet == wallet)
        )
        if stored != count:
            raise RuntimeError("Stored event count mismatch")
    print(f"wallet={wallet} stored={stored} projected={processed} [MEASURED]")
    print(f"ingestion_seconds={ingestion_seconds:.3f} [MEASURED]")
    print(f"projection_seconds={projection_seconds:.3f} [MEASURED]")
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args()
    if args.events < 1 or args.batch_size < 1:
        parser.error("events and batch-size must be positive")
    asyncio.run(run(args.events, args.batch_size))
