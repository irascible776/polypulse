import asyncio
import time

from sqlalchemy import text

from app.ingestion.backfill import run_wallet_backfill
from app.ingestion.data_api import DataApiSource
from app.storage.database import async_session_factory
from scripts.guard_test_database import main as guard_database

WALLETS = [
    ("AppleTiiime", "0x951634357346bb291df117f9ac410100feea2104"),
    ("JnStPrdctns", "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"),
]


async def run_live_backfill():
    if guard_database():
        raise SystemExit(1)
    # 0. Clean slate for data_api source
    async with async_session_factory() as session:
        await session.execute(text("DELETE FROM source_events WHERE source = 'data_api'"))
        await session.execute(text("DELETE FROM ingestion_cursors WHERE source = 'data_api'"))
        await session.commit()
    print("Database reset for source='data_api'. Starting live API backfill...")

    source = DataApiSource()

    total_requests = 0
    total_events = 0
    start_time = time.time()

    results = {}
    for label, wallet in WALLETS:
        w_start = time.time()
        res = await run_wallet_backfill(
            wallet=wallet,
            source=source,
            session_factory=async_session_factory,
        )
        w_duration = time.time() - w_start
        pages = res.get("pages", 0)
        inserted = res.get("total_inserted", 0)
        dupes = res.get("total_duplicates", 0)
        results[label] = {
            "duration": w_duration,
            "pages": pages,
            "inserted": inserted,
            "duplicates": dupes,
        }
        total_requests += pages
        total_events += inserted
        print(
            f"[{label}] Backfilled in {w_duration:.2f}s across {pages} requests "
            f"({inserted} events saved, {dupes} dupes)."
        )

    total_duration = time.time() - start_time
    reqs_per_sec = total_requests / total_duration if total_duration > 0 else 0
    events_per_sec = total_events / total_duration if total_duration > 0 else 0

    print("\n=== LIVE BACKFILL MEASUREMENT SUMMARY ===")
    print(f"Total Duration: {total_duration:.2f}s")
    print(f"Total Requests: {total_requests}")
    print(f"Total Events Saved: {total_events}")
    print(f"Backfill Throughput: {reqs_per_sec:.2f} requests/second [MEASURED]")
    print(f"Backfill Throughput: {events_per_sec:.2f} events/second [MEASURED]")

    return results, reqs_per_sec, events_per_sec


if __name__ == "__main__":
    asyncio.run(run_live_backfill())
