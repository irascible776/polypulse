"""
Chaos testing script for PolyPulse ingestion backfill.
Starts a backfill of AppleTiiime, sends SIGKILL at a random moment,
restarts the process to resume from the last saved state, and verifies
zero data corruption, zero duplicates, and exact per-type event counts.
Repeats 5 times.
"""

import asyncio
import logging
import multiprocessing
import os
import random
import signal
import sys
import time
from typing import Any

from app.ingestion.backfill import run_wallet_backfill
from app.ingestion.fixture_replay import FixtureReplaySource
from app.storage.database import async_session_factory, check_database_connection
from app.storage.models import SourceEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("chaos_ingest")

APPLE_TIIIME_WALLET = "0x951634357346bb291df117f9ac410100feea2104"
EXPECTED_COUNTS = {
    "TRADE": 2261,
    "REDEEM": 158,
    "MAKER_REBATE": 15,
    "TAKER_REBATE": 11,
    "REFERRAL_REWARD": 9,
}
EXPECTED_TOTAL = 2454


async def reset_wallet_data(wallet: str) -> None:
    """Clear existing events and cursor for clean chaos trial."""
    async with async_session_factory() as session:
        from sqlalchemy import text

        await session.execute(
            text("DELETE FROM source_events WHERE wallet = :w"), {"w": wallet.lower()}
        )
        await session.execute(
            text("DELETE FROM ingestion_cursors WHERE wallet = :w"), {"w": wallet.lower()}
        )
        await session.commit()


def _worker_process(wallet: str, simulate_delay: float = 0.04) -> None:
    """Worker child process that runs backfill until completion or SIGKILL."""

    async def _run() -> None:
        from app.storage.database import AsyncSession, async_sessionmaker, engine

        # Ensure child process does not share connection pool or prepared statements
        await engine.dispose(close=False)
        fresh_session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        source = FixtureReplaySource(fixtures_dir="fixtures")
        original_fetch = source.fetch_page

        async def _delayed_fetch(*args: Any, **kwargs: Any) -> Any:
            await asyncio.sleep(simulate_delay)
            return await original_fetch(*args, **kwargs)

        source.fetch_page = _delayed_fetch  # type: ignore[method-assign]
        res = await run_wallet_backfill(
            wallet=wallet,
            source=source,
            session_factory=fresh_session_factory,
        )
        logger.info("Worker finished naturally with status: %s", res.get("status"))

    asyncio.run(_run())


async def verify_database_counts(wallet: str) -> dict[str, int]:
    """Verify total and per-type event counts in source_events."""
    from app.storage.database import engine

    await engine.dispose(close=False)
    async with async_session_factory() as session:
        from sqlalchemy import func, select

        stmt = (
            select(SourceEvent.event_type, func.count(SourceEvent.id))
            .where(SourceEvent.wallet == wallet.lower())
            .group_by(SourceEvent.event_type)
        )
        res = await session.execute(stmt)
        rows = res.all()
        return {r[0]: r[1] for r in rows}


def run_single_chaos_trial(trial_num: int) -> dict[str, Any]:
    logger.info("=== STARTING CHAOS TRIAL %d/5 ===", trial_num)

    ctx = multiprocessing.get_context("spawn")

    # 0. Clean wallet data before each trial
    asyncio.run(reset_wallet_data(APPLE_TIIIME_WALLET))

    # 1. Start worker child process
    p = ctx.Process(
        target=_worker_process,
        args=(APPLE_TIIIME_WALLET,),
    )
    p.start()
    pid = p.pid
    logger.info("Worker process %d spawned.", pid)

    # 2. Wait a random interval and kill with SIGKILL
    kill_delay = random.uniform(0.04, 0.12)
    time.sleep(kill_delay)

    if p.is_alive():
        logger.warning("Sending SIGKILL to worker %d after %.3fs...", pid, kill_delay)
        os.kill(pid, signal.SIGKILL)
        p.join()
        killed = True
    else:
        logger.info("Worker %d finished before kill.", pid)
        killed = False

    # 3. Restart worker to resume from last saved cursor/state
    logger.info("Restarting backfill worker to resume from interrupted state...")
    resume_p = ctx.Process(
        target=_worker_process,
        args=(APPLE_TIIIME_WALLET,),
    )
    t0 = time.time()
    resume_p.start()
    resume_p.join(timeout=30.0)
    elapsed = time.time() - t0

    # 4. Verify results
    counts = asyncio.run(verify_database_counts(APPLE_TIIIME_WALLET))
    total_rows = sum(counts.values())

    is_exact = total_rows == EXPECTED_TOTAL and counts == EXPECTED_COUNTS
    logger.info(
        "Trial %d result: total=%d (expected %d), match=%s, resume_time=%.2fs",
        trial_num,
        total_rows,
        EXPECTED_TOTAL,
        is_exact,
        elapsed,
    )

    return {
        "trial": trial_num,
        "killed": killed,
        "kill_delay_s": round(kill_delay, 3),
        "resume_time_s": round(elapsed, 3),
        "total_rows": total_rows,
        "per_type_counts": counts,
        "success": is_exact,
    }


def main() -> None:
    is_connected = asyncio.run(check_database_connection())
    if not is_connected:
        logger.error(
            "Database not reachable. Run 'make db-up' (or start PostgreSQL container) first."
        )
        sys.exit(1)

    trials_results = []
    for i in range(1, 6):
        res = run_single_chaos_trial(i)
        trials_results.append(res)

    all_passed = all(t["success"] for t in trials_results)
    logger.info("All 5 trials passed: %s", all_passed)


if __name__ == "__main__":
    main()
