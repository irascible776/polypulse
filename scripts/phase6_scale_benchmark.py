"""Run a repeatable mixed-wallet workload through shard workers and the API."""

import argparse
import asyncio
import hashlib
import json
import multiprocessing
import resource
import tempfile
import time
from pathlib import Path

import httpx
from sqlalchemy import func, select, text

from app.accounting.projector import AccountingProjector
from app.ingestion.source import PageResult
from app.ingestion.writer import write_event_batch
from app.main import app
from app.storage.database import (
    async_session_factory,
    engine,
    secondary_engine,
    secondary_session_factory,
    wallet_session_factory,
)
from app.storage.models import Position, SourceEvent, TrackedWallet, WalletTotal
from app.worker import Worker

BASE_TIME = 1700000000


class SyntheticSource:
    source_tag = "data_api"

    def __init__(self, counts):
        self.counts = counts

    async def fetch_page(self, wallet, cursor=None, start=None, limit=500, **kwargs):
        begin = int(cursor) if cursor else max(0, (start or 1) - BASE_TIME)
        end = min(begin + limit, self.counts[wallet])
        events = []
        for index in range(begin, end):
            phase = index % 6
            kind = ("TRADE", "TRADE", "TRADE", "REDEEM", "SPLIT", "MERGE")[phase]
            side = "SELL" if phase == 2 else "BUY"
            cash = "0.6" if phase == 2 else ("1" if phase == 3 else "0.5")
            events.append(
                {
                    "proxy_wallet": wallet,
                    "type": kind,
                    "timestamp": BASE_TIME + index,
                    "transaction_hash": f"0x{index:064x}",
                    "condition_id": wallet,
                    "token_id": wallet,
                    "outcome_index": 0,
                    "side": side,
                    "size": "1",
                    "usdc_size": cash,
                    "price": cash,
                    "event_slug": "synthetic",
                }
            )
            if wallet == next(iter(self.counts)) and index == 0:
                events[-1]["outcome_index"] = 999
                events[-1]["event_slug"] = ""
        return PageResult(
            events=events,
            next_cursor=str(end) if end < self.counts[wallet] else None,
            has_more=end < self.counts[wallet],
        )

    async def fetch_status(self):
        return None

    async def close(self):
        pass


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(int((len(ordered) - 1) * fraction), len(ordered) - 1)] if ordered else None


def worker_process(counts, started, output):
    async def execute():
        completed = {}

        class MeasuredWorker(Worker):
            async def wallet_cycle(self, wallet, connection=None):
                result = await super().wallet_cycle(wallet, connection)
                if "error" not in result and not result.get("skipped_lock"):
                    completed.setdefault(wallet, time.perf_counter() - started)
                return result

        workers = [
            MeasuredWorker(shard_id=0),
            MeasuredWorker(
                session_factory=secondary_session_factory,
                db_engine=secondary_engine,
                market_sessions=async_session_factory,
                shard_id=1,
            ),
        ]
        for worker in workers:
            await worker.source.close()
            worker.source = SyntheticSource(counts)
        try:
            await asyncio.gather(*(worker.poll_cycle() for worker in workers))
            Path(output).write_text(
                json.dumps(
                    {
                        "completed": completed,
                        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        "errors": sum(worker.stats["C"]["errors"] for worker in workers),
                    }
                )
            )
        finally:
            for worker in workers:
                await worker.close()
            await engine.dispose()
            await secondary_engine.dispose()

    asyncio.run(execute())


async def run(event_count, wallet_count, worker_count, seed):
    if secondary_engine is None or secondary_session_factory is None:
        raise RuntimeError("Two scratch shard databases are required")
    if (
        engine.url.database != "polypulse_scale_a"
        or secondary_engine.url.database != "polypulse_scale_b"
    ):
        raise RuntimeError("Use dedicated polypulse_scale_a and polypulse_scale_b databases")
    if wallet_count < 2 or event_count < wallet_count:
        raise ValueError("At least two wallets and one event per wallet are required")
    from app.storage.sharding import wallet_bucket

    wallets = []
    candidate = 0
    while len(wallets) < wallet_count:
        wallet = "0x" + hashlib.sha256(f"{seed}:{candidate}".encode()).hexdigest()[:40]
        candidate += 1
        if len(wallets) < 2 and (wallet_bucket(wallet) < 8) != (len(wallets) == 0):
            continue
        wallets.append(wallet)
    hot_count = max(1, min(event_count // 5, event_count - wallet_count + 1))
    remaining = event_count - hot_count
    counts = {wallets[0]: hot_count}
    for index, wallet in enumerate(wallets[1:]):
        counts[wallet] = remaining // (wallet_count - 1) + (index < remaining % (wallet_count - 1))
    for factory in (async_session_factory, secondary_session_factory):
        async with factory() as session:
            if await session.scalar(select(func.count()).select_from(TrackedWallet)):
                raise RuntimeError("Scratch shards must have no tracked wallets")
    for wallet in wallets:
        async with wallet_session_factory(wallet)() as session:
            session.add(TrackedWallet(address=wallet))
            await session.commit()

    started = time.perf_counter()

    samples = []
    status_counts = {}
    stop = asyncio.Event()

    async def sample_api():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            while not stop.is_set():
                begin = time.perf_counter()
                response = await client.get(f"/wallets/{wallets[0]}/trades")
                samples.append((time.perf_counter() - begin) * 1000)
                status_counts[response.status_code] = status_counts.get(response.status_code, 0) + 1
                await asyncio.sleep(0.1)

    completion_times = {}
    worker_memory = []
    with tempfile.TemporaryDirectory(prefix="polypulse-scale-") as tempdir:
        context = multiprocessing.get_context("spawn")
        paths = [str(Path(tempdir) / f"worker-{index}.json") for index in range(worker_count)]
        processes = [
            context.Process(target=worker_process, args=(counts, started, path)) for path in paths
        ]
        for process in processes:
            process.start()
        sampler = asyncio.create_task(sample_api())
        try:
            while any(process.is_alive() for process in processes):
                await asyncio.sleep(0.1)
        finally:
            stop.set()
            await sampler
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join()
        for process, path in zip(processes, paths, strict=True):
            if process.exitcode != 0:
                raise RuntimeError(f"Worker process exited with code {process.exitcode}")
            result = json.loads(Path(path).read_text())
            if result["errors"]:
                raise RuntimeError(f"Worker reported {result['errors']} errors")
            worker_memory.append(result["max_rss_kib"])
            for wallet, completed_at in result["completed"].items():
                completion_times[wallet] = min(
                    completed_at, completion_times.get(wallet, completed_at)
                )
    elapsed = time.perf_counter() - started
    if len(completion_times) != wallet_count:
        raise RuntimeError(
            f"Expected {wallet_count} completed wallets, got {len(completion_times)}"
        )
    first_completions = list(completion_times.values())
    # All events are available at start. Wallet completion includes post-projection
    # bookkeeping, so its maximum bounds the age of the oldest unfinished event.
    maximum_backlog_age = max(first_completions)
    shard_completion = {
        str(shard): max(
            (
                completed
                for wallet, completed in completion_times.items()
                if (0 if wallet_bucket(wallet) < 8 else 1) == shard
            ),
            default=None,
        )
        for shard in (0, 1)
    }
    stored = projected = 0
    sizes = []
    for factory in (async_session_factory, secondary_session_factory):
        async with factory() as session:
            stored += await session.scalar(select(func.count()).select_from(SourceEvent))
            projected += (
                await session.scalar(select(func.sum(WalletTotal.last_applied_api_order))) or 0
            )
            sizes.append(await session.scalar(text("SELECT pg_database_size(current_database())")))
    if stored != event_count or projected != event_count:
        raise RuntimeError(f"Event count mismatch stored={stored} projected={projected}")
    original_count = stored
    workers = [
        Worker(shard_id=0),
        Worker(
            session_factory=secondary_session_factory,
            db_engine=secondary_engine,
            market_sessions=async_session_factory,
            shard_id=1,
        ),
    ]
    for worker in workers:
        await worker.source.close()
        worker.source = SyntheticSource(counts)
    await asyncio.gather(*(worker.poll_cycle() for worker in workers))
    stored = 0
    for factory in (async_session_factory, secondary_session_factory):
        async with factory() as session:
            stored += await session.scalar(select(func.count()).select_from(SourceEvent))
    if stored != original_count:
        raise RuntimeError("Duplicate delivery changed the stored event count")

    hot_wallet = wallets[0]
    late = {
        "type": "TRADE",
        "side": "BUY",
        "timestamp": BASE_TIME + hot_count - 2,
        "transaction_hash": "0x" + "f" * 64,
        "condition_id": hot_wallet,
        "token_id": hot_wallet,
        "outcome_index": 0,
        "size": "1",
        "usdc_size": "0.5",
        "price": "0.5",
        "event_slug": "late",
    }
    async with wallet_session_factory(hot_wallet)() as session:
        async with session.begin():
            await write_event_batch(session, hot_wallet, "data_api", [(late, f"{hot_wallet}-late")])
            begin = time.perf_counter()
            projector = AccountingProjector()
            summary = await projector.catch_up_wallet(session, hot_wallet)
            correction_seconds = time.perf_counter() - begin
            position = await session.get(Position, (hot_wallet, hot_wallet, hot_wallet))
            total = await session.get(WalletTotal, hot_wallet)
            before = (
                position.shares,
                position.avg_cost_basis,
                position.realized_pnl,
                total.total_realized_pnl,
                total.total_trade_count,
                total.total_redeem_count,
                total.unsupported_event_count,
                total.reject_count,
            )
            await session.execute(
                text("""
                CREATE TEMP TABLE expected_ledger ON COMMIT DROP AS
                SELECT wallet, condition_id, token_id, source_event_id, api_order, event_type,
                       event_time, shares_before, shares_delta, shares_after, avg_cost_before,
                       avg_cost_after, cash_delta, realized_pnl_delta, cash_price_gap
                FROM position_ledger WHERE wallet = :wallet
            """),
                {"wallet": hot_wallet},
            )
            await session.execute(
                text("""
                CREATE TEMP TABLE expected_rejects ON COMMIT DROP AS
                SELECT wallet, source_event_id, api_order, event_type, event_time,
                       condition_id, token_id, size, usdc_size, reject_reason
                FROM position_ledger_rejects WHERE wallet = :wallet
            """),
                {"wallet": hot_wallet},
            )
            begin = time.perf_counter()
            full = await projector.recompute_wallet(session, hot_wallet)
            full_seconds = time.perf_counter() - begin
            position = await session.get(Position, (hot_wallet, hot_wallet, hot_wallet))
            after = (
                position.shares,
                position.avg_cost_basis,
                position.realized_pnl,
                total.total_realized_pnl,
                total.total_trade_count,
                total.total_redeem_count,
                total.unsupported_event_count,
                total.reject_count,
            )
            if before != after:
                raise RuntimeError("Checkpoint replay does not match full replay")
            comparisons = (
                (
                    "expected_ledger",
                    "position_ledger",
                    "wallet, condition_id, token_id, source_event_id, api_order, event_type, "
                    "event_time, shares_before, shares_delta, shares_after, avg_cost_before, "
                    "avg_cost_after, cash_delta, realized_pnl_delta, cash_price_gap",
                ),
                (
                    "expected_rejects",
                    "position_ledger_rejects",
                    "wallet, source_event_id, api_order, event_type, event_time, condition_id, "
                    "token_id, size, usdc_size, reject_reason",
                ),
            )
            for expected, actual, columns in comparisons:
                mismatch = await session.scalar(
                    text(f"""
                        SELECT EXISTS (
                            (SELECT {columns} FROM {expected} EXCEPT ALL
                             SELECT {columns} FROM {actual} WHERE wallet = :wallet)
                            UNION ALL
                            (SELECT {columns} FROM {actual} WHERE wallet = :wallet EXCEPT ALL
                             SELECT {columns} FROM {expected})
                        )
                    """),
                    {"wallet": hot_wallet},
                )
                if mismatch:
                    raise RuntimeError(f"Checkpoint replay differs from full replay in {actual}")

            original = await session.scalar(
                select(SourceEvent).where(
                    SourceEvent.wallet == hot_wallet,
                    SourceEvent.transaction_hash == f"0x{0:064x}",
                    SourceEvent.event_type == "TRADE",
                )
            )
            if original is None:
                raise RuntimeError("Synthetic unenriched event was not found")
            enriched = dict(original.raw, outcome_index=0, event_slug="synthetic")
            await write_event_batch(
                session, hot_wallet, "data_api", [(enriched, f"{hot_wallet}-enriched")]
            )
            await session.refresh(original)
            if original.superseded_by is None:
                raise RuntimeError("Enrichment did not supersede the original observation")
            begin = time.perf_counter()
            enrichment = await projector.catch_up_wallet(session, hot_wallet)
            enrichment_seconds = time.perf_counter() - begin
            if not enrichment.is_recompute:
                raise RuntimeError("Enrichment did not trigger accounting replay")
            position = await session.get(Position, (hot_wallet, hot_wallet, hot_wallet))
            enriched_state = (
                position.shares,
                position.avg_cost_basis,
                position.realized_pnl,
                total.total_realized_pnl,
                total.total_trade_count,
                total.total_redeem_count,
                total.unsupported_event_count,
                total.reject_count,
            )
            for expected, actual, columns in comparisons:
                await session.execute(text(f"TRUNCATE {expected}"))
                await session.execute(
                    text(
                        f"INSERT INTO {expected} SELECT {columns} FROM {actual} "
                        "WHERE wallet = :wallet"
                    ),
                    {"wallet": hot_wallet},
                )
            await projector.recompute_wallet(session, hot_wallet)
            position = await session.get(Position, (hot_wallet, hot_wallet, hot_wallet))
            rebuilt_state = (
                position.shares,
                position.avg_cost_basis,
                position.realized_pnl,
                total.total_realized_pnl,
                total.total_trade_count,
                total.total_redeem_count,
                total.unsupported_event_count,
                total.reject_count,
            )
            if enriched_state != rebuilt_state:
                raise RuntimeError("Enrichment replay does not match full replay")
            for expected, actual, columns in comparisons:
                mismatch = await session.scalar(
                    text(f"""
                        SELECT EXISTS (
                            (SELECT {columns} FROM {expected} EXCEPT ALL
                             SELECT {columns} FROM {actual} WHERE wallet = :wallet)
                            UNION ALL
                            (SELECT {columns} FROM {actual} WHERE wallet = :wallet EXCEPT ALL
                             SELECT {columns} FROM {expected})
                        )
                    """),
                    {"wallet": hot_wallet},
                )
                if mismatch:
                    raise RuntimeError(f"Enrichment replay differs from full replay in {actual}")
    for worker in workers:
        await worker.close()
    print(
        json.dumps(
            {
                "events": event_count,
                "wallets": wallet_count,
                "workers_per_shard": worker_count,
                "seed": seed,
                "hot_wallet_events": hot_count,
                "seconds": elapsed,
                "events_per_second": event_count / elapsed,
                "wallet_completion_p95_seconds": percentile(first_completions, 0.95),
                "maximum_backlog_age_upper_bound_seconds": maximum_backlog_age,
                "backlog_age_basis": "all events available at start; bound includes bookkeeping",
                "last_wallet_completion_seconds_by_shard": shard_completion,
                "api_p95_ms": percentile(samples, 0.95),
                "api_status_counts": status_counts,
                "checkpoint_replay_events": summary.events_processed,
                "checkpoint_replay_seconds": correction_seconds,
                "full_replay_events": full.events_processed,
                "full_replay_seconds": full_seconds,
                "enrichment_replay_events": enrichment.events_processed,
                "enrichment_replay_seconds": enrichment_seconds,
                "database_bytes": sizes,
                "process_max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "worker_process_max_rss_kib": worker_memory,
                "duplicate_delivery_added": stored - original_count,
            },
            indent=2,
        )
    )
    await engine.dispose()
    await secondary_engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--wallets", type=int, default=20)
    parser.add_argument("--workers", type=int, choices=(1, 2, 4), default=1)
    parser.add_argument("--seed", type=int, default=6)
    args = parser.parse_args()
    asyncio.run(run(args.events, args.wallets, args.workers, args.seed))
