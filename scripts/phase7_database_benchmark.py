"""Synthetic single-shard ingestion/projection/correction benchmark with concurrent ASGI reads.

Requires an empty, migrated database named polypulse_phase7_bench. Leaves its
synthetic rows for inspection. Never connects to Polymarket or deletes live data.
"""

import argparse
import asyncio
import hashlib
import json
import resource
import time
from dataclasses import asdict

import httpx
from sqlalchemy import func, select, text

from app.accounting.executor import ConditionExecutor
from app.accounting.projector import AccountingProjector
from app.ingestion.writer import write_event_batch
from app.main import app
from app.storage.database import async_session_factory, engine, secondary_engine
from app.storage.models import (
    AccountingCheckpoint,
    ConditionCheckpoint,
    Position,
    PositionLedger,
    PositionLedgerReject,
    SourceEvent,
    TrackedWallet,
    WalletTotal,
)

BASE_TIME = 1700000000


def event(index, conditions):
    condition = str(index % conditions)
    phase = (index // conditions) % 4
    return {
        "type": "REDEEM" if phase == 3 else "TRADE",
        "timestamp": BASE_TIME + index,
        "condition_id": condition,
        "token_id": "" if phase == 3 else condition,
        "outcome_index": 0,
        "side": "SELL" if phase == 2 else "BUY",
        "size": "3",
        "usdc_size": "2" if phase >= 2 else "1",
        "price": "0",
        "transaction_hash": f"0x{index:064x}",
        "event_slug": "synthetic-phase7",
    }


async def digest(session, wallet):
    output = hashlib.sha256()
    ignored = {
        "id",
        "wallet",
        "source_event_id",
        "created_at",
        "updated_at",
        "is_recompute",
        "publication_revision",
        "recompute_count",
        "last_recompute_at",
    }
    for model in (Position, PositionLedger, PositionLedgerReject, WalletTotal):
        columns = [c for c in model.__table__.columns if c.name not in ignored]
        ordering = (
            [model.api_order]
            if hasattr(model, "api_order")
            else ([model.condition_id, model.token_id] if model is Position else [model.wallet])
        )
        rows = await session.stream(
            select(*columns)
            .where(model.wallet == wallet)
            .order_by(*ordering)
            .execution_options(yield_per=1000)
        )
        output.update(model.__tablename__.encode())
        async for row in rows:
            output.update(json.dumps(list(row), default=str).encode())
    return output.hexdigest()


class MeasuredExecutor(ConditionExecutor):
    def __init__(self, workers):
        super().__init__(workers)
        self.overlaps = 0
        self.pids = set()

    async def compute(self, *args, **kwargs):
        results = await super().compute(*args, **kwargs)
        end = 0
        for result in sorted(results, key=lambda r: r[5]):
            self.pids.add(result[4])
            self.overlaps += result[5] < end
            end = max(end, result[6])
        return results


async def measure(args, processes, repeat):
    wallet = (
        "0x" + hashlib.sha256(f"{args.conditions}:{repeat}:{processes}".encode()).hexdigest()[:40]
    )
    executor = MeasuredExecutor(processes) if processes else None
    projector = AccountingProjector(executor=executor, condition_replay=True)
    async with async_session_factory() as session, session.begin():
        session.add(TrackedWallet(address=wallet))
    stop = asyncio.Event()
    latencies, statuses, projection_attempts = [], {}, []

    async def sample():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            while not stop.is_set():
                start = time.perf_counter()
                response = await client.get(f"/wallets/{wallet}/pnl")
                latencies.append((time.perf_counter() - start) * 1000)
                statuses[response.status_code] = statuses.get(response.status_code, 0) + 1
                await asyncio.sleep(0.02)

    sampler = asyncio.create_task(sample())
    ingestion_seconds = 0
    started = time.perf_counter()
    try:
        for offset in range(0, args.events, args.batch_size):
            batch = [
                (event(i, args.conditions), f"{wallet}-{i}")
                for i in range(offset, min(args.events, offset + args.batch_size))
            ]
            begin = time.perf_counter()
            async with async_session_factory() as session, session.begin():
                await write_event_batch(session, wallet, "phase7-benchmark", batch)
            ingestion_seconds += time.perf_counter() - begin
            begin = time.perf_counter()
            async with async_session_factory() as session, session.begin():
                await projector.catch_up_wallet(session, wallet, batch_size=args.batch_size)
            projection_attempts.append(time.perf_counter() - begin)
        ingestion_projection_seconds = time.perf_counter() - started
        late = event(max(1, args.events - args.batch_size // 2), args.conditions)
        late.update(
            type="TRADE",
            side="BUY",
            condition_id="0",
            token_id="0",
            transaction_hash="late",
            size="1",
            usdc_size="0.25",
        )
        async with async_session_factory() as session, session.begin():
            await write_event_batch(session, wallet, "phase7-benchmark", [(late, wallet + "-late")])
        begin = time.perf_counter()
        async with async_session_factory() as session, session.begin():
            correction = await projector.catch_up_wallet(session, wallet)
        correction_seconds = time.perf_counter() - begin
        async with async_session_factory() as session:
            corrected = await digest(session, wallet)
            checkpoint_bytes = 0
            for model in (AccountingCheckpoint, ConditionCheckpoint):
                checkpoint_bytes += await session.scalar(
                    select(func.coalesce(func.sum(func.pg_column_size(model.positions)), 0)).where(
                        model.wallet == wallet
                    )
                )
        begin = time.perf_counter()
        async with async_session_factory() as session, session.begin():
            full = await AccountingProjector().recompute_wallet(session, wallet)
            if await digest(session, wallet) != corrected:
                raise RuntimeError("Correction differs from serial full rebuild")
        full_seconds = time.perf_counter() - begin
    finally:
        stop.set()
        await sampler
        if executor:
            await executor.close()
    if set(statuses) != {200}:
        raise RuntimeError(f"API sampling failed: {statuses}")
    async with async_session_factory() as session:
        database_bytes = await session.scalar(text("SELECT pg_database_size(current_database())"))
    ordered = sorted(latencies)
    return {
        "scope": (
            "single scratch shard; in-process ASGI; synthetic events; no external ingestion API"
        ),
        "repeat": repeat,
        "processes": processes,
        "events": args.events,
        "conditions": args.conditions,
        "ingestion_seconds": ingestion_seconds,
        "ingestion_projection_seconds": ingestion_projection_seconds,
        "projection_transaction_max_seconds": max(projection_attempts),
        "projection_transaction_total_seconds": sum(projection_attempts),
        "transaction_timing_note": "client-observed duration, includes lock acquisition and commit",
        "correction_seconds": correction_seconds,
        "correction": asdict(correction),
        "full_replay_seconds": full_seconds,
        "full_replay_events": full.events_processed,
        "checkpoint_position_json_bytes": checkpoint_bytes,
        "database_bytes": database_bytes,
        "api_p95_ms": ordered[int((len(ordered) - 1) * 0.95)],
        "api_status_counts": statuses,
        "process_lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "children_lifetime_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
        "overlapping_task_intervals": executor.overlaps if executor else 0,
        "child_pids": sorted(executor.pids) if executor else [],
        "economic_digest": corrected,
    }


async def run(args):
    if engine.url.database != "polypulse_phase7_bench" or secondary_engine is not None:
        raise RuntimeError("Use only the dedicated single-shard polypulse_phase7_bench database")
    async with async_session_factory() as session:
        if await session.scalar(
            select(func.count()).select_from(TrackedWallet)
        ) or await session.scalar(select(func.count()).select_from(SourceEvent)):
            raise RuntimeError("Benchmark database must be empty")
    reference = None
    try:
        for repeat in range(args.repeats):
            for processes in (0, 1, args.processes):
                result = await measure(args, processes, repeat)
                reference = reference or result["economic_digest"]
                if result["economic_digest"] != reference:
                    raise RuntimeError("Serial and parallel economic states differ")
                print(json.dumps({"parity": True, **result}), flush=True)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--conditions", type=int, default=100)
    parser.add_argument("--processes", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if (
        min(args.conditions, args.batch_size, args.repeats) < 1
        or args.processes < 2
        or args.events <= args.batch_size
    ):
        parser.error("Require positive counts, processes >= 2, and events > batch size")
    asyncio.run(run(args))
