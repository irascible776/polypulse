"""Synthetic CPU-only condition benchmark; excludes ingestion, SQL and API latency.

Run after the testing hold is lifted. No database is read or written.
"""

import argparse
import asyncio
import hashlib
import json
import time
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal

from app.accounting.engine import compute_condition_events
from app.accounting.executor import ConditionExecutor
from app.accounting.types import AccountingEvent

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def make_event(index, conditions):
    condition = str(index % conditions)
    phase = (index // conditions) % 4
    return AccountingEvent(
        source_event_id=str(index),
        api_order=index + 1,
        event_type="REDEEM" if phase == 3 else "TRADE",
        event_time=NOW,
        wallet="synthetic",
        condition_id=condition,
        token_id=None if phase == 3 else condition,
        outcome_index=0,
        side="SELL" if phase == 2 else "BUY",
        size=Decimal(3),
        usdc_size=Decimal(2 if phase >= 2 else 1),
        price_reported=Decimal(0),
        cash_price_gap=False,
        raw={},
    )


async def measure(events, conditions, processes, batch_size):
    executor = ConditionExecutor(processes) if processes else None
    positions = {}
    digest = hashlib.sha256()
    pnl = Decimal(0)
    overlaps = 0
    pids = set()
    started = time.perf_counter()
    try:
        for offset in range(0, events, batch_size):
            groups = {}
            selected = [
                make_event(i, conditions) for i in range(offset, min(events, offset + batch_size))
            ]
            for event in selected:
                groups.setdefault(event.condition_id, []).append(event)
            work = [
                (condition, batch, positions.get(condition, {}))
                for condition, batch in groups.items()
            ]
            if executor:
                results = await executor.compute("synthetic", work, NOW, 120)
                batches = [result[3] for result in results]
                last_end = 0
                for result in sorted(results, key=lambda result: result[5]):
                    pids.add(result[4])
                    overlaps += result[5] < last_end
                    last_end = max(last_end, result[6])
            else:
                batches = [
                    compute_condition_events("synthetic", c, ev, pos, NOW) for c, ev, pos in work
                ]
            effects = {effect.event.api_order: effect for batch in batches for effect in batch}
            for event in selected:
                effect = effects[event.api_order]
                if effect.position is not None:
                    state = effect.position
                    positions.setdefault(state.condition_id, {})[
                        (state.condition_id, state.token_id)
                    ] = state
                    pnl += effect.result.success.realized_pnl_delta
                digest.update(json.dumps(asdict(effect), sort_keys=True, default=str).encode())
        elapsed = time.perf_counter() - started
    finally:
        if executor:
            await executor.close()
    return {
        "scope": "synthetic CPU computation plus result serialization; no SQL or API",
        "processes": processes,
        "events": events,
        "conditions": conditions,
        "batch_size": batch_size,
        "seconds": elapsed,
        "events_per_second": events / elapsed,
        "effect_digest": digest.hexdigest(),
        "realized_pnl": str(pnl),
        "child_pids": sorted(pids),
        "overlapping_task_intervals": overlaps,
    }


async def run(args):
    reference = None
    for repeat in range(args.repeats):
        for processes in (0, 1, args.processes):
            result = await measure(args.events, args.conditions, processes, args.batch_size)
            identity = (result["effect_digest"], result["realized_pnl"])
            if reference is None:
                reference = identity
            if identity != reference:
                raise RuntimeError("Parallel effects differ from serial effects")
            print(json.dumps({"repeat": repeat, "parity": True, **result}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=100000)
    parser.add_argument("--conditions", type=int, default=100)
    parser.add_argument("--processes", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if min(args.events, args.conditions, args.processes, args.batch_size, args.repeats) < 1:
        parser.error("All counts must be positive")
    asyncio.run(run(args))
