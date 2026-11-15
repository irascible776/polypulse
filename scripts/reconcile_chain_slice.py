#!/usr/bin/env python3
"""
scripts/reconcile_chain_slice.py

Phase 4 On-Chain Ground Truth Reconciliation.
Directly compares off-chain derived positions in PostgreSQL against live Polygon
blockchain state via eth_call balanceOf(address, uint256) on the CTF contract.
Also audits decoded chain_events to verify transfer-in / split accounting.
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Ensure project root is in sys.path when script is executed directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.chain.reconcile import WALLETS, reconcile_wallet
from app.chain.reconcile import print_ingestion_freshness as print_ingestion_freshness
from app.chain.rpc import PolygonRpcClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("reconcile_chain")


async def main():
    client = PolygonRpcClient(endpoints=["https://polygon.drpc.org/"], max_retries=1)
    t0 = time.perf_counter()
    reference_block = await client.get_latest_block_number() - 32

    all_summaries = []
    for attempt in range(3):
        try:
            block = await client.call("eth_getBlockByNumber", [hex(reference_block), False])
            reference_time = datetime.fromtimestamp(int(block["timestamp"], 16), tz=timezone.utc)
            all_summaries = []
            for label, wallet in WALLETS:
                summary = await reconcile_wallet(
                    client,
                    label,
                    wallet,
                    reference_block=reference_block,
                    reference_time=reference_time,
                )
                all_summaries.append(summary)
            break
        except RuntimeError as exc:
            if "historical state" not in str(exc).lower() or attempt == 2:
                raise
            reference_block = await client.get_latest_block_number() - 32
            print(f"Historical state unavailable; retrying both wallets at block {reference_block}")

    dur = time.perf_counter() - t0
    stats = client.get_latency_stats()

    print("\n=======================================================")
    print("FINAL ON-CHAIN RECONCILIATION SUMMARY [MEASURED]")
    print("=======================================================")
    for s in all_summaries:
        print(
            f"• {s['label']}: MATCH={s['perfect_matches']} IN_FLIGHT={s['in_flight_count']} "
            f"MISMATCH={s['mismatches']} "
            f"UNKNOWN={s['query_failures']}"
        )
    print(f"Total RPC Calls:    [MEASURED] {int(stats['count'])}")
    print(f"RPC Latency Avg:    [MEASURED] {stats['avg'] * 1000:.2f}ms")
    print(f"Wall-Clock Time:    [MEASURED] {dur:.3f}s")
    print("=======================================================\n")


if __name__ == "__main__":
    asyncio.run(main())
