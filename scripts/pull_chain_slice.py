#!/usr/bin/env python3
"""
scripts/pull_chain_slice.py

Phase 4 Chain Slice & Polygon RPC Headline Experiment.
Fetches a targeted slice of Polygon on-chain ground truth data:
1. Historical transaction receipts for trades & redeems (including edge-case redeems).
2. Live block range slice via eth_getLogs on Gnosis CTF contract.
3. Decodes CTF PositionSplit, PositionsMerge, PayoutRedemption, and
   ERC-1155 TransferSingle/TransferBatch.
4. Idempotently writes decoded events into the chain_events PostgreSQL table.
5. Emits measured RPC latency and throughput benchmarks.
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.chain.decoder import decode_log
from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from app.storage.database import (
    secondary_engine,
    validate_wallet_shard,
    wallet_read_session,
    wallet_session_factory,
)
from app.storage.models import ChainEvent, SourceEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("pull_chain_slice")
block_timestamps: dict[int, str] = {}

WALLETS = [
    ("AppleTiiime", "0x951634357346bb291df117f9ac410100feea2104"),
    ("JnStPrdctns", "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"),
]


async def decode_wallet_logs(client: PolygonRpcClient, logs: list[dict], wallet: str) -> tuple:
    """Resolve timestamps once per block; skip and count logs without one."""
    decoded = []
    missing_timestamps = 0
    for log in logs:
        if log.get("address", "").lower() != CTF_CONTRACT.lower():
            continue
        block = int(log["blockNumber"], 16)
        if block not in block_timestamps:
            try:
                result = await client.call("eth_getBlockByNumber", [hex(block), False])
                if not result or not result.get("timestamp"):
                    raise ValueError("missing block timestamp")
                block_timestamps[block] = result["timestamp"]
            except Exception as exc:
                logger.warning("Unable to fetch timestamp for block %s: %s", block, exc)
                missing_timestamps += 1
                continue
        enriched = dict(log, blockTimestamp=block_timestamps[block])
        decoded.extend(decode_log(enriched, target_wallet=wallet))
    return decoded, missing_timestamps


async def pull_transaction_receipt_slice(
    client: PolygonRpcClient,
    wallet: str,
) -> tuple[list[dict], list[int], list[str]]:
    """Fetch each known trade/redeem transaction once in JSON-RPC batches."""
    async with wallet_read_session(wallet) as session:
        stmt = (
            select(SourceEvent.transaction_hash)
            .where(
                SourceEvent.wallet == wallet,
                SourceEvent.transaction_hash.is_not(None),
                SourceEvent.event_type.in_(["TRADE", "REDEEM"]),
            )
            .distinct()
        )
        hashes = (await session.execute(stmt)).scalars().all()

    logger.info("Fetching %d distinct transaction receipts for %s", len(hashes), wallet)
    all_logs = []
    blocks = []
    missing = []
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "PolyPulse/1.0"}) as http:
        for start in range(0, len(hashes), 20):
            batch = hashes[start : start + 20]
            payload = [
                {"jsonrpc": "2.0", "id": i, "method": "eth_getTransactionReceipt", "params": [h]}
                for i, h in enumerate(batch)
            ]
            for attempt in range(5):
                try:
                    response = await http.post(client.current_endpoint, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    if not isinstance(data, list) or len(data) != len(batch):
                        raise RuntimeError("incomplete batch response")
                    by_id = {item["id"]: item for item in data}
                    if any(i not in by_id or by_id[i].get("error") for i in range(len(batch))):
                        raise RuntimeError("missing or failed response in batch")
                    break
                except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                    if attempt == 4:
                        raise RuntimeError(f"receipt batch {start} failed") from exc
                    await asyncio.sleep(0.5 * 2**attempt)
            for item in data:
                receipt = item["result"]
                if receipt is None:
                    missing.append(batch[item["id"]])
                    continue
                blocks.append(int(receipt["blockNumber"], 16))
                all_logs.extend(receipt.get("logs", []))
            if (start + len(batch)) % 200 == 0:
                logger.info("%s: fetched %d/%d receipts", wallet, start + len(batch), len(hashes))
    return all_logs, blocks, missing


async def pull_recent_block_slice(
    client: PolygonRpcClient,
    block_count: int = 50,
) -> list[dict]:
    """Fetch logs from the latest Polygon blocks for the CTF contract."""
    latest = await client.get_latest_block_number()
    from_block = latest - block_count
    logger.info(
        "Fetching CTF logs for recent blocks [%d -> %d] (%d blocks)...",
        from_block,
        latest,
        block_count,
    )
    logs = await client.get_logs_adaptive(
        from_block=from_block,
        to_block=latest,
        address=CTF_CONTRACT,
        max_chunk=25,
    )
    return logs


async def save_chain_events(events: list, session: AsyncSession | None = None) -> int:
    """Save decoded events to chain_events table with ON CONFLICT DO NOTHING."""
    if not events:
        return 0

    if session is None:
        wallets = {event.wallet for event in events}
        if secondary_engine is not None and len(wallets) != 1:
            raise ValueError("A chain-event batch must contain exactly one wallet when sharded")
        wallet = events[0].wallet
        async with wallet_session_factory(wallet)() as owned_session:
            await validate_wallet_shard(owned_session, wallet)
            inserted = await save_chain_events(events, owned_session)
            await owned_session.commit()
            return inserted

    inserted_count = 0
    now_ts = datetime.now(timezone.utc)

    for ev in events:
        stmt = (
            insert(ChainEvent)
            .values(
                wallet=ev.wallet,
                block_number=ev.block_number,
                block_time=ev.block_time,
                transaction_hash=ev.transaction_hash,
                log_index=ev.log_index,
                batch_index=ev.batch_index,
                block_hash=ev.raw_log["blockHash"],
                contract_address=ev.contract_address,
                event_name=ev.event_name,
                condition_id=ev.condition_id,
                token_id=ev.token_id,
                from_address=ev.from_address,
                to_address=ev.to_address,
                amount=ev.amount,
                raw_log=ev.raw_log,
                created_at=now_ts,
            )
            .on_conflict_do_nothing(
                index_elements=["wallet", "transaction_hash", "log_index", "batch_index"]
            )
            .returning(ChainEvent.id)
        )
        res = await session.execute(stmt)
        if res.scalar_one_or_none() is not None:
            inserted_count += 1
    return inserted_count


async def main():
    t_start = time.perf_counter()
    client = PolygonRpcClient()

    print("\n=======================================================")
    print("STARTING POLYGON RPC CHAIN-SLICE EXPERIMENT (PHASE 4)")
    print("=======================================================")
    print(f"Primary RPC Endpoint:  {client.current_endpoint}")
    print(f"CTF Contract Address:  {CTF_CONTRACT}")

    total_logs_fetched = 0
    total_events_decoded = 0
    total_events_saved = 0

    # 1. Pull historical receipt slices for tracked wallets
    for label, wallet in WALLETS:
        t0 = time.perf_counter()
        raw_logs, blocks, missing = await pull_transaction_receipt_slice(client, wallet)
        total_logs_fetched += len(raw_logs)

        decoded, missing_timestamps = await decode_wallet_logs(client, raw_logs, wallet)
        total_events_decoded += len(decoded)

        saved = await save_chain_events(decoded)
        total_events_saved += saved
        dur = time.perf_counter() - t0
        print(f"\n[{label}] {wallet}:")
        print(f"  Raw Logs Fetched:    [MEASURED] {len(raw_logs)}")
        print(f"  Events Decoded:      [MEASURED] {len(decoded)}")
        print(f"  New Events Saved:    [MEASURED] {saved}")
        print(f"  Slice Pull Time:     [MEASURED] {dur:.3f}s")
        print(f"  Earliest block:      [MEASURED] {min(blocks) if blocks else None}")
        print(f"  Missing receipts:    [MEASURED] {len(missing)}")
        print(f"  Missing timestamps:  [MEASURED] {missing_timestamps}")
        for tx in missing:
            print(f"  MISSING RECEIPT {tx}")

    t_total = time.perf_counter() - t_start
    stats = client.get_latency_stats()

    print("\n=======================================================")
    print("PHASE 4 BENCHMARK SUMMARY [MEASURED]")
    print("=======================================================")
    print(f"Total RPC Requests:      [MEASURED] {int(stats['count'])}")
    print(f"Total Logs Fetched:      [MEASURED] {total_logs_fetched}")
    print(f"Total Events Decoded:    [MEASURED] {total_events_decoded}")
    print(f"Total Events Saved:      [MEASURED] {total_events_saved}")
    print(f"Total Wall-Clock Time:   [MEASURED] {t_total:.3f}s")
    print(f"RPC Latency Min:         [MEASURED] {stats['min'] * 1000:.2f}ms")
    print(f"RPC Latency Avg:         [MEASURED] {stats['avg'] * 1000:.2f}ms")
    print(f"RPC Latency Max:         [MEASURED] {stats['max'] * 1000:.2f}ms")
    print("=======================================================\n")


if __name__ == "__main__":
    asyncio.run(main())
