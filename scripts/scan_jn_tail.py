"""Scan the requested JnStPrdctns tail without touching the old-range checkpoint."""

import asyncio
import json
import time
from pathlib import Path

import httpx

import scripts.scan_chain_slice as scan
from app.chain.decoder import TRANSFER_BATCH_TOPIC, TRANSFER_SINGLE_TOPIC, decode_log
from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from scripts.pull_chain_slice import save_chain_events

WALLET = "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"
START = 97087241
END = 97294556


async def main():
    scan.CHECKPOINT = Path(__file__).with_name("jn_tail_checkpoint.json")
    state = json.loads(scan.CHECKPOINT.read_text()) if scan.CHECKPOINT.exists() else {}
    scanner = scan.Scanner(state, max_calls=100)
    client = PolygonRpcClient(endpoints=[scan.ENDPOINT])
    padded = "0x" + WALLET[2:].rjust(64, "0")
    estimate = 2 * ((END - START + 10000) // 10000)
    print(f"TAIL start={START} end={END} estimate={estimate} max=100", flush=True)
    started = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=40, headers={"User-Agent": "PolyPulse/1.0"}) as http:
            for direction in ("from", "to"):
                start = state.get(direction, START - 1) + 1
                while start <= END:
                    end = min(start + 9999, END)
                    topics = [
                        [TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC],
                        None,
                        padded if direction == "from" else None,
                        padded if direction == "to" else None,
                    ]
                    params = {
                        "fromBlock": hex(start),
                        "toBlock": hex(end),
                        "address": CTF_CONTRACT,
                        "topics": topics,
                    }
                    logs = await scanner.get_logs(http, params)
                    await scanner.prefetch_timestamps(http, logs)
                    events = []
                    for log in logs:
                        timestamp = await scanner.timestamp(client, int(log["blockNumber"], 16))
                        events.extend(decode_log(dict(log, blockTimestamp=timestamp), WALLET))
                    await save_chain_events(events)
                    state[direction] = end
                    scan.save_checkpoint(state)
                    start = end + 1
    finally:
        print(
            f"TAIL RESULT calls={scanner.calls} transient_errors={scanner.errors} "
            f"seconds={time.perf_counter() - started:.3f} checkpoint={state}",
            flush=True,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except scan.ScanStopped as exc:
        print(f"HARD STOP: {exc}", flush=True)
