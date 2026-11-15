"""Incrementally scan wallet transfer logs from the last completed ranges."""

import json

import httpx

from app.chain.decoder import TRANSFER_BATCH_TOPIC, TRANSFER_SINGLE_TOPIC, decode_log
from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from app.storage.database import engine
from scripts.pull_chain_slice import save_chain_events
from scripts.scan_chain_slice import CHECKPOINT, Scanner, save_checkpoint


async def scan_tail(wallet: str, end: int) -> dict:
    database = engine.url.database
    if not database or not database.replace("_", "").isalnum():
        raise ValueError("database name cannot be used for a scan checkpoint")
    checkpoint = CHECKPOINT.with_name(f"tail_checkpoint_{database}.json")
    state = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    base = json.loads(CHECKPOINT.read_text()) if CHECKPOINT.exists() else {}
    jn_path = CHECKPOINT.with_name("jn_tail_checkpoint.json")
    jn = json.loads(jn_path.read_text()) if jn_path.exists() else {}
    jn_wallet = "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"
    scanner = Scanner(state, checkpoint=checkpoint)
    client = PolygonRpcClient()
    inserted = 0
    async with httpx.AsyncClient(timeout=40, headers={"User-Agent": "PolyPulse/1.0"}) as http:
        for direction in ("from", "to"):
            key = f"{wallet}:{direction}"
            if key not in state:
                previous = base.get(key)
                if previous is None and wallet == jn_wallet:
                    previous = jn.get(direction)
                if previous is None:
                    raise ValueError(f"no completed scan checkpoint for {key}")
                state[key] = previous
            start = state[key] + 1
            topic = "0x" + wallet[2:].rjust(64, "0")
            while start <= end:
                last = min(start + 9999, end)
                logs = await scanner.get_logs(
                    http,
                    {
                        "fromBlock": hex(start),
                        "toBlock": hex(last),
                        "address": CTF_CONTRACT,
                        "topics": [
                            [TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC],
                            None,
                            topic if direction == "from" else None,
                            topic if direction == "to" else None,
                        ],
                    },
                )
                await scanner.prefetch_timestamps(http, logs)
                events = []
                for log in logs:
                    timestamp = await scanner.timestamp(client, int(log["blockNumber"], 16))
                    events.extend(decode_log(dict(log, blockTimestamp=timestamp), wallet))
                inserted += await save_chain_events(events)
                state[key] = last
                save_checkpoint(state, checkpoint)
                start = last + 1
    return {"calls": scanner.calls, "errors": scanner.errors, "inserted": inserted}
