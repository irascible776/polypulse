#!/usr/bin/env python3
"""Resumable, wallet-filtered CTF transfer scan with bounded RPC usage."""

import asyncio
import json
import os
import time
from collections import deque
from pathlib import Path

import httpx
from sqlalchemy import func, select

from app.chain.decoder import TRANSFER_BATCH_TOPIC, TRANSFER_SINGLE_TOPIC, decode_log
from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from app.storage.database import async_session_factory
from app.storage.models import SourceEvent
from scripts.pull_chain_slice import WALLETS, save_chain_events

ENDPOINT = "https://polygon-bor-rpc.publicnode.com"
FALLBACK_ENDPOINT = "https://polygon.drpc.org/"
CHECKPOINT = Path(__file__).with_name("chain_scan_checkpoint.json")
MARGIN_BLOCKS = 100
MAX_CALLS = 6000


class ScanStopped(Exception):
    pass


class Scanner:
    def __init__(self, state=None, max_calls=MAX_CALLS, checkpoint=None):
        self.state = state if state is not None else {}
        self.calls = self.state.get("_log_calls", 0)
        self.errors = self.state.get("_log_errors", 0)
        self.recent = deque(self.state.get("_recent_errors", []), maxlen=100)
        self.lock = asyncio.Lock()
        self.cache = {}
        self.missing_timestamps = 0
        self.max_calls = max_calls
        self.checkpoint = checkpoint if checkpoint is not None else CHECKPOINT

    async def get_logs(self, http, params):
        endpoint = ENDPOINT
        permanent_providers = set()
        for attempt in range(12):
            async with self.lock:
                if self.calls >= self.max_calls:
                    raise ScanStopped(f"{self.max_calls:,} actual eth_getLogs calls reached")
                if len(self.recent) == 100 and sum(self.recent) > 20:
                    raise ScanStopped("error rate exceeded 20% over 100 calls")
                self.calls += 1
                call_number = self.calls
                self.state["_log_calls"] = self.calls
                save_checkpoint(self.state, self.checkpoint)
            error = None
            try:
                response = await http.post(
                    endpoint,
                    json={
                        "jsonrpc": "2.0",
                        "id": call_number,
                        "method": "eth_getLogs",
                        "params": [params],
                    },
                )
                if response.status_code >= 400:
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:250]}")
                body = response.json()
                if body.get("error"):
                    raise RuntimeError(str(body["error"]))
                logs = body["result"]
            except (httpx.HTTPError, ValueError, KeyError, RuntimeError) as exc:
                error = str(exc)
            permanent = bool(error and "403" in error and "archive" in error.lower())
            async with self.lock:
                if not permanent:
                    self.recent.append(error is not None)
                if error and not permanent:
                    self.errors += 1
                self.state["_log_errors"] = self.errors
                self.state["_recent_errors"] = list(self.recent)
                save_checkpoint(self.state, self.checkpoint)
                if call_number % 100 == 0:
                    print(f"PROGRESS calls={self.calls} errors={self.errors}", flush=True)
                if len(self.recent) == 100 and sum(self.recent) > 20:
                    raise ScanStopped("error rate exceeded 20% over 100 calls")
            if not error:
                return logs
            if permanent:
                print(f"PERMANENT archive 403 endpoint={endpoint} range={params}", flush=True)
                permanent_providers.add(endpoint)
                if endpoint == ENDPOINT:
                    endpoint = FALLBACK_ENDPOINT
                    continue
                unscanned = self.state.setdefault("_unscanned", [])
                unscanned.append({"params": params, "providers": sorted(permanent_providers)})
                save_checkpoint(self.state, self.checkpoint)
                raise RuntimeError(f"UNSCANNED: archive access denied by both providers: {params}")
            print(f"RPC ERROR call={call_number}: {error}", flush=True)
            if attempt == 5 and endpoint == ENDPOINT:
                endpoint = FALLBACK_ENDPOINT
            if attempt == 11:
                self.state.setdefault("_unscanned", []).append({"params": params, "reason": error})
                save_checkpoint(self.state, self.checkpoint)
                raise RuntimeError(f"scan request failed after retries: {error}")
            await asyncio.sleep(min(0.5 * 2**attempt, 16))
        raise RuntimeError("scan request failed")

    async def timestamp(self, client, block):
        if block not in self.cache:
            item = await client.call("eth_getBlockByNumber", [hex(block), False])
            if not item or not item.get("timestamp"):
                raise ValueError(f"missing timestamp for block {block}")
            self.cache[block] = item["timestamp"]
        return self.cache[block]

    async def prefetch_timestamps(self, http, logs):
        blocks = sorted({int(log["blockNumber"], 16) for log in logs} - self.cache.keys())
        for start in range(0, len(blocks), 50):
            batch = blocks[start : start + 50]
            payload = [
                {
                    "jsonrpc": "2.0",
                    "id": i,
                    "method": "eth_getBlockByNumber",
                    "params": [hex(block), False],
                }
                for i, block in enumerate(batch)
            ]
            for attempt in range(5):
                try:
                    response = await http.post(ENDPOINT, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    if not isinstance(data, list) or len(data) != len(batch):
                        raise RuntimeError("incomplete block timestamp batch")
                    for item in data:
                        if not item.get("result") or not item["result"].get("timestamp"):
                            raise RuntimeError(f"block timestamp unavailable: {item}")
                    for item in data:
                        self.cache[batch[item["id"]]] = item["result"]["timestamp"]
                    break
                except (httpx.HTTPError, ValueError, KeyError, RuntimeError):
                    if attempt == 4:
                        raise
                    await asyncio.sleep(0.5 * 2**attempt)


async def first_block(client, target, latest):
    low, high = 0, latest
    while low < high:
        mid = (low + high) // 2
        block = await client.call("eth_getBlockByNumber", [hex(mid), False])
        if not block or not block.get("timestamp"):
            raise RuntimeError(f"cannot resolve block {mid}")
        if int(block["timestamp"], 16) < target:
            low = mid + 1
        else:
            high = mid
    return max(0, low - MARGIN_BLOCKS)


def save_checkpoint(state, path=CHECKPOINT):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(state, indent=2) + "\n")
    os.replace(temp, path)


async def main():
    state = json.loads(CHECKPOINT.read_text()) if CHECKPOINT.exists() else {}
    scanner = Scanner(state)
    client = PolygonRpcClient(endpoints=[ENDPOINT])
    end = state.get("_reference_block")
    if end is None:
        end = await client.get_latest_block_number() - 32
        state["_reference_block"] = end
        save_checkpoint(state)
    print(f"REFERENCE_BLOCK {end} margin={MARGIN_BLOCKS} endpoint={ENDPOINT}", flush=True)
    async with async_session_factory() as session:
        earliest = {}
        for label, wallet in WALLETS:
            value = await session.scalar(
                select(func.min(SourceEvent.event_time)).where(SourceEvent.wallet == wallet)
            )
            if value is None:
                raise RuntimeError(f"no source events for {wallet}")
            earliest[wallet] = await first_block(client, int(value.timestamp()), end)
            print(
                f"START {label} timestamp={value.isoformat()} block={earliest[wallet]}", flush=True
            )
    estimate = sum(2 * ((max(0, end - start + 1) + 9999) // 10000) for start in earliest.values())
    print(f"ESTIMATED eth_getLogs calls={estimate}", flush=True)
    if estimate > 5000:
        raise ScanStopped("estimated calls exceed approved 5,000 limit")
    async with httpx.AsyncClient(timeout=40, headers={"User-Agent": "PolyPulse/1.0"}) as http:
        for label, wallet in WALLETS:
            started = time.perf_counter()
            before_calls, before_errors = scanner.calls, scanner.errors
            wallet_topic = "0x" + wallet[2:].rjust(64, "0")
            for direction in ("from", "to"):
                key = f"{wallet}:{direction}"
                start = max(earliest[wallet], state.get(key, earliest[wallet] - 1) + 1)
                while start <= end:
                    ranges = [
                        (n, min(n + 9999, end))
                        for n in range(start, min(start + 40000, end + 1), 10000)
                    ]
                    topics = [
                        [TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC],
                        None,
                        wallet_topic if direction == "from" else None,
                        wallet_topic if direction == "to" else None,
                    ]

                    async def fetch(a, b):
                        return await scanner.get_logs(
                            http,
                            {
                                "fromBlock": hex(a),
                                "toBlock": hex(b),
                                "address": CTF_CONTRACT,
                                "topics": topics,
                            },
                        )

                    pages = await asyncio.gather(*(fetch(a, b) for a, b in ranges))
                    for (a, b), logs in zip(ranges, pages, strict=True):
                        try:
                            await scanner.prefetch_timestamps(http, logs)
                        except Exception as exc:
                            missing_blocks = {
                                int(log["blockNumber"], 16)
                                for log in logs
                                if int(log["blockNumber"], 16) not in scanner.cache
                            }
                            missing_rows = sum(
                                int(log["blockNumber"], 16) in missing_blocks for log in logs
                            )
                            scanner.missing_timestamps += missing_rows
                            print(
                                f"TIMESTAMP FAILURE skipped_logs={missing_rows} "
                                f"total={scanner.missing_timestamps} range={a}-{b}: {exc}",
                                flush=True,
                            )
                            raise
                        events = []
                        for log in logs:
                            try:
                                ts = await scanner.timestamp(client, int(log["blockNumber"], 16))
                                events.extend(decode_log(dict(log, blockTimestamp=ts), wallet))
                            except Exception as exc:
                                scanner.missing_timestamps += 1
                                raise RuntimeError(
                                    f"cannot checkpoint block range {a}-{b}: {exc}"
                                ) from exc
                        await save_chain_events(events)
                        state[key] = b
                        save_checkpoint(state)
                    start = ranges[-1][1] + 1
            print(
                f"WALLET {label} calls={scanner.calls - before_calls} "
                f"errors={scanner.errors - before_errors} "
                f"seconds={time.perf_counter() - started:.3f} "
                f"missing_timestamps={scanner.missing_timestamps}",
                flush=True,
            )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except ScanStopped as exc:
        print(f"HARD STOP: {exc}", flush=True)
