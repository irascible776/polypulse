#!/usr/bin/env python3
"""
Listen to Polymarket Market WebSocket on active markets until a
'last_trade_price' event appears (up to 300s).
Saves the event to fixtures/sample_last_trade_price.json.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any

import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("capture_last_trade")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL = (
    "https://gamma-api.polymarket.com/markets"
    "?limit=5&active=true&closed=false&order=volume24hr&ascending=false"
)


def get_active_tokens() -> list[str]:
    # First try cached top markets from fixtures
    if os.path.exists("fixtures/top_markets.json"):
        try:
            with open("fixtures/top_markets.json") as f:
                markets = json.load(f)
            tokens = []
            for m in markets:
                ids_raw = m.get("clobTokenIds")
                if ids_raw:
                    ids = json.loads(ids_raw) if isinstance(ids_raw, str) else ids_raw
                    tokens.extend([str(i) for i in ids[:2]])
            if tokens:
                logger.info(f"Loaded {len(tokens)} active tokens from fixtures/top_markets.json")
                return tokens
        except Exception as err:
            logger.warning(f"Could not read cached markets: {err}")

    # Fallback: Query Gamma API with retry
    for attempt in range(3):
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(GAMMA_URL, headers={"User-Agent": "PolyPulse/1.0"})
                resp.raise_for_status()
                markets = resp.json()
                tokens = []
                for m in markets:
                    ids_raw = m.get("clobTokenIds")
                    if ids_raw:
                        ids = json.loads(ids_raw) if isinstance(ids_raw, str) else ids_raw
                        tokens.extend([str(i) for i in ids[:2]])
                if tokens:
                    return tokens
        except Exception as err:
            logger.warning(f"Gamma API attempt {attempt + 1} failed: {err}")
            time.sleep(1.0)

    # Fallback to known active token IDs
    return [
        "35090254409399344650860070211816464534198960801264823587796928404808383727584",
        "17037715451587457598270390992151934195448806241696976473961259598235638233970",
        "86132577780664017779119549716911163213893271058373921763094613555010405925848",
        "49805669707853625159846501481513113748599988720421725806305656995932631560212",
    ]


async def heartbeat_sender(ws: Any) -> None:
    try:
        while True:
            await asyncio.sleep(10.0)
            await ws.send("PING")
    except (asyncio.CancelledError, websockets.ConnectionClosed):
        pass


async def capture_until_trade(max_wait_seconds: float = 300.0) -> dict[str, Any] | None:
    tokens = get_active_tokens()
    start_time = time.monotonic()

    async with websockets.connect(WS_URL, ping_interval=None) as ws:
        payload = {
            "type": "market",
            "assets_ids": tokens,
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(payload))
        logger.info(f"Subscribed to {len(tokens)} tokens. Listening for 'last_trade_price'...")

        hb_task = asyncio.create_task(heartbeat_sender(ws))
        try:
            while (time.monotonic() - start_time) < max_wait_seconds:
                try:
                    raw_msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                except asyncio.TimeoutError:
                    continue

                if raw_msg == "PONG":
                    continue

                try:
                    parsed = json.loads(raw_msg)
                    events = parsed if isinstance(parsed, list) else [parsed]
                except Exception:
                    continue

                for e in events:
                    event_type = e.get("event_type") or e.get("type")
                    if event_type == "last_trade_price":
                        logger.info("Captured last_trade_price event!")
                        with open("fixtures/sample_last_trade_price.json", "w") as f:
                            json.dump(e, f, indent=2)
                        return e
        finally:
            hb_task.cancel()

    logger.warning("Reached timeout without last_trade_price event")
    return None


if __name__ == "__main__":
    event = asyncio.run(capture_until_trade(300.0))
    if event:
        print("SUCCESS! Captured last_trade_price:")
        print(json.dumps(event, indent=2))
    else:
        print("Timed out.")
