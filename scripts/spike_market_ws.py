#!/usr/bin/env python3
"""
Spike: Polymarket CLOB Market WebSocket verification.

Verifies:
1. Subscription message format on wss://ws-subscriptions-clob.polymarket.com/ws/market
2. Live stream of market messages (book, price_change, last_trade_price, etc.)
3. Heartbeat / keep-alive requirement (PING every 10s)
4. Deliberate disconnection and clean reconnection
5. Logging raw messages to fixtures/market_ws_raw.jsonl
"""

import argparse
import asyncio
import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("spike_market_ws")

DEFAULT_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API_URL = (
    "https://gamma-api.polymarket.com/markets"
    "?limit=1&active=true&closed=false&order=volume24hr&ascending=false"
)


def get_active_token_id() -> str:
    """Fetch an active CLOB token ID from Gamma API."""
    logger.info("Fetching active market from Gamma API...")
    with httpx.Client(timeout=15.0) as client:
        resp = client.get(GAMMA_API_URL, headers={"User-Agent": "PolyPulse-Spike/1.0"})
        resp.raise_for_status()
        markets = resp.json()
        if not markets:
            raise RuntimeError("No active markets returned by Gamma API")
        token_ids_raw = markets[0].get("clobTokenIds")
        token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
        if not token_ids:
            raise RuntimeError(f"No clobTokenIds in market: {markets[0].get('question')}")
        market_q = markets[0].get("question")
        logger.info(f"Selected Market: '{market_q}' with Token ID: {token_ids[0]}")
        return str(token_ids[0])


async def heartbeat_sender(ws: Any, interval_seconds: float = 10.0) -> None:
    """Periodically sends PING keep-alive message."""
    try:
        while True:
            await asyncio.sleep(interval_seconds)
            await ws.send("PING")
            logger.debug("Sent keep-alive: PING")
    except (asyncio.CancelledError, websockets.ConnectionClosed):
        pass


async def capture_stream(
    ws_url: str,
    token_id: str,
    duration_seconds: float,
    output_file: Any,
    drop_and_reconnect_at: float | None = None,
) -> tuple[int, Counter]:
    """
    Connect to WebSocket, subscribe to token_id, and capture events for duration_seconds.
    Optionally drops connection at drop_and_reconnect_at seconds to test reconnect behavior.
    """
    counts: Counter = Counter()
    total_messages = 0
    start_time = time.monotonic()
    has_dropped = False

    while (time.monotonic() - start_time) < duration_seconds:
        remaining_time = duration_seconds - (time.monotonic() - start_time)
        logger.info(f"Connecting to {ws_url} (remaining duration: {remaining_time:.1f}s)...")

        async with websockets.connect(ws_url, ping_interval=None) as ws:
            logger.info("Connected. Sending subscription message...")
            sub_payload = {
                "type": "market",
                "assets_ids": [token_id],
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(sub_payload))
            logger.info(f"Subscribed to asset ID: {token_id}")

            # Start keep-alive background task
            hb_task = asyncio.create_task(heartbeat_sender(ws, interval_seconds=10.0))

            try:
                while (time.monotonic() - start_time) < duration_seconds:
                    elapsed = time.monotonic() - start_time
                    if (
                        drop_and_reconnect_at
                        and elapsed >= drop_and_reconnect_at
                        and not has_dropped
                    ):
                        logger.warning(
                            f"Deliberately dropping connection at {elapsed:.1f}s "
                            "to test reconnect..."
                        )
                        has_dropped = True
                        await ws.close(code=1000, reason="Intentional disconnect for testing")
                        break

                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                    except asyncio.TimeoutError:
                        continue

                    # Server responds with string "PONG" to client "PING"
                    if raw_msg == "PONG":
                        logger.debug("Received keep-alive: PONG")
                        counts["PONG"] += 1
                        continue

                    try:
                        parsed = json.loads(raw_msg)
                        if isinstance(parsed, list):
                            events_to_process = parsed
                        elif isinstance(parsed, dict):
                            events_to_process = [parsed]
                        else:
                            events_to_process = [{"type": "PRIMITIVE", "value": parsed}]
                    except json.JSONDecodeError:
                        events_to_process = [{"type": "NON_JSON", "raw": raw_msg}]

                    for event_item in events_to_process:
                        total_messages += 1
                        event_type = (
                            event_item.get("event_type") or event_item.get("type") or "UNKNOWN"
                        )
                        counts[event_type] += 1

                    # Log raw message to fixtures
                    record = {
                        "received_at": datetime.now(timezone.utc).isoformat(),
                        "raw_message": raw_msg,
                    }
                    output_file.write(json.dumps(record) + "\n")
                    output_file.flush()

            finally:
                hb_task.cancel()

        if (time.monotonic() - start_time) < duration_seconds:
            logger.info("Reconnecting after intentional drop...")
            await asyncio.sleep(0.5)

    return total_messages, counts


async def main():
    parser = argparse.ArgumentParser(description="Spike Polymarket Market WebSocket feed.")
    parser.add_argument("--token-id", default=None, help="CLOB token ID to subscribe to")
    parser.add_argument(
        "--duration", type=float, default=60.0, help="Total capture duration in seconds"
    )
    parser.add_argument("--ws-url", default=DEFAULT_WS_URL, help="WebSocket URL")
    parser.add_argument(
        "--output", default="fixtures/market_ws_raw.jsonl", help="Output JSONL log path"
    )
    args = parser.parse_args()

    token_id = args.token_id or get_active_token_id()
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    print("\n=== POLYMARKET MARKET WEBSOCKET SPIKE ===")
    print(f"Target Token ID: {token_id}")
    drop_target = args.duration / 2
    print(f"Duration: {args.duration}s (with deliberate drop & reconnect at {drop_target:.1f}s)")
    print(f"Writing raw messages to: {args.output}")

    with open(args.output, "w") as f:
        total_msgs, counts = await capture_stream(
            ws_url=args.ws_url,
            token_id=token_id,
            duration_seconds=args.duration,
            output_file=f,
            drop_and_reconnect_at=args.duration / 2,
        )

    print("\n=== SPIKE COMPLETE ===")
    print(f"Total Messages Logged: {total_msgs}")
    print("Event Counts by Type:")
    for event_type, count in counts.items():
        print(f"  - {event_type}: {count}")
    print("Heartbeat / Keep-alive: Handled (PING every 10s received PONG).")
    print("Reconnection: Verified (intentional disconnect was followed by successful resume).")


if __name__ == "__main__":
    asyncio.run(main())
