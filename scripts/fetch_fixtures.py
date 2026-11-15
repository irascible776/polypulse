"""
Script to regenerate raw fixture data for approved Polymarket test wallets.
Uses polite cursor-based pagination from Data API v2.
"""

import json
import logging
import time
from pathlib import Path

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fetch_fixtures")

BASE_URL = "https://data-api.polymarket.com/v2/activity"

APPROVED_WALLETS = {
    "appletiiime": "0x951634357346bb291df117f9ac410100feea2104",
    "jnstprdctns": "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92",
}


def fetch_wallet_fixtures(label: str, address: str, output_dir: Path) -> None:
    logger.info("Starting fetch for %s (%s)", label, address)
    client = httpx.Client(timeout=15.0)

    cursor = None
    page = 1
    total_events = 0

    while True:
        params: dict[str, str | int] = {
            "user": address,
            "start": 1,
            "sort_direction": "ASC",
            "limit": 500,
        }
        if cursor:
            params["cursor"] = cursor

        logger.info("Fetching %s page %d...", label, page)
        resp = client.get(BASE_URL, params=params)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "5"))
            logger.warning("Rate limited (429). Sleeping %d seconds...", retry_after)
            time.sleep(retry_after)
            continue

        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])
        pagination = payload.get("pagination", {})

        out_path = output_dir / f"{label}_page_{page}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        total_events += len(data)
        logger.info("Saved %d events to %s", len(data), out_path.name)

        has_more = pagination.get("has_more", False)
        next_cursor = pagination.get("next_cursor")

        if not has_more or not next_cursor:
            logger.info("Completed %s: %d total events across %d pages", label, total_events, page)
            break

        cursor = next_cursor
        page += 1
        time.sleep(1.0)  # Polite pacing


def main() -> None:
    fixtures_dir = Path("fixtures")
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    for label, address in APPROVED_WALLETS.items():
        fetch_wallet_fixtures(label, address, fixtures_dir)


if __name__ == "__main__":
    main()
