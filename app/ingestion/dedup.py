"""Identity based on economic fields and deterministic enrichment pairing."""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.ingestion.identifiers import normalize_decimal_str


def dedup_key(row: dict[str, Any]) -> str:
    event_type = str(row.get("event_type") or row.get("type") or "").upper()
    timestamp = row.get("event_time") or row.get("timestamp")
    if isinstance(timestamp, datetime):
        timestamp = int(timestamp.astimezone(timezone.utc).timestamp())
    asset = row.get("condition_id") if event_type == "REDEEM" else row.get("token_id")
    parts = [
        str(row.get("wallet") or row.get("proxy_wallet") or "").lower(),
        event_type,
        str(row.get("transaction_hash") or "").lower(),
        str(asset or ""),
        "" if event_type == "REDEEM" else str(row.get("side") or "").upper(),
        normalize_decimal_str(row.get("size")),
        normalize_decimal_str(row.get("usdc_size")),
        str(timestamp or 0),
    ]
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


def pair_enrichment(rows: list[dict[str, Any]]) -> dict[int, int]:
    """Return unenriched-to-enriched IDs for currently unpaired rows in one group."""
    ordered = sorted(rows, key=lambda row: row["api_order"])
    used = {row["superseded_by"] for row in ordered if row.get("superseded_by") is not None}
    live = [row for row in ordered if row.get("superseded_by") is None]
    incomplete = [
        row
        for row in live
        if row.get("outcome_index") == 999 or not (row.get("raw") or {}).get("event_slug")
    ]
    enriched = [row for row in live if row not in incomplete and row["id"] not in used]
    return {old["id"]: new["id"] for old, new in zip(incomplete, enriched)}
