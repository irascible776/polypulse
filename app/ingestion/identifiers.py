import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from typing import Any

# Fields to explicitly exclude from identity hashing because they are mutable user profiles
PROFILE_EXCLUDED_FIELDS = {
    "name",
    "bio",
    "pseudonym",
    "profile_image",
    "profile_image_optimized",
}


def parse_json_with_decimal(json_str: str) -> Any:
    """Parse JSON string into Python objects using Decimal for floating point numbers."""
    return json.loads(json_str, parse_float=Decimal)


def normalize_decimal_str(val: Any) -> str:
    """
    Format and normalize numbers via Decimal to eliminate float rounding and formatting differences.
    Ensures '0.50', '0.5', '100', '100.0', '1E+2' produce identical canonical strings.
    Handles zeroes cleanly ('0', '0.0', '0.00' -> '0').
    """
    if val is None or val == "":
        return "0"
    try:
        d = Decimal(str(val))
    except Exception:
        return "0"

    if d.is_zero():
        return "0"

    normalized = format(d.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def compute_base_key(
    event: dict[str, Any],
    wallet: str | None = None,
) -> str:
    """
    Compute base SHA-256 hash from normalized deterministic event fields.
    Exact fields: wallet, transaction_hash, type, condition_id, token_id, side,
    outcome_index, size, usdc_size, price, timestamp.
    Numeric values are formatted via normalize_decimal_str.
    Profile fields (name, bio, pseudonym, profile_image*) are excluded.
    """
    w = str(event.get("proxy_wallet") or event.get("wallet") or wallet or "").strip().lower()
    tx_hash = str(event.get("transaction_hash") or "").strip().lower()
    event_type = str(event.get("type") or "").strip().upper()
    condition_id = str(event.get("condition_id") or "").strip().lower()
    token_id = str(event.get("token_id") or "").strip()
    side = str(event.get("side") or "").strip().upper()

    raw_outcome = event.get("outcome_index")
    outcome_index = str(raw_outcome if raw_outcome is not None else "")

    size = normalize_decimal_str(event.get("size"))
    usdc_size = normalize_decimal_str(event.get("usdc_size"))
    price = normalize_decimal_str(event.get("price"))
    timestamp = str(event.get("timestamp") or 0)

    raw_text = (
        f"wallet:{w}|"
        f"tx:{tx_hash}|"
        f"type:{event_type}|"
        f"cond:{condition_id}|"
        f"token:{token_id}|"
        f"side:{side}|"
        f"outcome:{outcome_index}|"
        f"size:{size}|"
        f"usdc:{usdc_size}|"
        f"price:{price}|"
        f"ts:{timestamp}"
    )

    return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()


def compute_source_event_id(
    base_key_or_event: str | dict[str, Any],
    occurrence: int = 0,
    wallet: str | None = None,
) -> str:
    """
    Construct source_event_id as base_key plus occurrence index: {base_key}:{occurrence}.
    """
    if isinstance(base_key_or_event, dict):
        base_key = compute_base_key(base_key_or_event, wallet=wallet)
    else:
        base_key = str(base_key_or_event)
    return f"{base_key}:{occurrence}"


class EventIdAssigner:
    """
    Stateless and deterministic event ID assigner.

    A complete group is defined as all events for one wallet within one timestamp second.
    When is_final=False, events belonging to the trailing timestamp second are held back
    until the next page arrives, preventing page boundaries from splitting a group.
    Within a complete group, events sharing the same base_key receive occurrences 0, 1, 2...
    in API arrival order.
    """

    def __init__(self, wallet: str | None = None) -> None:
        self.wallet = wallet
        self._held_events: list[dict[str, Any]] = []

    def reset(self) -> None:
        """Clear any held-back events from an aborted or previous walk."""
        self._held_events.clear()

    def assign_ids(
        self,
        events: list[dict[str, Any]],
        is_final: bool = False,
    ) -> list[tuple[dict[str, Any], str]]:
        """
        Process a list of events from the API.
        Returns a list of (event, source_event_id) for all completed timestamp groups.
        If is_final=False, events belonging to the trailing timestamp second are held back.
        If is_final=True, all held and passed events are completed and returned.
        """
        combined = self._held_events + list(events)
        self._held_events = []

        if not combined:
            return []

        if not is_final:
            last_ts = combined[-1].get("timestamp")
            # Find the split index where trailing events match last_ts
            split_idx = len(combined)
            while split_idx > 0 and combined[split_idx - 1].get("timestamp") == last_ts:
                split_idx -= 1

            if split_idx == 0:
                # All events in combined belong to this same timestamp second
                self._held_events = combined
                return []

            ready_events = combined[:split_idx]
            self._held_events = combined[split_idx:]
        else:
            ready_events = combined

        return self._assign_group_ids(ready_events)

    def _assign_group_ids(self, events: list[dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
        group_counts: dict[tuple[str, Any, str], int] = defaultdict(int)
        result: list[tuple[dict[str, Any], str]] = []

        for ev in events:
            w = str(ev.get("proxy_wallet") or ev.get("wallet") or self.wallet or "").strip().lower()
            ts = ev.get("timestamp")
            base_key = compute_base_key(ev, wallet=w)
            group_key = (w, ts, base_key)
            occ = group_counts[group_key]
            group_counts[group_key] = occ + 1
            source_id = compute_source_event_id(base_key, occurrence=occ)
            result.append((ev, source_id))

        return result

    def get_id(self, event: dict[str, Any]) -> str:
        """Convenience method for a single event (occurrence 0)."""
        return compute_source_event_id(event, occurrence=0, wallet=self.wallet)


# Backwards compatibility alias
EventIdGenerator = EventIdAssigner
