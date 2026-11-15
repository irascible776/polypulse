"""Canonical serialization of timezone-aware API timestamps."""

from datetime import datetime, timezone


def utc_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.utcoffset() is None:
        raise ValueError("API timestamps must include a timezone")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
