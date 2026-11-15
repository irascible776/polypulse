import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.storage.models import TrackedWallet
from app.storage.validation import normalize_and_validate_address

logger = logging.getLogger("wallet_seeding")


def load_wallets_from_yaml(path: str | Path = "wallets.yaml") -> list[dict[str, Any]]:
    """
    Read and validate wallet declarations from a YAML configuration file.
    Validates addresses against Ethereum/Polygon 0x hexadecimal format.
    Never hardcode wallet lists in source code.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Wallets configuration file not found at: {p.resolve()}")

    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or "wallets" not in data:
        raise ValueError("Invalid wallets.yaml format: top-level 'wallets' list required")

    wallets_raw = data.get("wallets") or []
    validated_wallets: list[dict[str, Any]] = []

    for entry in wallets_raw:
        raw_addr = entry.get("address")
        if not raw_addr:
            continue

        normalized_addr = normalize_and_validate_address(str(raw_addr))
        label = str(entry.get("label") or "").strip() or None

        raw_start = entry.get("tracking_start")
        if raw_start:
            if isinstance(raw_start, datetime):
                tracking_start = (
                    raw_start.replace(tzinfo=timezone.utc)
                    if raw_start.tzinfo is None
                    else raw_start
                )
            else:
                tracking_start = datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
        else:
            tracking_start = datetime.now(timezone.utc)

        validated_wallets.append(
            {
                "address": normalized_addr,
                "label": label,
                "tracking_start": tracking_start,
                "status": "active",
            }
        )

    return validated_wallets


async def seed_tracked_wallets(
    session: AsyncSession,
    path: str | Path = "wallets.yaml",
) -> int:
    """
    Upsert validated wallets from YAML into the tracked_wallets database table.
    Returns the count of upserted wallets.
    """
    wallets = load_wallets_from_yaml(path)
    if not wallets:
        return 0

    for w in wallets:
        stmt = (
            insert(TrackedWallet)
            .values(
                address=w["address"],
                label=w["label"],
                tracking_start=w["tracking_start"],
                status=w["status"],
            )
            .on_conflict_do_update(
                index_elements=["address"],
                set_={
                    "label": w["label"],
                    "status": w["status"],
                },
            )
        )
        await session.execute(stmt)

    await session.commit()
    logger.info("Successfully seeded %d wallets from %s", len(wallets), path)
    return len(wallets)
