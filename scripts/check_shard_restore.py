"""Check backup file integrity and invariants on a separately restored shard pair."""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

from sqlalchemy import select, text

from app.storage.database import (
    async_session_factory,
    engine,
    secondary_engine,
    secondary_session_factory,
)
from app.storage.models import WalletTotal
from scripts.initialize_wallet_shards import verify_ownership
from scripts.verify_rebuild import verify_wallet_rebuild


def check_manifest(directory: Path) -> None:
    manifest = json.loads((directory / "manifest.json").read_text())
    files = manifest["files"]
    if set(files) != {"primary.dump", "secondary.dump"}:
        raise ValueError("Manifest must describe exactly the two expected shard dumps")
    for name, metadata in files.items():
        path = directory / name
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as data:
            while block := data.read(1024 * 1024):
                digest.update(block)
                size += len(block)
        if size != metadata["bytes"] or digest.hexdigest() != metadata["sha256"]:
            raise ValueError(f"Backup integrity mismatch: {name}")
        print(f"Backup integrity verified: {name} ({size} bytes)")


async def check_restored_shards(directory: Path) -> None:
    check_manifest(directory)
    if secondary_engine is None or secondary_session_factory is None:
        raise RuntimeError("Configure both restored shard database URLs")
    if engine.url == secondary_engine.url:
        raise RuntimeError("The two restored shard URLs must be distinct")
    try:
        async with async_session_factory() as primary, secondary_session_factory() as secondary:
            for shard, session in enumerate((primary, secondary)):
                row = (
                    await session.execute(
                        text(
                            "SELECT shard_id, migration_state FROM wallet_shard_settings WHERE id=1"
                        )
                    )
                ).one()
                if row != (shard, "active"):
                    raise RuntimeError(f"Restored shard {shard} is not active")
            await verify_ownership(primary, secondary)
            wallets = []
            for session in (primary, secondary):
                wallets.extend((await session.scalars(select(WalletTotal.wallet))).all())
        for wallet in wallets:
            if not await verify_wallet_rebuild(wallet):
                raise RuntimeError(f"Wallet invariant check failed: {wallet}")
        print(f"Shard ownership and invariants checked for {len(wallets)} wallets")
        print("Database contents were not compared byte-for-byte with the dumps.")
    finally:
        await engine.dispose()
        await secondary_engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", type=Path, required=True, help="Backup manifest directory")
    args = parser.parse_args()
    asyncio.run(check_restored_shards(args.backup))
