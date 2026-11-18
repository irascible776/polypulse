"""Take coordinated PostgreSQL backups while shard writes are blocked."""

import argparse
import asyncio
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from app.storage.database import engine, secondary_engine
from scripts.initialize_wallet_shards import TABLES

MARKET_TABLES = (
    "market_prices",
    "market_price_archive",
    "market_price_hourly",
    "market_resolutions",
)


async def dump_database(db_engine, snapshot, path, executable):
    url = db_engine.url.set(drivername="postgresql", password=None)
    environment = os.environ.copy()
    if db_engine.url.password:
        environment["PGPASSWORD"] = db_engine.url.password
    process = await asyncio.create_subprocess_exec(
        executable,
        "--format=custom",
        "--no-owner",
        "--no-acl",
        "--lock-wait-timeout=10s",
        f"--snapshot={snapshot}",
        "--file",
        str(path),
        url.render_as_string(hide_password=False),
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, errors = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.terminate()
            await process.wait()
        raise
    if process.returncode:
        raise RuntimeError(f"pg_dump failed: {errors.decode(errors='replace')}")


async def main(output: Path, executable: str):
    if secondary_engine is None or secondary_engine.url == engine.url:
        raise RuntimeError("Two distinct shard databases are required")
    output.mkdir(parents=True, exist_ok=False)
    started = datetime.now(timezone.utc)
    files = [output / "primary.dump", output / "secondary.dump"]
    async with engine.connect() as primary, secondary_engine.connect() as secondary:
        transactions = [await primary.begin(), await secondary.begin()]
        try:
            for index, connection in enumerate((primary, secondary)):
                await connection.execute(text("SET LOCAL lock_timeout = '10s'"))
                await connection.execute(text("LOCK TABLE wallet_shard_settings IN SHARE MODE"))
                for model in TABLES:
                    await connection.execute(
                        text(f"LOCK TABLE {model.__tablename__} IN SHARE MODE")
                    )
                if index == 0:
                    for table in MARKET_TABLES:
                        await connection.execute(text(f"LOCK TABLE {table} IN SHARE MODE"))
            for index, connection in enumerate((primary, secondary)):
                row = (
                    await connection.execute(
                        text(
                            "SELECT shard_id, migration_state FROM wallet_shard_settings WHERE id=1"
                        )
                    )
                ).one()
                if row != (index, "active"):
                    raise RuntimeError(f"Shard {index} is not active")
            snapshots = [
                await connection.scalar(text("SELECT pg_export_snapshot()"))
                for connection in (primary, secondary)
            ]
            results = await asyncio.gather(
                dump_database(engine, snapshots[0], files[0], executable),
                dump_database(secondary_engine, snapshots[1], files[1], executable),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            for transaction in reversed(transactions):
                await transaction.rollback()
    manifest = {
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "files": {},
    }
    for path in files:
        digest = hashlib.sha256()
        with path.open("rb") as data:
            while block := data.read(1024 * 1024):
                digest.update(block)
        manifest["files"][path.name] = {"bytes": path.stat().st_size, "sha256": digest.hexdigest()}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))
    await engine.dispose()
    await secondary_engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pg-dump", default=shutil.which("pg_dump"))
    args = parser.parse_args()
    if not args.pg_dump:
        parser.error("pg_dump was not found; specify --pg-dump")
    asyncio.run(main(args.output, args.pg_dump))
