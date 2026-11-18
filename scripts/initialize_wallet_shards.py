"""Move half of wallet data into an empty second shard while writes are blocked."""

import asyncio
import json

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.storage.database import engine, secondary_engine
from app.storage.models import (
    AccountingCheckpoint,
    ChainEvent,
    ChainScanCursor,
    ConditionCheckpoint,
    ConditionReplayDirty,
    IngestionCursor,
    Position,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    ReconcileRun,
    SourceEvent,
    TrackedWallet,
    WalletTotal,
)
from scripts.scan_chain_slice import CHECKPOINT

TABLES = (
    TrackedWallet,
    SourceEvent,
    IngestionCursor,
    Position,
    WalletTotal,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    AccountingCheckpoint,
    ConditionCheckpoint,
    ConditionReplayDirty,
    ChainEvent,
    ReconcileRun,
    ChainScanCursor,
)


def belongs_on_secondary(model):
    column = model.address if model is TrackedWallet else model.wallet
    return func.get_byte(func.sha256(func.convert_to(func.lower(column), "UTF8")), 0) % 16 >= 8


async def rows(session, model, secondary_only):
    table = model.__table__
    stmt = select(table).order_by(*table.primary_key.columns)
    if secondary_only:
        stmt = stmt.where(belongs_on_secondary(model))
    result = await session.stream(stmt)
    async for batch in result.mappings().partitions(500):
        yield [dict(row) for row in batch]


async def verify_copies(primary, secondary):
    for model in TABLES:
        left = rows(primary, model, True)
        right = rows(secondary, model, False)
        while True:
            first = await anext(left, None)
            second = await anext(right, None)
            if first != second:
                raise RuntimeError(f"Shard copy differs for {model.__tablename__}")
            if first is None:
                break


async def copy_wallets(primary, secondary):
    for model in TABLES:
        async for batch in rows(primary, model, True):
            if model is SourceEvent:
                batch = [{**row, "superseded_by": None} for row in batch]
            await secondary.execute(model.__table__.insert(), batch)
    async for batch in rows(primary, SourceEvent, True):
        for row in batch:
            if row["superseded_by"] is not None:
                await secondary.execute(
                    update(SourceEvent)
                    .where(SourceEvent.id == row["id"])
                    .values(superseded_by=row["superseded_by"])
                )
    for model in (SourceEvent, PositionLedger, PositionLedgerReject, ChainEvent, ReconcileRun):
        name = model.__tablename__
        await secondary.execute(
            text(f"""
                SELECT setval(pg_get_serial_sequence('{name}', 'id'),
                    COALESCE((SELECT max(id) FROM {name}), 1),
                    EXISTS (SELECT 1 FROM {name}))
            """)
        )


async def verify_ownership(primary, secondary):
    for model in TABLES:
        wrong_primary = await primary.scalar(
            select(func.count()).select_from(model).where(belongs_on_secondary(model))
        )
        wrong_secondary = await secondary.scalar(
            select(func.count()).select_from(model).where(~belongs_on_secondary(model))
        )
        if wrong_primary or wrong_secondary:
            raise RuntimeError(f"Incorrect shard ownership in {model.__tablename__}")


async def import_scan_cursors(primary):
    name = engine.url.database
    if not name or not name.replace("_", "").isalnum():
        raise ValueError("Database name cannot be used for a scan checkpoint")
    path = CHECKPOINT.with_name(f"tail_checkpoint_{name}.json")
    saved = json.loads(path.read_text()) if path.exists() else {}
    base = json.loads(CHECKPOINT.read_text()) if CHECKPOINT.exists() else {}
    jn_path = CHECKPOINT.with_name("jn_tail_checkpoint.json")
    jn = json.loads(jn_path.read_text()) if jn_path.exists() else {}
    wallets = (await primary.execute(select(TrackedWallet.address))).scalars().all()
    for wallet in wallets:
        for direction in ("from", "to"):
            key = f"{wallet}:{direction}"
            block = saved.get(key, base.get(key))
            if block is None and wallet == "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92":
                block = jn.get(direction)
            if block is not None:
                await primary.execute(
                    insert(ChainScanCursor)
                    .values(wallet=wallet, direction=direction, last_block=block)
                    .on_conflict_do_nothing(index_elements=["wallet", "direction"])
                )


async def main():
    if secondary_engine is None or secondary_engine.url == engine.url:
        raise RuntimeError("Configure two distinct database URLs")
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    other_sessions = async_sessionmaker(
        secondary_engine, class_=AsyncSession, expire_on_commit=False
    )
    async with sessions() as primary, other_sessions() as secondary:
        await primary.begin()
        await secondary.begin()
        try:
            for session in (primary, secondary):
                await session.execute(text("SET LOCAL TIME ZONE 'UTC'"))
                await session.execute(
                    text("LOCK TABLE wallet_shard_settings IN ACCESS EXCLUSIVE MODE")
                )
                for model in TABLES:
                    await session.execute(
                        text(f"LOCK TABLE {model.__tablename__} IN ACCESS EXCLUSIVE MODE")
                    )
            primary_id, primary_state = (
                await primary.execute(
                    text("SELECT shard_id, migration_state FROM wallet_shard_settings")
                )
            ).one()
            secondary_id, secondary_state = (
                await secondary.execute(
                    text("SELECT shard_id, migration_state FROM wallet_shard_settings")
                )
            ).one()
            if (primary_id, secondary_id) == (0, 1):
                await verify_ownership(primary, secondary)
                await secondary.execute(
                    text("UPDATE wallet_shard_settings SET migration_state = 'active'")
                )
                await secondary.commit()
                await primary.execute(
                    text("UPDATE wallet_shard_settings SET migration_state = 'active'")
                )
                await primary.commit()
                print("Wallet shards are already initialized")
                return
            if primary_id == 0 and secondary_id is None and secondary_state == "copied":
                await verify_ownership(primary, secondary)
                await secondary.execute(
                    text(
                        "UPDATE wallet_shard_settings SET shard_id = 1, migration_state = 'active'"
                    )
                )
                await secondary.commit()
                await primary.rollback()
                print("Completed activation of the copied secondary shard")
                return
            if primary_id is not None or primary_state is not None or secondary_id is not None:
                raise RuntimeError("Unexpected shard identities")
            await import_scan_cursors(primary)
            if secondary_state is None:
                for model in TABLES:
                    if await secondary.scalar(select(func.count()).select_from(model)):
                        raise RuntimeError(f"Secondary {model.__tablename__} is not empty")
                await copy_wallets(primary, secondary)
            elif secondary_state == "copied":
                try:
                    await verify_copies(primary, secondary)
                except RuntimeError:
                    await secondary.execute(
                        text("UPDATE wallet_shard_settings SET migration_state = NULL")
                    )
                    for model in reversed(TABLES):
                        await secondary.execute(delete(model))
                    await copy_wallets(primary, secondary)
            else:
                raise RuntimeError("Unexpected secondary migration state")
            await verify_copies(primary, secondary)
            await secondary.execute(
                text("UPDATE wallet_shard_settings SET migration_state = 'copied'")
            )
            await secondary.commit()
            for model in reversed(TABLES):
                await primary.execute(delete(model).where(belongs_on_secondary(model)))
            await primary.execute(
                text("UPDATE wallet_shard_settings SET shard_id = 0, migration_state = 'active'")
            )
            await primary.commit()
            await secondary.execute(
                text("UPDATE wallet_shard_settings SET shard_id = 1, migration_state = 'active'")
            )
            await secondary.commit()
            print("Wallet shard migration completed and copies verified")
        except BaseException:
            await primary.rollback()
            await secondary.rollback()
            raise
    await engine.dispose()
    await secondary_engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
