"""Maintain UTC price partitions without discarding late observations."""

from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def ensure_price_partitions(session: AsyncSession, days_ahead: int = 7) -> None:
    today = datetime.now(timezone.utc).date()
    days = [today + timedelta(days=offset) for offset in range(days_ahead + 1)]
    names = [f"market_prices_{day:%Y%m%d}" for day in days]
    existing = set(
        (
            await session.execute(
                text("""
                    SELECT child.relname FROM pg_inherits
                    JOIN pg_class parent ON parent.oid = inhparent
                    JOIN pg_class child ON child.oid = inhrelid
                    WHERE parent.oid = 'market_prices'::regclass
                """)
            )
        ).scalars()
    )
    if all(name in existing for name in names):
        return

    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext('market_prices_partitions'))")
    )
    await session.execute(text("LOCK TABLE market_prices IN ACCESS EXCLUSIVE MODE"))
    await session.execute(
        text("CREATE TEMP TABLE price_partition_move (LIKE market_prices) ON COMMIT DROP")
    )
    for day, name in zip(days, names):
        attached = await session.scalar(
            text("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_inherits
                    WHERE inhparent = 'market_prices'::regclass AND inhrelid = to_regclass(:name)
                )
            """),
            {"name": name},
        )
        if attached:
            continue
        start = datetime.combine(day, datetime.min.time(), timezone.utc)
        end = start + timedelta(days=1)
        await session.execute(text("TRUNCATE price_partition_move"))
        await session.execute(
            text("""
                WITH moved AS (
                    DELETE FROM market_prices_default
                    WHERE observed_at >= :start AND observed_at < :end RETURNING *
                ) INSERT INTO price_partition_move SELECT * FROM moved
            """),
            {"start": start, "end": end},
        )
        await session.execute(
            text(
                f"CREATE TABLE {name} PARTITION OF market_prices "
                f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
            )
        )
        await session.execute(text("INSERT INTO market_prices SELECT * FROM price_partition_move"))
