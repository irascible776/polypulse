"""Roll up and archive old ticks while preserving exact historical prices."""

import re
from datetime import datetime, time, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def _roll_up(session: AsyncSession, table: str) -> None:
    hour = "date_trunc('hour', observed_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'"
    await session.execute(
        text(f"""
        WITH ranked AS (
            SELECT *, {hour} AS hour_start,
                row_number() OVER (PARTITION BY token_id, source, {hour}
                                   ORDER BY observed_at DESC, id DESC) AS rank,
                count(*) OVER (PARTITION BY token_id, source, {hour}) AS samples
            FROM {table}
        )
        INSERT INTO market_price_hourly
            (token_id, source, hour_start, condition_id, observed_at, tick_id,
             price, bid, ask, sample_count)
        SELECT token_id, source, hour_start, condition_id, observed_at, id,
               price, bid, ask, samples FROM ranked WHERE rank = 1
        ON CONFLICT (token_id, source, hour_start) DO UPDATE SET
            sample_count = market_price_hourly.sample_count + EXCLUDED.sample_count,
            condition_id = CASE WHEN (EXCLUDED.observed_at, EXCLUDED.tick_id) >
                (market_price_hourly.observed_at, market_price_hourly.tick_id)
                THEN EXCLUDED.condition_id ELSE market_price_hourly.condition_id END,
            price = CASE WHEN (EXCLUDED.observed_at, EXCLUDED.tick_id) >
                (market_price_hourly.observed_at, market_price_hourly.tick_id)
                THEN EXCLUDED.price ELSE market_price_hourly.price END,
            bid = CASE WHEN (EXCLUDED.observed_at, EXCLUDED.tick_id) >
                (market_price_hourly.observed_at, market_price_hourly.tick_id)
                THEN EXCLUDED.bid ELSE market_price_hourly.bid END,
            ask = CASE WHEN (EXCLUDED.observed_at, EXCLUDED.tick_id) >
                (market_price_hourly.observed_at, market_price_hourly.tick_id)
                THEN EXCLUDED.ask ELSE market_price_hourly.ask END,
            observed_at = greatest(EXCLUDED.observed_at, market_price_hourly.observed_at),
            tick_id = CASE WHEN (EXCLUDED.observed_at, EXCLUDED.tick_id) >
                (market_price_hourly.observed_at, market_price_hourly.tick_id)
                THEN EXCLUDED.tick_id ELSE market_price_hourly.tick_id END
    """)
    )


async def archive_old_prices(session: AsyncSession, retention_days: int) -> None:
    claimed = await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(hashtext('market_prices_archive'))")
    )
    if not claimed:
        return
    cutoff = datetime.combine(
        datetime.now(timezone.utc).date() - timedelta(days=retention_days), time.min, timezone.utc
    )
    names = (
        (
            await session.execute(
                text("""
            SELECT child.relname FROM pg_inherits
            JOIN pg_class child ON child.oid = inhrelid
            WHERE inhparent = 'market_prices'::regclass
        """)
            )
        )
        .scalars()
        .all()
    )
    old = sorted(
        name
        for name in names
        if re.fullmatch(r"market_prices_\d{8}", name)
        and datetime.strptime(name[-8:], "%Y%m%d").replace(tzinfo=timezone.utc) < cutoff
    )
    has_default = await session.scalar(
        text("SELECT EXISTS (SELECT 1 FROM market_prices_default WHERE observed_at < :cutoff)"),
        {"cutoff": cutoff},
    )
    if not old and not has_default:
        return
    await session.execute(text("LOCK TABLE market_prices IN ACCESS EXCLUSIVE MODE"))
    await session.execute(text("LOCK TABLE market_price_archive IN ACCESS EXCLUSIVE MODE"))
    if old:
        name = old[0]
        start = datetime.strptime(name[-8:], "%Y%m%d").replace(tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        await _roll_up(session, name)
        await session.execute(text(f"ALTER TABLE market_prices DETACH PARTITION {name}"))
        await session.execute(
            text(f"""
                WITH moved AS (
                    DELETE FROM market_price_archive_default
                    WHERE observed_at >= :start AND observed_at < :end RETURNING *
                ) INSERT INTO {name} SELECT * FROM moved
            """),
            {"start": start, "end": end},
        )
        await session.execute(
            text(
                f"ALTER TABLE market_price_archive ATTACH PARTITION {name} "
                f"FOR VALUES FROM ('{start.isoformat()}') TO ('{end.isoformat()}')"
            )
        )

    await session.execute(
        text(
            "CREATE TEMP TABLE IF NOT EXISTS price_archive_batch "
            "(LIKE market_prices) ON COMMIT DROP"
        )
    )
    await session.execute(text("TRUNCATE price_archive_batch"))
    await session.execute(
        text("""
            INSERT INTO price_archive_batch SELECT * FROM market_prices_default
            WHERE observed_at < :cutoff ORDER BY observed_at, id LIMIT 5000
        """),
        {"cutoff": cutoff},
    )
    await _roll_up(session, "price_archive_batch")
    await session.execute(
        text("INSERT INTO market_price_archive SELECT * FROM price_archive_batch")
    )
    await session.execute(
        text("""
        DELETE FROM market_prices_default AS original USING price_archive_batch AS batch
        WHERE original.id = batch.id AND original.observed_at = batch.observed_at
    """)
    )
