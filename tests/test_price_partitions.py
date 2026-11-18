from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select, text

from app.storage.database import async_session_factory
from app.storage.models import MarketPrice, MarketResolution
from app.storage.price_archive import archive_old_prices
from app.storage.price_partitions import ensure_price_partitions
from app.valuation.pricing import PriceResolver


@pytest.mark.asyncio
async def test_archive_failure_rolls_back_tick_move_and_rollup():
    token = uuid4().hex
    observed = datetime(2002, 1, 1, tzinfo=timezone.utc)
    async with async_session_factory() as session:
        session.add(
            MarketPrice(
                token_id=token,
                price=Decimal("0.5"),
                source="last_trade",
                observed_at=observed,
                created_at=observed,
            )
        )
        await session.commit()
        try:
            with pytest.raises(RuntimeError, match="interrupt"):
                async with session.begin():
                    await archive_old_prices(session, retention_days=7)
                    raise RuntimeError("interrupt")
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM market_prices WHERE token_id = :token"),
                    {"token": token},
                )
                == 1
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM market_price_archive WHERE token_id = :token"),
                    {"token": token},
                )
                == 0
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM market_price_hourly WHERE token_id = :token"),
                    {"token": token},
                )
                == 0
            )
        finally:
            await session.execute(
                text("DELETE FROM market_prices WHERE token_id = :token"), {"token": token}
            )
            await session.commit()


@pytest.mark.asyncio
async def test_archive_preserves_exact_history_and_builds_hourly_rollup():
    token, condition = uuid4().hex, uuid4().hex
    observed = datetime(2001, 1, 1, 12, 0, tzinfo=timezone.utc)
    async with async_session_factory() as session:
        await session.execute(
            text("""
            CREATE TABLE market_prices_20010101 PARTITION OF market_prices
            FOR VALUES FROM ('2001-01-01 00:00:00+00') TO ('2001-01-02 00:00:00+00')
        """)
        )
        session.add(
            MarketResolution(
                condition_id=condition,
                resolved=False,
                payout_numerators=[],
                payout_denominator=Decimal(0),
                gamma_closed=False,
                gamma_raw={},
                chain_block=1,
                checked_at=observed - timedelta(days=1),
                agreement=True,
            )
        )
        for minute, price in ((0, "0.4"), (30, "0.8")):
            instant = observed + timedelta(minutes=minute)
            session.add(
                MarketPrice(
                    token_id=token,
                    condition_id=condition,
                    price=Decimal(price),
                    source="last_trade",
                    observed_at=instant,
                    created_at=instant,
                )
            )
        await session.flush()
        resolver = PriceResolver()
        times = [observed + timedelta(minutes=10), observed + timedelta(minutes=40)]
        before = [
            await resolver.get_mark_price(session, token, condition, as_of=at) for at in times
        ]
        await archive_old_prices(session, retention_days=7)
        after = [await resolver.get_mark_price(session, token, condition, as_of=at) for at in times]
        assert [quote.price for quote in before] == [Decimal("0.4"), Decimal("0.8")]
        assert [quote.price for quote in after] == [quote.price for quote in before]
        rollup = (
            await session.execute(
                text("SELECT price, sample_count FROM market_price_hourly WHERE token_id = :token"),
                {"token": token},
            )
        ).one()
        assert rollup == (Decimal("0.8"), 2)
        assert (
            await session.scalar(
                text("SELECT count(*) FROM market_price_archive WHERE token_id = :token"),
                {"token": token},
            )
            == 2
        )
        late_at = observed + timedelta(minutes=20)
        session.add(
            MarketPrice(
                token_id=token,
                condition_id=condition,
                price=Decimal("0.6"),
                source="last_trade",
                observed_at=late_at,
                created_at=late_at,
            )
        )
        await session.flush()
        await archive_old_prices(session, retention_days=7)
        await archive_old_prices(session, retention_days=7)
        assert (
            await resolver.get_mark_price(
                session, token, condition, as_of=observed + timedelta(minutes=25)
            )
        ).price == Decimal("0.6")
        rollup = (
            await session.execute(
                text("SELECT price, sample_count FROM market_price_hourly WHERE token_id = :token"),
                {"token": token},
            )
        ).one()
        assert rollup == (Decimal("0.8"), 3)
        await session.rollback()


@pytest.mark.asyncio
async def test_default_partition_rows_move_without_changing_prices():
    token = uuid4().hex
    day = datetime.now(timezone.utc).date() + timedelta(days=8)
    observed = datetime.combine(day, datetime.min.time(), timezone.utc)
    async with async_session_factory() as session:
        session.add(
            MarketPrice(
                token_id=token,
                price=Decimal("0.5"),
                source="last_trade",
                observed_at=observed,
                created_at=observed,
            )
        )
        await session.flush()
        before = await session.scalar(select(MarketPrice.id).where(MarketPrice.token_id == token))
        assert (
            await session.scalar(
                text("SELECT tableoid::regclass::text FROM market_prices WHERE token_id = :token"),
                {"token": token},
            )
            == "market_prices_default"
        )
        await ensure_price_partitions(session, days_ahead=8)
        row = (
            await session.execute(
                text(
                    "SELECT id, price, tableoid::regclass::text FROM market_prices "
                    "WHERE token_id = :token"
                ),
                {"token": token},
            )
        ).one()
        assert row == (before, Decimal("0.5"), f"market_prices_{day:%Y%m%d}")
        await session.rollback()
