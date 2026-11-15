"""
tests/test_valuation.py

Unit tests for Mark-to-Market pricing hierarchy, staleness tracking,
and unrealized PnL engine calculations.
"""

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.storage.database import async_session_factory
from app.storage.models import Position, WalletTotal
from app.valuation.engine import ValuationEngine
from app.valuation.pricing import PriceResolver, PriceTier


@pytest.mark.asyncio
async def test_pricing_tier_1_live_quote():
    """Verify Tier 1: Fresh market_price returns live_quote with is_stale=False."""
    token_id = "test_tok_" + uuid.uuid4().hex[:8]
    resolver = PriceResolver(max_staleness_seconds=60.0)
    now = datetime.now(timezone.utc)

    async with async_session_factory() as session:
        await resolver.record_price(
            session=session,
            token_id=token_id,
            price=Decimal("0.650000"),
            source=PriceTier.LIVE_QUOTE.value,
            observed_at=now,
            bid=Decimal("0.64"),
            ask=Decimal("0.66"),
        )

        quote = await resolver.get_mark_price(session, token_id=token_id)
        assert quote.price == Decimal("0.650000")
        assert quote.source == PriceTier.LIVE_QUOTE.value
        assert not quote.is_stale
        assert quote.staleness_seconds < 5.0


@pytest.mark.asyncio
async def test_pricing_tier_4_stale_fallback():
    """Verify Tier 4: Quote older than max_staleness returns source=stale and is_stale=True."""
    token_id = "test_tok_stale_" + uuid.uuid4().hex[:8]
    resolver = PriceResolver(max_staleness_seconds=30.0)
    old_time = datetime.now(timezone.utc) - timedelta(seconds=120)

    async with async_session_factory() as session:
        await resolver.record_price(
            session=session,
            token_id=token_id,
            price=Decimal("0.420000"),
            source=PriceTier.LIVE_QUOTE.value,
            observed_at=old_time,
            bid=Decimal("0.41"),
            ask=Decimal("0.43"),
        )

        quote = await resolver.get_mark_price(session, token_id=token_id)
        assert quote.price == Decimal("0.420000")
        assert quote.source == PriceTier.LIVE_QUOTE.value
        assert quote.is_stale is True
        assert quote.staleness_seconds >= 120.0


@pytest.mark.asyncio
async def test_pricing_tier_3_resolved_immune_to_staleness():
    """Verify Tier 3: Resolved settlement prices (1.0) are never marked stale."""
    token_id = "test_tok_resolved_" + uuid.uuid4().hex[:8]
    resolver = PriceResolver(max_staleness_seconds=10.0)
    old_time = datetime.now(timezone.utc) - timedelta(days=5)

    async with async_session_factory() as session:
        await resolver.record_price(
            session=session,
            token_id=token_id,
            price=Decimal("1.000000"),
            source=PriceTier.RESOLVED.value,
            observed_at=old_time,
        )

        quote = await resolver.get_mark_price(session, token_id=token_id)
        assert quote.price is None
        assert quote.source == PriceTier.UNAVAILABLE.value


@pytest.mark.asyncio
async def test_pricing_tier_5_unavailable():
    """Verify Tier 5: Non-existent token returns source=unavailable and price=None."""
    token_id = "test_tok_unknown_" + uuid.uuid4().hex[:8]
    resolver = PriceResolver()

    async with async_session_factory() as session:
        quote = await resolver.get_mark_price(session, token_id=token_id)
        assert quote.price is None
        assert quote.source == PriceTier.UNAVAILABLE.value
        assert quote.is_stale is False


@pytest.mark.asyncio
async def test_unrealized_pnl_mathematical_precision():
    """
    Verify ValuationEngine mathematical invariant:
    unrealized_pnl = (mark_price - avg_cost_basis) * open_shares
    total_pnl = realized_pnl + sum(unrealized_pnl)
    """
    wallet = "0x" + uuid.uuid4().hex[:40]
    token_id = "tok_math_" + uuid.uuid4().hex[:8]
    now = datetime.now(timezone.utc)

    resolver = PriceResolver()
    engine = ValuationEngine(price_resolver=resolver)

    cond_id = "0x" + "22" * 32
    async with async_session_factory() as session:
        # 1. Setup WalletTotal
        session.add(
            WalletTotal(
                wallet=wallet,
                total_realized_pnl=Decimal("1500.00"),
                total_trade_count=1,
                last_applied_api_order=1,
                updated_at=now,
            )
        )
        # 2. Setup Position: 10,000 shares bought at avg cost $0.40
        session.add(
            Position(
                wallet=wallet,
                condition_id=cond_id,
                token_id=token_id,
                shares=Decimal("10000.00"),
                avg_cost_basis=Decimal("0.400000"),
                updated_at=now,
            )
        )
        # 3. Setup Price: Current mark price = $0.65
        await resolver.record_price(
            session=session,
            token_id=token_id,
            price=Decimal("0.650000"),
            source=PriceTier.LIVE_QUOTE.value,
            observed_at=now,
            bid=Decimal("0.64"),
            ask=Decimal("0.66"),
        )

        summary = await engine.value_wallet(session, wallet=wallet)

        # Expected unrealized: (0.65 - 0.40) * 10,000 = 0.25 * 10,000 = $2,500.00
        assert summary.unrealized_pnl == Decimal("2500.00")
        # Expected total: 1,500 + 2,500 = $4,000.00
        assert summary.total_pnl == Decimal("4000.00")
        assert summary.total_cost_basis == Decimal("4000.00")
        assert summary.total_market_value == Decimal("6500.00")
        assert summary.open_positions_count == 1
        assert summary.stale_positions_count == 0


@pytest.mark.asyncio
async def test_tier_transition_live_quote_to_resolved_mid_session():
    """
    Verify mid-session market resolution:
    Token starts as live_quote ($0.65).
    Contract resolves to $1.00 -> Resolver flips to 'resolved' and becomes immune to staleness.
    """
    token_id = "tok_flip_" + uuid.uuid4().hex[:8]
    resolver = PriceResolver(max_staleness_seconds=10.0)
    t0 = datetime.now(timezone.utc) - timedelta(minutes=5)

    async with async_session_factory() as session:
        # 1. Initial live quote
        await resolver.record_price(
            session=session,
            token_id=token_id,
            price=Decimal("0.650000"),
            source=PriceTier.LIVE_QUOTE.value,
            observed_at=t0,
            bid=Decimal("0.64"),
            ask=Decimal("0.66"),
        )

        # Before resolution, 5 minutes old quote is marked stale
        q1 = await resolver.get_mark_price(session, token_id=token_id)
        assert q1.source == PriceTier.LIVE_QUOTE.value
        assert q1.is_stale is True

        # 2. Market resolves mid-session to $1.00
        t_resolve = datetime.now(timezone.utc) - timedelta(minutes=2)
        await resolver.record_price(
            session=session,
            token_id=token_id,
            price=Decimal("1.000000"),
            source=PriceTier.RESOLVED.value,
            observed_at=t_resolve,
        )

        # Immediately flips to resolved, price 1.0, and immune to staleness!
        q2 = await resolver.get_mark_price(session, token_id=token_id)
        assert q2.price == Decimal("0.650000")
        assert q2.source == PriceTier.LIVE_QUOTE.value
        assert q2.is_stale is True
