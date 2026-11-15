import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from app.storage.database import async_session_factory
from app.storage.models import MarketPrice, MarketResolution, Position
from app.valuation.pricing import PriceResolver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,expected,source,stale,estimated",
    [
        ("winner", "1", "resolved", False, True),
        ("loser", "0", "resolved", False, True),
        ("half", "0.5", "resolved", False, True),
        ("closed_unresolved", "0.4", "last_trade", True, False),
        ("one_sided", "0.4", "last_trade", True, False),
        ("wide", "0.4", "last_trade", True, False),
        ("fresh", "0.5", "live_quote", False, False),
        ("live_precedence", "0.5", "live_quote", False, False),
        ("stale_trade", "0.4", "last_trade", True, False),
        ("stale_live_over_newer_trade", "0.5", "live_quote", True, False),
        ("missing_bid_ask", None, "unavailable", False, False),
        ("nothing", None, "unavailable", False, False),
        ("disagreement", None, "unavailable", False, False),
    ],
)
async def test_pricing_policy_cases(case, expected, source, stale, estimated):
    now = datetime.now(timezone.utc)
    token, condition, wallet = (uuid.uuid4().hex for _ in range(3))
    async with async_session_factory() as session:
        session.add(
            Position(
                wallet=wallet,
                token_id=token,
                condition_id=condition,
                outcome_index=0,
                shares=Decimal(1),
                avg_cost_basis=Decimal("0.2"),
                updated_at=now,
            )
        )
        if case in {"winner", "loser", "half", "closed_unresolved", "disagreement"}:
            nums = {"winner": [1, 0], "loser": [0, 1], "half": [1, 1]}.get(case, [0, 0])
            den = 2 if case == "half" else 1 if case in {"winner", "loser", "disagreement"} else 0
            session.add(
                MarketResolution(
                    condition_id=condition,
                    resolved=bool(den),
                    payout_numerators=nums,
                    payout_denominator=den,
                    gamma_closed=True,
                    gamma_uma_status="resolved" if den else "proposed",
                    gamma_raw={"clobTokenIds": [token, "other"], "outcomes": ["Yes", "No"]},
                    chain_block=100,
                    checked_at=now,
                    agreement=case != "disagreement",
                )
            )
        if case in {
            "closed_unresolved",
            "one_sided",
            "wide",
            "stale_trade",
            "live_precedence",
            "stale_live_over_newer_trade",
        }:
            age = timedelta(hours=7) if case == "stale_trade" else timedelta(seconds=5)
            session.add(
                MarketPrice(
                    token_id=token,
                    condition_id=condition,
                    price=Decimal("0.4"),
                    source="last_trade",
                    observed_at=now - age,
                    created_at=now,
                )
            )
        if case in {
            "closed_unresolved",
            "one_sided",
            "wide",
            "fresh",
            "missing_bid_ask",
            "live_precedence",
            "stale_live_over_newer_trade",
        }:
            bid = None if case in {"one_sided", "missing_bid_ask"} else Decimal("0.45")
            ask = (
                None
                if case == "missing_bid_ask"
                else Decimal("0.8")
                if case == "wide"
                else Decimal("0.55")
            )
            session.add(
                MarketPrice(
                    token_id=token,
                    condition_id=condition,
                    price=Decimal("0.5"),
                    bid=bid,
                    ask=ask,
                    source="live_quote",
                    observed_at=now - timedelta(seconds=120)
                    if case == "stale_live_over_newer_trade"
                    else now,
                    created_at=now,
                )
            )
        await session.flush()
        quote = await PriceResolver().get_mark_price(session, token)
        assert quote.price == (Decimal(expected) if expected is not None else None)
        assert (quote.source, quote.is_stale, quote.estimated) == (source, stale, estimated)
        if source == "last_trade":
            assert quote.price_time_known is False
            assert quote.staleness_seconds is None
        if case == "closed_unresolved":
            assert quote.market_closed_unresolved is True
        await session.rollback()


@pytest.mark.asyncio
async def test_historical_uses_latest_valid_tick_and_requires_resolution_observation():
    token, condition, wallet = (uuid.uuid4().hex for _ in range(3))
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        session.add(
            Position(
                wallet=wallet,
                token_id=token,
                condition_id=condition,
                shares=Decimal(1),
                avg_cost_basis=Decimal("0.2"),
                updated_at=now,
            )
        )
        session.add(
            MarketResolution(
                condition_id=condition,
                resolved=False,
                payout_numerators=[0, 0],
                payout_denominator=0,
                gamma_closed=False,
                gamma_raw={},
                chain_block=1,
                checked_at=now - timedelta(seconds=10),
                agreement=True,
            )
        )
        session.add_all(
            [
                MarketPrice(
                    token_id=token,
                    condition_id=condition,
                    source="last_trade",
                    price=Decimal("0.3"),
                    observed_at=now - timedelta(seconds=30),
                    created_at=now,
                ),
                MarketPrice(
                    token_id=token,
                    condition_id=condition,
                    source="live_quote",
                    price=Decimal("0.5"),
                    bid=Decimal("0.49"),
                    ask=Decimal("0.51"),
                    observed_at=now - timedelta(seconds=8),
                    created_at=now,
                ),
                MarketPrice(
                    token_id=token,
                    condition_id=condition,
                    source="last_trade",
                    price=Decimal("0.4"),
                    observed_at=now - timedelta(seconds=4),
                    created_at=now,
                ),
            ]
        )
        await session.flush()
        resolver = PriceResolver()
        assert (await resolver.get_mark_price(session, token, as_of=now)).price == Decimal("0.4")
        assert (
            await resolver.get_mark_price(session, token, as_of=now - timedelta(seconds=20))
        ).price is None
        await session.rollback()
