"""
app/valuation/pricing.py

Resolve auditable marks using agreed chain payouts or validated price ticks.
Staleness is calculated at read time and does not replace the source label.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import undefer

from app.config import get_settings
from app.serving.time_format import utc_timestamp
from app.storage.database import market_session
from app.storage.models import (
    ArchivedMarketPrice,
    MarketPrice,
    MarketResolution,
    Position,
    PositionLedger,
    SourceEvent,
)

logger = logging.getLogger(__name__)


class PriceTier(str, Enum):
    LIVE_QUOTE = "live_quote"
    LAST_TRADE = "last_trade"
    RESOLVED = "resolved"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class PriceQuote:
    token_id: str
    condition_id: str | None
    price: Decimal | None
    source: str
    observed_at: datetime | None
    staleness_seconds: float | None
    is_stale: bool
    estimated: bool = False
    market_closed_unresolved: bool = False
    bid: Decimal | None = None
    ask: Decimal | None = None
    reason: str | None = None
    outcome: str | None = None
    price_time_known: bool = True
    market_question: str | None = None
    category: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "condition_id": self.condition_id,
            "price": str(self.price) if self.price is not None else None,
            "source": self.source,
            "observed_at": utc_timestamp(self.observed_at),
            "staleness_seconds": (
                round(self.staleness_seconds, 2) if self.staleness_seconds is not None else None
            ),
            "is_stale": self.is_stale,
            "price_time_known": self.price_time_known,
            "estimated": self.estimated,
            "market_closed_unresolved": self.market_closed_unresolved,
            "bid": str(self.bid) if self.bid is not None else None,
            "ask": str(self.ask) if self.ask is not None else None,
            "unpriced": self.price is None,
            "reason": self.reason,
            "outcome": self.outcome,
            "market_question": self.market_question,
            "category": self.category,
        }


class PriceResolver:
    """
    Resolve marks from resolution evidence and validated, time-bounded ticks.
    """

    def __init__(self, max_staleness_seconds: float | None = None) -> None:
        settings = get_settings()
        self.max_staleness_seconds = (
            max_staleness_seconds
            if max_staleness_seconds is not None
            else settings.live_quote_staleness_seconds
        )
        self.last_trade_staleness = settings.last_trade_staleness_seconds
        self.max_spread = settings.max_spread

    async def get_mark_prices_batch(
        self,
        session: AsyncSession,
        token_ids: list[str],
        as_of: datetime | None = None,
        token_context: dict[str, tuple[str, int | None]] | None = None,
    ) -> dict[str, PriceQuote]:
        """
        Batch resolves mark prices for multiple tokens in O(1) database queries.
        """
        if not token_ids:
            return {}

        now = datetime.now(timezone.utc)
        ref_time = as_of or now
        results: dict[str, PriceQuote] = {}
        unique_tokens = list(set(token_ids))

        position_rows = (
            await session.execute(
                select(Position.token_id, Position.condition_id, Position.outcome_index).where(
                    Position.token_id.in_(unique_tokens)
                )
            )
        ).all()
        ledger_rows = (
            await session.execute(
                select(PositionLedger.token_id, PositionLedger.condition_id).where(
                    PositionLedger.token_id.in_(unique_tokens)
                )
            )
        ).all()
        event_rows = (
            await session.execute(
                select(SourceEvent.token_id, SourceEvent.raw)
                .where(SourceEvent.token_id.in_(unique_tokens), SourceEvent.event_time <= ref_time)
                .order_by(desc(SourceEvent.event_time), desc(SourceEvent.api_order))
            )
        ).all()
        event_labels: dict[str, str] = {}
        event_titles: dict[str, str] = {}
        for token, raw in event_rows:
            if not token or not isinstance(raw, dict):
                continue
            label = raw.get("outcome")
            if isinstance(label, str) and label:
                event_labels.setdefault(token, label)
            title = raw.get("title") or raw.get("question")
            if isinstance(title, str) and title:
                event_titles.setdefault(token, title)
        tokens = {token: (condition, index) for token, condition, index in position_rows}
        if token_context:
            tokens.update(token_context)
        for token, condition in ledger_rows:
            tokens.setdefault(token, (condition, None))
        async with market_session(session) as market_db:
            ticks = (
                (
                    await market_db.execute(
                        select(MarketPrice)
                        .options(undefer(MarketPrice.bid), undefer(MarketPrice.ask))
                        .where(
                            MarketPrice.token_id.in_(unique_tokens),
                            MarketPrice.observed_at <= ref_time,
                        )
                        .order_by(desc(MarketPrice.observed_at), desc(MarketPrice.id))
                    )
                )
                .scalars()
                .all()
            )
            archived = (
                (
                    await market_db.execute(
                        select(ArchivedMarketPrice).where(
                            ArchivedMarketPrice.token_id.in_(unique_tokens),
                            ArchivedMarketPrice.observed_at <= ref_time,
                        )
                    )
                )
                .scalars()
                .all()
            )
            ticks = sorted(
                [*ticks, *archived], key=lambda tick: (tick.observed_at, tick.id), reverse=True
            )
            for tick in ticks:
                if tick.condition_id:
                    tokens.setdefault(tick.token_id, (tick.condition_id, None))
            conditions = {c for c, _ in tokens.values() if c}
            resolutions = (
                (
                    await market_db.execute(
                        select(MarketResolution).where(
                            MarketResolution.condition_id.in_(conditions),
                            MarketResolution.checked_at <= ref_time,
                        )
                    )
                )
                .scalars()
                .all()
            )
        states = {r.condition_id: r for r in resolutions}
        grouped = {token: [] for token in unique_tokens}
        for tick in ticks:
            grouped[tick.token_id].append(tick)

        for token in unique_tokens:
            condition, outcome_index = tokens.get(token, (None, None))
            state = states.get(condition)
            label = event_labels.get(token)
            question = event_titles.get(token)
            category: str | None = None
            verified_outcome = False
            if state:
                gamma = state.gamma_raw if isinstance(state.gamma_raw, dict) else {}
                raw_question = gamma.get("question") or gamma.get("title")
                if isinstance(raw_question, str) and raw_question.strip():
                    question = raw_question.strip()
                raw_cat = gamma.get("category")
                if isinstance(raw_cat, str) and raw_cat.strip():
                    category = raw_cat.strip()
                try:
                    raw_ids = gamma.get("clobTokenIds") or "[]"
                    raw_labels = gamma.get("outcomes") or "[]"
                    ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
                    labels = json.loads(raw_labels) if isinstance(raw_labels, str) else raw_labels
                    if token in ids:
                        outcome_index = ids.index(token)
                        verified_outcome = True
                    if outcome_index is not None and outcome_index < len(labels):
                        label = labels[outcome_index]
                except (ValueError, TypeError):
                    pass
            if (
                state
                and state.resolved
                and state.agreement
                and state.payout_denominator > 0
                and verified_outcome
            ):
                if outcome_index is not None and 0 <= outcome_index < len(state.payout_numerators):
                    price = (
                        Decimal(state.payout_numerators[outcome_index]) / state.payout_denominator
                    )
                    results[token] = PriceQuote(
                        token,
                        condition,
                        price,
                        "resolved",
                        state.checked_at,
                        0.0,
                        False,
                        estimated=True,
                        outcome=label,
                        market_question=question,
                        category=category,
                    )
                    continue
            closed = bool(state and state.gamma_closed)
            closed_unresolved = bool(closed and state.payout_denominator == 0)
            valid = []
            if not (as_of is not None and state is None):
                for tick in grouped[token]:
                    if tick.source == "last_trade" and Decimal(0) <= tick.price <= Decimal(1):
                        valid.append(tick)
                    elif (
                        tick.source == "live_quote"
                        and not closed
                        and tick.bid is not None
                        and tick.ask is not None
                        and Decimal(0) <= tick.bid <= tick.ask <= Decimal(1)
                        and tick.ask - tick.bid <= self.max_spread
                    ):
                        valid.append(tick)
            fresh_live = [
                t
                for t in valid
                if t.source == "live_quote"
                and (ref_time - t.observed_at).total_seconds() <= self.max_staleness_seconds
            ]
            # The CLOB last-trade endpoint has no execution timestamp or trade ID;
            # observed_at records our poll, not the time of the trade.
            stale_live = [t for t in valid if t.source == "live_quote"]
            trades = [t for t in valid if t.source == "last_trade"]
            selected = valid if as_of is not None else fresh_live or stale_live or trades
            if selected:
                tick = selected[0]
                age = max(0.0, (ref_time - tick.observed_at).total_seconds())
                threshold = (
                    self.max_staleness_seconds
                    if tick.source == "live_quote"
                    else self.last_trade_staleness
                )
                price = (tick.bid + tick.ask) / 2 if tick.source == "live_quote" else tick.price
                results[token] = PriceQuote(
                    token,
                    condition,
                    price,
                    tick.source,
                    tick.observed_at,
                    None if tick.source == "last_trade" else age,
                    tick.source == "last_trade" or age > threshold,
                    market_closed_unresolved=closed_unresolved,
                    bid=tick.bid,
                    ask=tick.ask,
                    outcome=label,
                    price_time_known=tick.source != "last_trade",
                    market_question=question,
                    category=category,
                )
            else:
                reason = "no valid price tick"
                if as_of is not None and state is None:
                    reason = "no resolution observation at as_of"
                elif state and not state.agreement:
                    reason = "Gamma and chain resolution do not agree; no valid last trade"
                results[token] = PriceQuote(
                    token,
                    condition,
                    None,
                    "unavailable",
                    None,
                    0.0,
                    False,
                    market_closed_unresolved=closed_unresolved,
                    reason=reason,
                    outcome=label,
                    market_question=question,
                    category=category,
                )
        return results

    async def get_mark_price(
        self,
        session: AsyncSession,
        token_id: str,
        condition_id: str | None = None,
        as_of: datetime | None = None,
    ) -> PriceQuote:
        """Resolves the mark price for a single token."""
        context = {token_id: (condition_id, None)} if condition_id else None
        quotes = await self.get_mark_prices_batch(
            session, [token_id], as_of=as_of, token_context=context
        )
        return quotes[token_id]

    async def record_price(
        self,
        session: AsyncSession,
        token_id: str,
        price: Decimal,
        source: str = PriceTier.LIVE_QUOTE.value,
        condition_id: str | None = None,
        observed_at: datetime | None = None,
        bid: Decimal | None = None,
        ask: Decimal | None = None,
    ) -> MarketPrice:
        """Append a new price tick to the market_prices table."""
        now = datetime.now(timezone.utc)
        tick = MarketPrice(
            token_id=token_id,
            condition_id=condition_id,
            price=price,
            source=source,
            bid=bid,
            ask=ask,
            observed_at=observed_at or now,
            created_at=now,
        )
        session.add(tick)
        await session.commit()
        return tick
