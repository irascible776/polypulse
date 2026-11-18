"""
app/valuation/engine.py

Mark-to-Market valuation and PnL calculation engine.
Calculates unrealized PnL using the canonical formula:
    unrealized_pnl = (mark_price - avg_cost_basis) * open_shares
Supports real-time live queries and historical point-in-time (?as_of=) queries.
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.serving.time_format import utc_timestamp
from app.storage.models import Position, PositionLedger, WalletTotal
from app.valuation.pricing import PriceQuote, PriceResolver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PositionValuation:
    wallet: str
    token_id: str
    condition_id: str | None
    shares: Decimal
    avg_cost_basis: Decimal
    cost_basis_total: Decimal
    mark_price: Decimal | None
    market_value: Decimal | None
    unrealized_pnl: Decimal | None
    price_source: str
    staleness_seconds: float | None
    is_stale: bool
    observed_at: datetime | None = None
    estimated: bool = False
    market_closed_unresolved: bool = False
    unpriced_reason: str | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    outcome: str | None = None
    price_time_known: bool = True
    market_question: str | None = None
    category: str | None = None
    exposure_weight_pct: Decimal | None = None
    position_roi_pct: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "token_id": self.token_id,
            "condition_id": self.condition_id,
            "shares": str(self.shares),
            "avg_cost_basis": str(self.avg_cost_basis),
            "cost_basis_total": str(self.cost_basis_total),
            "mark_price": str(self.mark_price) if self.mark_price is not None else None,
            "market_value": (str(self.market_value) if self.market_value is not None else None),
            "unrealized_pnl": (
                str(self.unrealized_pnl) if self.unrealized_pnl is not None else None
            ),
            "price_source": self.price_source,
            "staleness_seconds": (
                round(self.staleness_seconds, 2) if self.staleness_seconds is not None else None
            ),
            "is_stale": self.is_stale,
            "price_time_known": self.price_time_known,
            "observed_at": utc_timestamp(self.observed_at),
            "estimated": self.estimated,
            "market_closed_unresolved": self.market_closed_unresolved,
            "unpriced_reason": self.unpriced_reason,
            "bid": str(self.bid) if self.bid is not None else None,
            "ask": str(self.ask) if self.ask is not None else None,
            "outcome": self.outcome,
            "market_question": self.market_question,
            "category": self.category,
            "exposure_weight_pct": (
                str(self.exposure_weight_pct) if self.exposure_weight_pct is not None else None
            ),
            "position_roi_pct": (
                str(self.position_roi_pct) if self.position_roi_pct is not None else None
            ),
        }


@dataclass(frozen=True)
class WalletPnLSummary:
    wallet: str
    as_of: datetime | None
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    total_cost_basis: Decimal
    total_market_value: Decimal
    open_positions_count: int
    unpriced_positions_count: int
    stale_positions_count: int
    positions: list[PositionValuation]

    @property
    def estimated_unrealized(self) -> Decimal:
        return sum(
            (
                p.unrealized_pnl
                for p in self.positions
                if p.estimated and p.unrealized_pnl is not None
            ),
            Decimal(0),
        )

    @property
    def unrealized_priced(self) -> Decimal:
        return sum(
            (
                p.unrealized_pnl
                for p in self.positions
                if not p.estimated and p.unrealized_pnl is not None
            ),
            Decimal(0),
        )

    @property
    def unpriced_cost_basis(self) -> Decimal:
        return sum((p.cost_basis_total for p in self.positions if p.mark_price is None), Decimal(0))

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "as_of": utc_timestamp(self.as_of),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_priced": str(self.unrealized_priced),
            "estimated_unrealized": str(self.estimated_unrealized),
            "unpriced_cost_basis": str(self.unpriced_cost_basis),
            "total_pnl_excl_unpriced": str(self.total_pnl),
            "complete": self.unpriced_positions_count == 0,
            "total_cost_basis": str(self.total_cost_basis),
            "total_market_value": str(self.total_market_value),
            "open_positions_count": self.open_positions_count,
            "unpriced_count": self.unpriced_positions_count,
            "unpriced_positions_count": self.unpriced_positions_count,
            "stale_positions_count": self.stale_positions_count,
            "estimated_count": sum(p.estimated for p in self.positions),
            "positions": [p.to_dict() for p in self.positions],
        }


class ValuationEngine:
    def __init__(
        self,
        price_resolver: PriceResolver | None = None,
        cache_ttl_seconds: float = 1.0,
    ) -> None:
        self.price_resolver = price_resolver or PriceResolver()
        self.cache_ttl_seconds = cache_ttl_seconds
        self._live_cache: dict[str, tuple[float, WalletPnLSummary]] = {}

    def invalidate(self, wallet: str | None = None) -> None:
        """Invalidates the micro-cache for a wallet or all wallets."""
        if wallet:
            self._live_cache.pop(wallet.lower(), None)
        else:
            self._live_cache.clear()

    async def value_wallet(
        self,
        session: AsyncSession,
        wallet: str,
        as_of: datetime | None = None,
    ) -> WalletPnLSummary:
        """
        Computes complete realized and mark-to-market unrealized PnL for a wallet.
        Supports time travel when as_of is provided.
        Uses high-speed event-invalidated micro-cache for sub-millisecond serving throughput.
        """
        clean_wallet = wallet.lower()

        if as_of is None and (
            os.environ.get("VALUATION_CACHE_DISABLED") == "1"
            or session.info.get("accounting_snapshot")
        ):
            # A process-wide cached summary may come from a newer/older transaction
            # than the freshness and revision read by this request.
            return await self._value_live_wallet(session, clean_wallet)
        if as_of is None:
            # Check micro-cache
            cached = self._live_cache.get(clean_wallet)
            now_mono = time.monotonic()
            if cached is not None and (now_mono - cached[0]) < self.cache_ttl_seconds:
                return cached[1]

            summary = await self._value_live_wallet(session, clean_wallet)
            self._live_cache[clean_wallet] = (now_mono, summary)
            return summary

        return await self._value_historical_wallet(session, clean_wallet, as_of)

    async def _value_live_wallet(
        self,
        session: AsyncSession,
        wallet: str,
    ) -> WalletPnLSummary:
        # 1. Fetch current realized PnL
        tot_stmt = select(WalletTotal).where(WalletTotal.wallet == wallet)
        wt = (await session.execute(tot_stmt)).scalar_one_or_none()
        realized_pnl = wt.total_realized_pnl if wt else Decimal("0")

        # 2. Fetch active positions (shares > 0)
        pos_stmt = (
            select(Position)
            .where(Position.wallet == wallet, Position.shares > Decimal("0"))
            .order_by(Position.shares.desc())
        )
        active_positions = (await session.execute(pos_stmt)).scalars().all()

        # Batch resolve mark prices in a single query
        token_ids = [pos.token_id for pos in active_positions]
        quotes_map = await self.price_resolver.get_mark_prices_batch(session, token_ids)

        valuations: list[PositionValuation] = []
        tot_unrealized = Decimal("0")
        tot_cost = Decimal("0")
        tot_mkt_val = Decimal("0")
        unpriced_count = 0
        stale_count = 0

        for pos in active_positions:
            quote: PriceQuote = quotes_map[pos.token_id]

            cost_total = pos.shares * pos.avg_cost_basis
            if quote.price is not None:
                mkt_val = pos.shares * quote.price
                unrealized = (quote.price - pos.avg_cost_basis) * pos.shares
                tot_unrealized += unrealized
                tot_mkt_val += mkt_val
            else:
                mkt_val = None
                unrealized = None
                unpriced_count += 1

            if quote.is_stale:
                stale_count += 1

            roi_pct = (
                ((unrealized / cost_total) * Decimal("100")).quantize(Decimal("0.01"))
                if unrealized is not None and cost_total > 0
                else None
            )
            pv = PositionValuation(
                wallet=wallet,
                token_id=pos.token_id,
                condition_id=pos.condition_id,
                shares=pos.shares,
                avg_cost_basis=pos.avg_cost_basis,
                cost_basis_total=cost_total,
                mark_price=quote.price,
                market_value=mkt_val,
                unrealized_pnl=unrealized,
                price_source=quote.source,
                staleness_seconds=quote.staleness_seconds,
                is_stale=quote.is_stale,
                observed_at=quote.observed_at,
                estimated=quote.estimated,
                market_closed_unresolved=quote.market_closed_unresolved,
                unpriced_reason=quote.reason,
                bid=quote.bid,
                ask=quote.ask,
                outcome=quote.outcome,
                price_time_known=quote.price_time_known,
                market_question=quote.market_question,
                category=quote.category,
                position_roi_pct=roi_pct,
            )
            valuations.append(pv)
            tot_cost += cost_total

        if tot_mkt_val > 0:
            valuations = [
                PositionValuation(
                    **{
                        **pv.__dict__,
                        "exposure_weight_pct": (
                            ((pv.market_value / tot_mkt_val) * Decimal("100")).quantize(
                                Decimal("0.01")
                            )
                            if pv.market_value is not None
                            else None
                        ),
                    }
                )
                for pv in valuations
            ]

        return WalletPnLSummary(
            wallet=wallet,
            as_of=None,
            realized_pnl=realized_pnl,
            unrealized_pnl=tot_unrealized,
            total_pnl=realized_pnl + tot_unrealized,
            total_cost_basis=tot_cost,
            total_market_value=tot_mkt_val,
            open_positions_count=len(active_positions),
            unpriced_positions_count=unpriced_count,
            stale_positions_count=stale_count,
            positions=valuations,
        )

    async def _value_historical_wallet(
        self,
        session: AsyncSession,
        wallet: str,
        as_of: datetime,
    ) -> WalletPnLSummary:
        """
        Reconstructs wallet positions and realized PnL as of historical timestamp.
        """
        # Fetch ledger entries up to as_of
        stmt = (
            select(PositionLedger)
            .where(
                PositionLedger.wallet == wallet,
                PositionLedger.event_time <= as_of,
            )
            .order_by(PositionLedger.event_time.asc(), PositionLedger.api_order.asc())
        )
        entries = (await session.execute(stmt)).scalars().all()

        realized_pnl = Decimal("0")
        positions_map: dict[str, dict[str, Any]] = {}

        for entry in entries:
            realized_pnl += entry.realized_pnl_delta
            if not entry.token_id:
                continue

            tok = entry.token_id
            if tok not in positions_map:
                positions_map[tok] = {
                    "shares": Decimal("0"),
                    "avg_cost": Decimal("0"),
                    "condition_id": entry.condition_id,
                }

            positions_map[tok]["shares"] = entry.shares_after
            positions_map[tok]["avg_cost"] = entry.avg_cost_after
            if entry.condition_id:
                positions_map[tok]["condition_id"] = entry.condition_id

        # Filter active positions at that point in time
        active_tokens = [
            tok for tok, state in positions_map.items() if state["shares"] > Decimal("0")
        ]
        quotes_map = await self.price_resolver.get_mark_prices_batch(
            session=session,
            token_ids=active_tokens,
            as_of=as_of,
        )

        valuations: list[PositionValuation] = []
        tot_unrealized = Decimal("0")
        tot_cost = Decimal("0")
        tot_mkt_val = Decimal("0")
        unpriced_count = 0
        stale_count = 0

        for tok in active_tokens:
            state = positions_map[tok]
            shares = state["shares"]
            avg_cost = state["avg_cost"]
            cond_id = state["condition_id"]
            quote = quotes_map[tok]

            cost_total = shares * avg_cost
            if quote.price is not None:
                mkt_val = shares * quote.price
                unrealized = (quote.price - avg_cost) * shares
                tot_unrealized += unrealized
                tot_mkt_val += mkt_val
            else:
                mkt_val = None
                unrealized = None
                unpriced_count += 1

            if quote.is_stale:
                stale_count += 1

            roi_pct = (
                ((unrealized / cost_total) * Decimal("100")).quantize(Decimal("0.01"))
                if unrealized is not None and cost_total > 0
                else None
            )
            pv = PositionValuation(
                wallet=wallet,
                token_id=tok,
                condition_id=cond_id,
                shares=shares,
                avg_cost_basis=avg_cost,
                cost_basis_total=cost_total,
                mark_price=quote.price,
                market_value=mkt_val,
                unrealized_pnl=unrealized,
                price_source=quote.source,
                staleness_seconds=quote.staleness_seconds,
                is_stale=quote.is_stale,
                observed_at=quote.observed_at,
                estimated=quote.estimated,
                market_closed_unresolved=quote.market_closed_unresolved,
                unpriced_reason=quote.reason,
                bid=quote.bid,
                ask=quote.ask,
                outcome=quote.outcome,
                price_time_known=quote.price_time_known,
                market_question=quote.market_question,
                category=quote.category,
                position_roi_pct=roi_pct,
            )
            valuations.append(pv)
            tot_cost += cost_total

        if tot_mkt_val > 0:
            valuations = [
                PositionValuation(
                    **{
                        **pv.__dict__,
                        "exposure_weight_pct": (
                            ((pv.market_value / tot_mkt_val) * Decimal("100")).quantize(
                                Decimal("0.01")
                            )
                            if pv.market_value is not None
                            else None
                        ),
                    }
                )
                for pv in valuations
            ]

        return WalletPnLSummary(
            wallet=wallet,
            as_of=as_of,
            realized_pnl=realized_pnl,
            unrealized_pnl=tot_unrealized,
            total_pnl=realized_pnl + tot_unrealized,
            total_cost_basis=tot_cost,
            total_market_value=tot_mkt_val,
            open_positions_count=len(valuations),
            unpriced_positions_count=unpriced_count,
            stale_positions_count=stale_count,
            positions=valuations,
        )
