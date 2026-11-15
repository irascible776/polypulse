from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Identity,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TrackedWallet(Base):
    """
    Represents a wallet registered for tracking by PolyPulse.

    Records the normalized address, an optional human-readable label,
    the tracking start timestamp, and current monitoring status.
    """

    __tablename__ = "tracked_wallets"

    address: Mapped[str] = mapped_column(String(42), primary_key=True, index=True)
    label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tracking_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")

    def __repr__(self) -> str:
        return f"<TrackedWallet address={self.address} label={self.label} status={self.status}>"


class SourceEvent(Base):
    """
    Append-only raw store of all events received from any source.
    Never update or delete rows.
    """

    __tablename__ = "source_events"
    __table_args__ = (
        UniqueConstraint("source", "source_event_id", name="uq_source_events_source_event_id"),
        UniqueConstraint("wallet", "api_order", name="uq_source_events_wallet_order"),
        UniqueConstraint("wallet", "source_event_id", name="uq_source_events_wallet_event_id"),
        Index("ix_source_events_wallet_time_order", "wallet", "event_time", "api_order"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=False), primary_key=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_event_id: Mapped[str] = mapped_column(Text, nullable=False)
    dedup_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    superseded_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    wallet: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    transaction_hash: Mapped[str] = mapped_column(Text, nullable=False)
    condition_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    side: Mapped[str | None] = mapped_column(Text, nullable=True)
    outcome_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    size: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=Decimal("0"))
    usdc_size: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=Decimal("0"))
    price_reported: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=Decimal("0"))
    effective_price: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    cash_price_gap: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    api_order: Mapped[int] = mapped_column(BigInteger, nullable=False)
    is_late: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    raw: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<SourceEvent id={self.id} source={self.source} wallet={self.wallet} "
            f"type={self.event_type} api_order={self.api_order}>"
        )


class IngestionCursor(Base):
    """
    Maintains ingestion cursor, state, and metrics per wallet and source.
    Also holds last_api_order for atomic per-wallet write locking.
    """

    __tablename__ = "ingestion_cursors"

    wallet: Mapped[str] = mapped_column(Text, primary_key=True)
    source: Mapped[str] = mapped_column(Text, primary_key=True)
    backfill_state: Mapped[str] = mapped_column(Text, nullable=False, default="not_started")
    last_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_api_order: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    def __repr__(self) -> str:
        return (
            f"<IngestionCursor wallet={self.wallet} source={self.source} "
            f"state={self.backfill_state} last_api_order={self.last_api_order}>"
        )


class Position(Base):
    """
    Current position state for a specific outcome token held by a wallet.
    Mutated only by the accounting projector inside transactions.
    """

    __tablename__ = "positions"
    __table_args__ = (CheckConstraint("shares >= 0", name="chk_positions_shares_non_negative"),)

    wallet: Mapped[str] = mapped_column(Text, primary_key=True)
    condition_id: Mapped[str] = mapped_column(Text, primary_key=True)
    token_id: Mapped[str] = mapped_column(Text, primary_key=True)
    outcome_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    shares: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=Decimal("0"))
    avg_cost_basis: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=Decimal("0"))
    realized_pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=Decimal("0"))
    last_applied_api_order: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<Position wallet={self.wallet} token={self.token_id[:10]}... "
            f"shares={self.shares} avg_cost={self.avg_cost_basis} pnl={self.realized_pnl}>"
        )


class WalletTotal(Base):
    """
    Cumulative metrics and concurrency locking row for each tracked wallet.
    Acquires SELECT ... FOR UPDATE here to serialize projector runs per wallet.
    """

    __tablename__ = "wallet_totals"

    wallet: Mapped[str] = mapped_column(Text, primary_key=True)
    total_realized_pnl: Mapped[Decimal] = mapped_column(
        Numeric, nullable=False, default=Decimal("0")
    )
    total_trade_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_redeem_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unsupported_event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reject_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_applied_api_order: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_recompute_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    recompute_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    recompute_needed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<WalletTotal wallet={self.wallet} pnl={self.total_realized_pnl} "
            f"trades={self.total_trade_count} redeems={self.total_redeem_count}>"
        )


class PositionLedger(Base):
    """
    Append-only audit trail recording every state transition applied to positions.
    Never update or delete rows during incremental catch-up.
    """

    __tablename__ = "position_ledger"
    __table_args__ = (
        UniqueConstraint("wallet", "source_event_id", name="uq_position_ledger_wallet_event"),
        Index("ix_position_ledger_wallet_order", "wallet", "api_order"),
        Index("ix_position_ledger_wallet_token_order", "wallet", "token_id", "api_order"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=False), primary_key=True)
    wallet: Mapped[str] = mapped_column(Text, nullable=False)
    condition_id: Mapped[str] = mapped_column(Text, nullable=False)
    token_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_event_id: Mapped[str] = mapped_column(Text, nullable=False)
    api_order: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    shares_before: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    shares_delta: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    shares_after: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    avg_cost_before: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    avg_cost_after: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    cash_delta: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    realized_pnl_delta: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    is_recompute: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cash_price_gap: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<PositionLedger id={self.id} wallet={self.wallet} type={self.event_type} "
            f"shares_delta={self.shares_delta} pnl_delta={self.realized_pnl_delta}>"
        )


class PositionLedgerReject(Base):
    """
    Records events that failed accounting invariants (e.g. Selling more than held,
    or unresolvable token IDs) without halting replay.
    """

    __tablename__ = "position_ledger_rejects"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=False), primary_key=True)
    wallet: Mapped[str] = mapped_column(Text, nullable=False)
    source_event_id: Mapped[str] = mapped_column(Text, nullable=False)
    api_order: Mapped[int] = mapped_column(BigInteger, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    condition_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    size: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    usdc_size: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    reject_reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<PositionLedgerReject id={self.id} wallet={self.wallet} "
            f"reason={self.reject_reason} event_id={self.source_event_id}>"
        )


class ProjectorCursor(Base):
    """
    Maintains the projector's high-water mark cursor per wallet.
    Decoupled from ingestion cursors.
    """

    __tablename__ = "projector_cursors"

    wallet: Mapped[str] = mapped_column(Text, primary_key=True)
    last_applied_api_order: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<ProjectorCursor wallet={self.wallet} "
            f"last_applied_api_order={self.last_applied_api_order}>"
        )


class ChainEvent(Base):
    """
    Append-only raw decoded event logs fetched from the Polygon blockchain.
    Used for independent on-chain ground truth verification.
    """

    __tablename__ = "chain_events"
    __table_args__ = (
        UniqueConstraint(
            "wallet",
            "transaction_hash",
            "log_index",
            "batch_index",
            name="uq_chain_events_wallet_tx_log_batch",
        ),
        Index("ix_chain_events_wallet_block", "wallet", "block_number"),
        Index("ix_chain_events_token_id", "token_id"),
        Index("ix_chain_events_condition_id", "condition_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    wallet: Mapped[str] = mapped_column(Text, nullable=False)
    block_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    block_hash: Mapped[str | None] = mapped_column(Text, nullable=True, index=True)
    block_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    transaction_hash: Mapped[str] = mapped_column(Text, nullable=False)
    log_index: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    contract_address: Mapped[str] = mapped_column(Text, nullable=False)
    event_name: Mapped[str] = mapped_column(Text, nullable=False)
    condition_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    from_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    amount: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    raw_log: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<ChainEvent id={self.id} event={self.event_name} wallet={self.wallet} "
            f"block={self.block_number} tx={self.transaction_hash[:10]}>"
        )


class ReconcileRun(Base):
    __tablename__ = "reconcile_runs"
    __table_args__ = (Index("ix_reconcile_runs_wallet_finished", "wallet", "finished_at"),)

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    wallet: Mapped[str] = mapped_column(Text, nullable=False)
    reference_block: Mapped[int | None] = mapped_column(BigInteger)
    reference_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    match_count: Mapped[int | None] = mapped_column(Integer)
    mismatch_count: Mapped[int | None] = mapped_column(Integer)
    in_flight_count: Mapped[int | None] = mapped_column(Integer)
    unknown_count: Mapped[int | None] = mapped_column(Integer)
    details: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    error: Mapped[str | None] = mapped_column(Text)


class MarketResolution(Base):
    """Latest checked Gamma status and on-chain payout vector."""

    __tablename__ = "market_resolutions"

    condition_id: Mapped[str] = mapped_column(Text, primary_key=True)
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False)
    payout_numerators: Mapped[list[int]] = mapped_column(JSONB, nullable=False)
    payout_denominator: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    gamma_closed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    gamma_uma_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    gamma_raw: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    chain_block: Mapped[int] = mapped_column(BigInteger, nullable=False)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    agreement: Mapped[bool] = mapped_column(Boolean, nullable=False)


class MarketPrice(Base):
    """
    Append-only price tick log for Polymarket tokens.
    Supports point-in-time valuation (?as_of= queries) and staleness tracking.
    """

    __tablename__ = "market_prices"
    __table_args__ = (
        CheckConstraint(
            "source IN ('live_quote', 'last_trade', 'resolved')", name="ck_market_prices_source"
        ),
        Index("ix_market_prices_token_observed", "token_id", "observed_at"),
        Index("ix_market_prices_condition_observed", "condition_id", "observed_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    condition_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    bid: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True, deferred=True)
    ask: Mapped[Decimal | None] = mapped_column(Numeric(18, 6), nullable=True, deferred=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return (
            f"<MarketPrice id={self.id} token={self.token_id[:10]} "
            f"price={self.price} source={self.source} observed={self.observed_at}>"
        )
