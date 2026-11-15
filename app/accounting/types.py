from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class AccountingEvent:
    source_event_id: str
    api_order: int
    event_type: str
    event_time: datetime
    wallet: str
    condition_id: str | None
    token_id: str | None
    outcome_index: int | None
    side: str | None
    size: Decimal
    usdc_size: Decimal
    price_reported: Decimal
    cash_price_gap: bool
    raw: dict[str, Any]


@dataclass(frozen=True)
class PositionState:
    wallet: str
    condition_id: str
    token_id: str
    outcome_index: int | None
    shares: Decimal
    avg_cost_basis: Decimal
    realized_pnl: Decimal
    last_applied_api_order: int
    updated_at: datetime


@dataclass(frozen=True)
class ApplySuccess:
    wallet: str
    condition_id: str
    token_id: str
    shares_before: Decimal
    shares_delta: Decimal
    shares_after: Decimal
    avg_cost_before: Decimal
    avg_cost_after: Decimal
    cash_delta: Decimal
    realized_pnl_delta: Decimal
    cash_price_gap: bool
    is_mutating: bool


@dataclass(frozen=True)
class ApplyReject:
    wallet: str
    condition_id: str | None
    token_id: str | None
    size: Decimal | None
    usdc_size: Decimal | None
    reject_reason: str


@dataclass(frozen=True)
class ApplyResult:
    success: ApplySuccess | None = None
    reject: ApplyReject | None = None
    is_unsupported: bool = False
