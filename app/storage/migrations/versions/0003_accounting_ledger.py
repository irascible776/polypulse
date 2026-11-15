"""create positions, wallet_totals, position_ledger, rejects, and projector_cursors

Revision ID: 0003_accounting_ledger
Revises: 0002_source_events_and_cursors
Create Date: 2026-11-13 01:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_accounting_ledger"
down_revision: Union[str, None] = "0002_source_events_and_cursors"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. positions table
    op.create_table(
        "positions",
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("condition_id", sa.Text(), nullable=False),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.Column("outcome_index", sa.Integer(), nullable=True),
        sa.Column("shares", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("avg_cost_basis", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("realized_pnl", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("last_applied_api_order", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("wallet", "condition_id", "token_id"),
        sa.CheckConstraint("shares >= 0", name="chk_positions_shares_non_negative"),
    )

    # 2. wallet_totals table
    op.create_table(
        "wallet_totals",
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("total_realized_pnl", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("total_trade_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("total_redeem_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("unsupported_event_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_applied_api_order", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("last_recompute_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("recompute_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "recompute_needed", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("wallet"),
    )

    # 3. position_ledger table (audit log)
    op.create_table(
        "position_ledger",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("condition_id", sa.Text(), nullable=False),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.Column("source_event_id", sa.Text(), nullable=False),
        sa.Column("api_order", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("shares_before", sa.Numeric(), nullable=False),
        sa.Column("shares_delta", sa.Numeric(), nullable=False),
        sa.Column("shares_after", sa.Numeric(), nullable=False),
        sa.Column("avg_cost_before", sa.Numeric(), nullable=False),
        sa.Column("avg_cost_after", sa.Numeric(), nullable=False),
        sa.Column("cash_delta", sa.Numeric(), nullable=False),
        sa.Column("realized_pnl_delta", sa.Numeric(), nullable=False),
        sa.Column("is_recompute", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("cash_price_gap", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_position_ledger_wallet_order",
        "position_ledger",
        ["wallet", "api_order"],
    )
    op.create_index(
        "ix_position_ledger_wallet_token_order",
        "position_ledger",
        ["wallet", "token_id", "api_order"],
    )

    # 4. position_ledger_rejects table
    op.create_table(
        "position_ledger_rejects",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("source_event_id", sa.Text(), nullable=False),
        sa.Column("api_order", sa.BigInteger(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("condition_id", sa.Text(), nullable=True),
        sa.Column("token_id", sa.Text(), nullable=True),
        sa.Column("size", sa.Numeric(), nullable=True),
        sa.Column("usdc_size", sa.Numeric(), nullable=True),
        sa.Column("reject_reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    # 5. projector_cursors table
    op.create_table(
        "projector_cursors",
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("last_applied_api_order", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("wallet"),
    )


def downgrade() -> None:
    op.drop_table("projector_cursors")
    op.drop_table("position_ledger_rejects")
    op.drop_index("ix_position_ledger_wallet_token_order", table_name="position_ledger")
    op.drop_index("ix_position_ledger_wallet_order", table_name="position_ledger")
    op.drop_table("position_ledger")
    op.drop_table("wallet_totals")
    op.drop_table("positions")
