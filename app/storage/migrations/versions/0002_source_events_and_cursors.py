"""create source_events and ingestion_cursors tables

Revision ID: 0002_source_events_and_cursors
Revises: 0001_create_tracked_wallets
Create Date: 2026-11-12 02:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002_source_events_and_cursors"
down_revision: Union[str, None] = "0001_create_tracked_wallets"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. source_events table
    op.create_table(
        "source_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("source_event_id", sa.Text(), nullable=False),
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("transaction_hash", sa.Text(), nullable=False),
        sa.Column("condition_id", sa.Text(), nullable=True),
        sa.Column("token_id", sa.Text(), nullable=True),
        sa.Column("side", sa.Text(), nullable=True),
        sa.Column("outcome_index", sa.Integer(), nullable=True),
        sa.Column("size", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("usdc_size", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("price_reported", sa.Numeric(), server_default="0", nullable=False),
        sa.Column("effective_price", sa.Numeric(), nullable=True),
        sa.Column("cash_price_gap", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("api_order", sa.BigInteger(), nullable=False),
        sa.Column("is_late", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("raw", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "source_event_id", name="uq_source_events_source_event_id"),
    )
    op.create_index(
        "ix_source_events_wallet_time_order",
        "source_events",
        ["wallet", "event_time", "api_order"],
    )

    # 2. ingestion_cursors table
    op.create_table(
        "ingestion_cursors",
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("backfill_state", sa.Text(), server_default="not_started", nullable=False),
        sa.Column("last_event_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_cursor", sa.Text(), nullable=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("consecutive_failures", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_api_order", sa.BigInteger(), server_default="0", nullable=False),
        sa.PrimaryKeyConstraint("wallet", "source"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_cursors")
    op.drop_index("ix_source_events_wallet_time_order", table_name="source_events")
    op.drop_table("source_events")
