"""create chain_events table for on-chain Polygon log verification

Revision ID: 0004_chain_events
Revises: 0003_accounting_ledger
Create Date: 2026-11-13 12:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0004_chain_events"
down_revision: Union[str, None] = "0003_accounting_ledger"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "chain_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("block_number", sa.BigInteger(), nullable=False),
        sa.Column("block_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("transaction_hash", sa.Text(), nullable=False),
        sa.Column("log_index", sa.Integer(), nullable=False),
        sa.Column("contract_address", sa.Text(), nullable=False),
        sa.Column("event_name", sa.Text(), nullable=False),
        sa.Column("condition_id", sa.Text(), nullable=True),
        sa.Column("token_id", sa.Text(), nullable=True),
        sa.Column("from_address", sa.Text(), nullable=True),
        sa.Column("to_address", sa.Text(), nullable=True),
        sa.Column("amount", sa.Numeric(), nullable=False),
        sa.Column("raw_log", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("transaction_hash", "log_index", name="uq_chain_events_tx_log"),
    )

    op.create_index(
        "ix_chain_events_wallet_block",
        "chain_events",
        ["wallet", "block_number"],
        unique=False,
    )
    op.create_index(
        "ix_chain_events_token_id",
        "chain_events",
        ["token_id"],
        unique=False,
    )
    op.create_index(
        "ix_chain_events_condition_id",
        "chain_events",
        ["condition_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_chain_events_condition_id", table_name="chain_events")
    op.drop_index("ix_chain_events_token_id", table_name="chain_events")
    op.drop_index("ix_chain_events_wallet_block", table_name="chain_events")
    op.drop_table("chain_events")
