"""create market_prices table and add block_hash to chain_events

Revision ID: 0005_pricing_and_reorg
Revises: 0004_chain_events
Create Date: 2026-11-13 14:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005_pricing_and_reorg"
down_revision: Union[str, None] = "0004_chain_events"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create market_prices append-only tick table
    op.create_table(
        "market_prices",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=False), nullable=False),
        sa.Column("condition_id", sa.Text(), nullable=True),
        sa.Column("token_id", sa.Text(), nullable=False),
        sa.Column("price", sa.Numeric(18, 6), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_market_prices_token_observed",
        "market_prices",
        ["token_id", "observed_at"],
        unique=False,
    )
    op.create_index(
        "ix_market_prices_condition_observed",
        "market_prices",
        ["condition_id", "observed_at"],
        unique=False,
    )

    # 2. Add block_hash to chain_events for reorg safety & detection
    op.add_column("chain_events", sa.Column("block_hash", sa.Text(), nullable=True))
    op.create_index(
        "ix_chain_events_block_hash",
        "chain_events",
        ["block_hash"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_chain_events_block_hash", table_name="chain_events")
    op.drop_column("chain_events", "block_hash")
    op.drop_index("ix_market_prices_condition_observed", table_name="market_prices")
    op.drop_index("ix_market_prices_token_observed", table_name="market_prices")
    op.drop_table("market_prices")
