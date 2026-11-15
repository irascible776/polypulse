"""Store the latest chain and Gamma resolution check for each condition."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0010_market_resolutions"
down_revision = "0009_auditable_prices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_resolutions",
        sa.Column("condition_id", sa.Text(), primary_key=True),
        sa.Column("resolved", sa.Boolean(), nullable=False),
        sa.Column("payout_numerators", JSONB(), nullable=False),
        sa.Column("payout_denominator", sa.Numeric(), nullable=False),
        sa.Column("gamma_closed", sa.Boolean(), nullable=True),
        sa.Column("gamma_uma_status", sa.Text(), nullable=True),
        sa.Column("gamma_raw", JSONB(), nullable=False),
        sa.Column("chain_block", sa.BigInteger(), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("agreement", sa.Boolean(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("market_resolutions")
