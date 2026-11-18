"""Store accounting checkpoints and the earliest invalidated event."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0016_accounting_checkpoints"
down_revision = "0015_partition_market_prices"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("wallet_totals", sa.Column("recompute_from_time", sa.DateTime(timezone=True)))
    op.add_column("wallet_totals", sa.Column("recompute_from_order", sa.BigInteger()))
    op.create_table(
        "accounting_checkpoints",
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("boundary_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("boundary_order", sa.BigInteger(), nullable=False),
        sa.Column("last_api_order", sa.BigInteger(), nullable=False),
        sa.Column("positions", JSONB(), nullable=False),
        sa.Column("totals", JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("wallet", "boundary_time", "boundary_order"),
    )


def downgrade() -> None:
    op.drop_table("accounting_checkpoints")
    op.drop_column("wallet_totals", "recompute_from_order")
    op.drop_column("wallet_totals", "recompute_from_time")
