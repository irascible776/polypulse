"""Record the results of each wallet's chain reconciliation."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0011_reconcile_runs"
down_revision = "0010_market_resolutions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "reconcile_runs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("wallet", sa.Text(), nullable=False),
        sa.Column("reference_block", sa.BigInteger(), nullable=True),
        sa.Column("reference_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("match_count", sa.Integer(), nullable=True),
        sa.Column("mismatch_count", sa.Integer(), nullable=True),
        sa.Column("unknown_count", sa.Integer(), nullable=True),
        sa.Column("details", JSONB(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_reconcile_runs_wallet_finished", "reconcile_runs", ["wallet", "finished_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_reconcile_runs_wallet_finished", table_name="reconcile_runs")
    op.drop_table("reconcile_runs")
