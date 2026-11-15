"""Count explained post-ingestion chain changes separately from mismatches."""

import sqlalchemy as sa
from alembic import op

revision = "0012_reconcile_in_flight"
down_revision = "0011_reconcile_runs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("reconcile_runs", sa.Column("in_flight_count", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("reconcile_runs", "in_flight_count")
