"""Track the last fully committed timestamp group for backfill recovery."""

import sqlalchemy as sa
from alembic import op

revision = "0021_backfill_resume_time"
down_revision = "0020_price_archive"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ingestion_cursors", sa.Column("backfill_resume_time", sa.DateTime(timezone=True))
    )


def downgrade() -> None:
    op.drop_column("ingestion_cursors", "backfill_resume_time")
