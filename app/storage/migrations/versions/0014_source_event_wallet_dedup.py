"""Index wallet and natural key for finding enrichment candidates."""

from alembic import op

revision = "0014_source_event_wallet_dedup"
down_revision = "0013_source_event_dedup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_source_events_wallet_dedup", "source_events", ["wallet", "dedup_key"])


def downgrade() -> None:
    op.drop_index("ix_source_events_wallet_dedup", table_name="source_events")
