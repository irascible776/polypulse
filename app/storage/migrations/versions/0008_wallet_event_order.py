"""Enforce wallet-wide event identity and ordering."""

from alembic import op

revision = "0008_wallet_event_order"
down_revision = "0007_chain_event_identity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_source_events_wallet_order", "source_events", ["wallet", "api_order"]
    )
    op.create_unique_constraint(
        "uq_source_events_wallet_event_id", "source_events", ["wallet", "source_event_id"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_source_events_wallet_event_id", "source_events", type_="unique")
    op.drop_constraint("uq_source_events_wallet_order", "source_events", type_="unique")
