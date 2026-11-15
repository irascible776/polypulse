"""Identify chain events by wallet and batch element."""

import sqlalchemy as sa
from alembic import op

revision = "0007_chain_event_identity"
down_revision = "0006_accounting_integrity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_chain_events_tx_log", "chain_events", type_="unique")
    op.add_column(
        "chain_events", sa.Column("batch_index", sa.Integer(), server_default="0", nullable=False)
    )
    op.create_unique_constraint(
        "uq_chain_events_wallet_tx_log_batch",
        "chain_events",
        ["wallet", "transaction_hash", "log_index", "batch_index"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_chain_events_wallet_tx_log_batch", "chain_events", type_="unique")
    op.drop_column("chain_events", "batch_index")
    op.create_unique_constraint(
        "uq_chain_events_tx_log", "chain_events", ["transaction_hash", "log_index"]
    )
