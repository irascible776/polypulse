"""Enforce ledger event uniqueness and persist wallet reject counts."""

import sqlalchemy as sa
from alembic import op

revision = "0006_accounting_integrity"
down_revision = "0005_pricing_and_reorg"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_position_ledger_wallet_event", "position_ledger", ["wallet", "source_event_id"]
    )
    op.add_column(
        "wallet_totals", sa.Column("reject_count", sa.Integer(), server_default="0", nullable=False)
    )
    op.execute(
        "UPDATE wallet_totals SET reject_count = "
        "(SELECT count(*) FROM position_ledger_rejects r WHERE r.wallet = wallet_totals.wallet)"
    )


def downgrade() -> None:
    op.drop_column("wallet_totals", "reject_count")
    op.drop_constraint("uq_position_ledger_wallet_event", "position_ledger", type_="unique")
