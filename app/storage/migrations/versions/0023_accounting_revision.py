"""Track committed wallet accounting publications independently of event order."""

import sqlalchemy as sa
from alembic import op

revision = "0023_accounting_revision"
down_revision = "0022_shard_guard_integrity"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "wallet_totals",
        sa.Column("publication_revision", sa.BigInteger(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("wallet_totals", "publication_revision")
