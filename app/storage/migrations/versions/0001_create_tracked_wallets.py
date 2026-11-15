"""create tracked_wallets table

Revision ID: 0001_create_tracked_wallets
Revises:
Create Date: 2026-11-12 01:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_create_tracked_wallets"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tracked_wallets",
        sa.Column("address", sa.String(length=42), nullable=False),
        sa.Column("label", sa.String(length=255), nullable=True),
        sa.Column(
            "tracking_start",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=32), server_default="active", nullable=False),
        sa.PrimaryKeyConstraint("address"),
    )
    op.create_index(op.f("ix_tracked_wallets_address"), "tracked_wallets", ["address"], unique=True)


def downgrade() -> None:
    op.drop_index(op.f("ix_tracked_wallets_address"), table_name="tracked_wallets")
    op.drop_table("tracked_wallets")
