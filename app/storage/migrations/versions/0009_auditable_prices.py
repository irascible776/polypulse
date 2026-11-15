"""Add auditable price inputs and constrain price sources."""

import sqlalchemy as sa
from alembic import op

revision = "0009_auditable_prices"
down_revision = "0008_wallet_event_order"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("market_prices", sa.Column("bid", sa.Numeric(18, 6), nullable=True))
    op.add_column("market_prices", sa.Column("ask", sa.Numeric(18, 6), nullable=True))
    op.create_check_constraint(
        "ck_market_prices_source",
        "market_prices",
        "source IN ('live_quote', 'last_trade', 'resolved')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_market_prices_source", "market_prices", type_="check")
    op.drop_column("market_prices", "ask")
    op.drop_column("market_prices", "bid")
