"""Store hourly price summaries and preserve archived raw price observations."""

from alembic import op

revision = "0020_price_archive"
down_revision = "0019_shard_migration_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE market_price_archive (
            LIKE market_prices INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
            PRIMARY KEY (id, observed_at)
        ) PARTITION BY RANGE (observed_at)
    """)
    op.execute(
        "CREATE TABLE market_price_archive_default PARTITION OF market_price_archive DEFAULT"
    )
    op.execute(
        "CREATE INDEX ix_price_archive_token_time ON market_price_archive (token_id, observed_at)"
    )
    op.execute(
        "CREATE INDEX ix_price_archive_condition_time "
        "ON market_price_archive (condition_id, observed_at)"
    )
    op.execute("""
        CREATE TABLE market_price_hourly (
            token_id text NOT NULL,
            source text NOT NULL,
            hour_start timestamptz NOT NULL,
            condition_id text,
            observed_at timestamptz NOT NULL,
            tick_id bigint NOT NULL,
            price numeric(18, 6) NOT NULL,
            bid numeric(18, 6),
            ask numeric(18, 6),
            sample_count bigint NOT NULL,
            PRIMARY KEY (token_id, source, hour_start)
        )
    """)


def downgrade() -> None:
    op.execute("""
        INSERT INTO market_prices (id, condition_id, token_id, price, source, observed_at,
                                   created_at, bid, ask)
        SELECT id, condition_id, token_id, price, source, observed_at, created_at, bid, ask
        FROM market_price_archive
    """)
    op.execute("DROP TABLE market_price_hourly")
    op.execute("DROP TABLE market_price_archive")
