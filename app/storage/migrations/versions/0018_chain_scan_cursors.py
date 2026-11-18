"""Persist chain scan progress with the wallet data it describes."""

from alembic import op

revision = "0018_chain_scan_cursors"
down_revision = "0017_shard_ownership"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE chain_scan_cursors (
            wallet text NOT NULL,
            direction text NOT NULL CHECK (direction IN ('from', 'to')),
            last_block bigint NOT NULL,
            PRIMARY KEY (wallet, direction)
        )
    """)
    op.execute("""
        CREATE TRIGGER enforce_wallet_shard BEFORE INSERT OR UPDATE ON chain_scan_cursors
        FOR EACH ROW EXECUTE FUNCTION enforce_wallet_shard('wallet')
    """)


def downgrade() -> None:
    op.execute("DROP TABLE chain_scan_cursors")
