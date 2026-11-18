"""Add opt-in database enforcement for fixed wallet shard ownership."""

from alembic import op

revision = "0017_shard_ownership"
down_revision = "0016_accounting_checkpoints"
branch_labels = None
depends_on = None

WALLET_TABLES = (
    "source_events",
    "ingestion_cursors",
    "positions",
    "wallet_totals",
    "position_ledger",
    "position_ledger_rejects",
    "projector_cursors",
    "accounting_checkpoints",
    "chain_events",
    "reconcile_runs",
)


def upgrade() -> None:
    op.execute("""
        CREATE TABLE wallet_shard_settings (
            id integer PRIMARY KEY CHECK (id = 1),
            shard_id integer CHECK (shard_id IN (0, 1))
        )
    """)
    op.execute("INSERT INTO wallet_shard_settings (id, shard_id) VALUES (1, NULL)")
    op.execute("""
        CREATE FUNCTION enforce_wallet_shard() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE configured integer; address text; bucket integer;
        BEGIN
            SELECT shard_id INTO configured FROM wallet_shard_settings WHERE id = 1;
            IF configured IS NULL THEN RETURN NEW; END IF;
            address := to_jsonb(NEW) ->> TG_ARGV[0];
            bucket := get_byte(sha256(convert_to(lower(address), 'UTF8')), 0) % 16;
            IF (CASE WHEN bucket < 8 THEN 0 ELSE 1 END) <> configured THEN
                RAISE EXCEPTION 'wallet belongs to another shard' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END $$
    """)
    for table in WALLET_TABLES:
        op.execute(
            f"CREATE TRIGGER enforce_wallet_shard BEFORE INSERT OR UPDATE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION enforce_wallet_shard('wallet')"
        )
    op.execute("""
        CREATE TRIGGER enforce_wallet_shard BEFORE INSERT OR UPDATE ON tracked_wallets
        FOR EACH ROW EXECUTE FUNCTION enforce_wallet_shard('address')
    """)


def downgrade() -> None:
    for table in (*WALLET_TABLES, "tracked_wallets"):
        op.execute(f"DROP TRIGGER enforce_wallet_shard ON {table}")
    op.execute("DROP FUNCTION enforce_wallet_shard()")
    op.execute("DROP TABLE wallet_shard_settings")
