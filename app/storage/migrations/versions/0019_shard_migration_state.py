"""Seal a copied shard until the primary commits the wallet move."""

from alembic import op

revision = "0019_shard_migration_state"
down_revision = "0018_chain_scan_cursors"
branch_labels = None
depends_on = None

TABLES = (
    "tracked_wallets",
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
    "chain_scan_cursors",
)


def upgrade() -> None:
    op.execute("ALTER TABLE wallet_shard_settings ADD COLUMN migration_state text")
    op.execute("""
        CREATE OR REPLACE FUNCTION enforce_wallet_shard() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE configured integer; state text; address text; bucket integer; item jsonb;
        BEGIN
            SELECT shard_id, migration_state INTO configured, state
                FROM wallet_shard_settings WHERE id = 1;
            IF state = 'copied' THEN
                RAISE EXCEPTION 'wallet shard migration is not active' USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN item := to_jsonb(OLD); ELSE item := to_jsonb(NEW); END IF;
            IF configured IS NOT NULL THEN
                address := item ->> TG_ARGV[0];
                bucket := get_byte(sha256(convert_to(lower(address), 'UTF8')), 0) % 16;
                IF (CASE WHEN bucket < 8 THEN 0 ELSE 1 END) <> configured THEN
                    RAISE EXCEPTION 'wallet belongs to another shard' USING ERRCODE = '23514';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
        END $$
    """)
    for table in TABLES:
        column = "address" if table == "tracked_wallets" else "wallet"
        op.execute(f"DROP TRIGGER enforce_wallet_shard ON {table}")
        op.execute(
            f"CREATE TRIGGER enforce_wallet_shard BEFORE INSERT OR UPDATE OR DELETE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION enforce_wallet_shard('{column}')"
        )


def downgrade() -> None:
    for table in TABLES:
        column = "address" if table == "tracked_wallets" else "wallet"
        op.execute(f"DROP TRIGGER enforce_wallet_shard ON {table}")
        op.execute(
            f"CREATE TRIGGER enforce_wallet_shard BEFORE INSERT OR UPDATE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION enforce_wallet_shard('{column}')"
        )
    op.execute("""
        CREATE OR REPLACE FUNCTION enforce_wallet_shard() RETURNS trigger LANGUAGE plpgsql AS $$
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
    op.execute("ALTER TABLE wallet_shard_settings DROP COLUMN migration_state")
