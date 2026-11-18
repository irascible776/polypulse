"""Protect shard identity and wallet ownership keys from accidental changes."""

from alembic import op

revision = "0022_shard_guard_integrity"
down_revision = "0021_backfill_resume_time"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE wallet_shard_settings ADD CONSTRAINT ck_shard_migration_state
        CHECK (migration_state IS NULL OR migration_state IN ('copied', 'active'))
    """)
    op.execute("""
        CREATE OR REPLACE FUNCTION enforce_wallet_shard() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE configured integer; state text; address text; bucket integer; item jsonb;
        BEGIN
            SELECT shard_id, migration_state INTO configured, state
                FROM wallet_shard_settings WHERE id = 1;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'wallet shard settings missing' USING ERRCODE = '55000';
            END IF;
            IF state = 'copied' THEN
                RAISE EXCEPTION 'wallet shard migration is not active' USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'UPDATE' AND (to_jsonb(OLD) ->> TG_ARGV[0]) IS DISTINCT FROM
                                    (to_jsonb(NEW) ->> TG_ARGV[0]) THEN
                RAISE EXCEPTION 'wallet ownership cannot be changed' USING ERRCODE = '23514';
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
    op.execute("""
        CREATE FUNCTION prevent_shard_settings_delete() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'wallet shard settings cannot be deleted' USING ERRCODE = '55000';
        END $$
    """)
    op.execute("""
        CREATE TRIGGER prevent_shard_settings_delete BEFORE DELETE ON wallet_shard_settings
        FOR EACH ROW EXECUTE FUNCTION prevent_shard_settings_delete()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER prevent_shard_settings_delete ON wallet_shard_settings")
    op.execute("DROP FUNCTION prevent_shard_settings_delete()")
    op.execute("ALTER TABLE wallet_shard_settings DROP CONSTRAINT ck_shard_migration_state")
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
