"""Versioned condition snapshots and transactional correction boundaries."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0024_condition_replay"
down_revision = "0023_accounting_revision"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "condition_checkpoints",
        sa.Column("wallet", sa.Text(), primary_key=True),
        sa.Column("condition_id", sa.Text(), primary_key=True),
        sa.Column("boundary_time", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("boundary_order", sa.BigInteger(), primary_key=True),
        sa.Column("format_version", sa.Integer(), nullable=False),
        sa.Column("positions", postgresql.JSONB(), nullable=False),
    )
    op.create_table(
        "condition_replay_dirty",
        sa.Column("wallet", sa.Text(), primary_key=True),
        sa.Column("condition_id", sa.Text(), primary_key=True),
        sa.Column("boundary_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("boundary_order", sa.BigInteger(), nullable=False),
    )
    for table in ("condition_checkpoints", "condition_replay_dirty"):
        op.execute(
            f"CREATE TRIGGER guard_{table} BEFORE INSERT OR UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION enforce_wallet_shard('wallet')"
        )
    op.create_index(
        "ix_source_events_wallet_condition_time",
        "source_events",
        ["wallet", "condition_id", "event_time", "api_order"],
    )


def downgrade() -> None:
    op.drop_index("ix_source_events_wallet_condition_time", table_name="source_events")
    op.drop_table("condition_replay_dirty")
    op.drop_table("condition_checkpoints")
