"""Add service_heartbeats table for /fleet and observed_at index for rolling throughput."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0025_service_heartbeats"
down_revision = "0024_condition_replay"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "service_heartbeats",
        sa.Column("instance_id", sa.Text(), primary_key=True),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("shard_id", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("ws_connections", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("listener_ok", sa.Boolean(), nullable=True),
        sa.Column(
            "buckets_held",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
        sa.Column("latency_p50_ms", sa.Numeric(), nullable=True),
        sa.Column("latency_p95_ms", sa.Numeric(), nullable=True),
        sa.Column("latency_p99_ms", sa.Numeric(), nullable=True),
        sa.Column(
            "meta",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index("ix_service_heartbeats_last_seen", "service_heartbeats", ["last_seen_at"])
    op.create_index("ix_source_events_observed_at", "source_events", ["observed_at"])


def downgrade() -> None:
    op.drop_index("ix_source_events_observed_at", table_name="source_events")
    op.drop_index("ix_service_heartbeats_last_seen", table_name="service_heartbeats")
    op.drop_table("service_heartbeats")
