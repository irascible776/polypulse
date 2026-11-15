"""Add a stable natural key and retain superseded source observations."""

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

import sqlalchemy as sa
from alembic import op


def _normalize(value):
    if value is None or value == "":
        return "0"
    try:
        number = Decimal(str(value))
    except Exception:
        return "0"
    if number.is_zero():
        return "0"
    result = format(number.normalize(), "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


def dedup_key(row):
    event_type = str(row.get("event_type") or row.get("type") or "").upper()
    timestamp = row.get("event_time") or row.get("timestamp")
    if isinstance(timestamp, datetime):
        timestamp = int(timestamp.astimezone(timezone.utc).timestamp())
    asset = row.get("condition_id") if event_type == "REDEEM" else row.get("token_id")
    parts = [
        str(row.get("wallet") or row.get("proxy_wallet") or "").lower(),
        event_type,
        str(row.get("transaction_hash") or "").lower(),
        str(asset or ""),
        "" if event_type == "REDEEM" else str(row.get("side") or "").upper(),
        _normalize(row.get("size")),
        _normalize(row.get("usdc_size")),
        str(timestamp or 0),
    ]
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


revision = "0013_source_event_dedup"
down_revision = "0012_reconcile_in_flight"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("source_events", sa.Column("dedup_key", sa.Text(), nullable=True))
    op.add_column("source_events", sa.Column("superseded_by", sa.BigInteger(), nullable=True))
    op.create_foreign_key(
        "fk_source_events_superseded_by",
        "source_events",
        "source_events",
        ["superseded_by"],
        ["id"],
    )
    connection = op.get_bind()
    rows = connection.execute(sa.text("select * from source_events order by id")).mappings().all()
    statement = sa.text("update source_events set dedup_key=:key where id=:id")
    for row in rows:
        connection.execute(statement, {"key": dedup_key(dict(row)), "id": row["id"]})


def downgrade() -> None:
    op.drop_constraint("fk_source_events_superseded_by", "source_events", type_="foreignkey")
    op.drop_column("source_events", "superseded_by")
    op.drop_column("source_events", "dedup_key")
