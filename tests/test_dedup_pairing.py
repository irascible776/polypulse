import pytest

from app.ingestion.dedup import dedup_key, pair_enrichment


def row(identity, enriched):
    return {
        "id": identity,
        "api_order": identity,
        "outcome_index": 0 if enriched else 999,
        "raw": {"event_slug": "event" if enriched else ""},
        "superseded_by": None,
    }


@pytest.mark.parametrize(
    "statuses,expected",
    [
        ([False, True], {1: 2}),
        ([True, True], {}),
        ([False, False, True, True], {1: 3, 2: 4}),
        ([True, False], {2: 1}),
    ],
)
def test_pairs_and_idempotence(statuses, expected):
    rows = [row(i + 1, status) for i, status in enumerate(statuses)]
    assert pair_enrichment(rows) == expected
    for item in rows:
        item["superseded_by"] = expected.get(item["id"])
    assert pair_enrichment(rows) == {}


def test_redeem_identity_ignores_token_and_enrichment():
    event = {
        "wallet": "w",
        "event_type": "REDEEM",
        "transaction_hash": "tx",
        "condition_id": "c",
        "size": "1",
        "usdc_size": "2",
        "timestamp": 5,
    }
    assert dedup_key(event) == dedup_key({**event, "token_id": "new", "outcome_index": 0})
    assert dedup_key(event) != dedup_key({**event, "condition_id": "different"})


def test_two_unenriched_without_twin_stay_live():
    assert pair_enrichment([row(1, False), row(2, False)]) == {}


def test_unenriched_and_two_enriched_leave_two_live():
    assert pair_enrichment([row(1, False), row(2, True), row(3, True)]) == {1: 2}


@pytest.mark.asyncio
async def test_projected_trade_is_recomputed_after_enrichment():
    import uuid
    from decimal import Decimal

    from sqlalchemy import select

    from app.accounting.projector import AccountingProjector
    from app.ingestion.writer import write_event_batch
    from app.storage.database import async_session_factory
    from app.storage.models import Position, SourceEvent, WalletTotal

    wallet = f"dedup-{uuid.uuid4()}"
    event = {
        "type": "TRADE",
        "side": "BUY",
        "timestamp": 1700000000,
        "size": "2",
        "usdc_size": "1",
        "condition_id": "condition",
        "token_id": "token",
        "transaction_hash": "tx",
        "outcome_index": 999,
        "event_slug": "",
    }
    async with async_session_factory() as session:
        await write_event_batch(session, wallet, "fixture", [(event, "old")])
        projector = AccountingProjector()
        await projector.catch_up_wallet(session, wallet)
        before = await session.get(Position, (wallet, "condition", "token"))
        before_shares = before.shares
        await write_event_batch(
            session, wallet, "live", [(dict(event, outcome_index=0, event_slug="event"), "new")]
        )
        total = await session.get(WalletTotal, wallet, populate_existing=True)
        assert total.recompute_needed
        old = await session.scalar(
            select(SourceEvent).where(
                SourceEvent.wallet == wallet, SourceEvent.source_event_id == "old"
            )
        )
        assert old.superseded_by is not None
        result = await projector.catch_up_wallet(session, wallet)
        assert result.is_recompute
        after = await session.get(Position, (wallet, "condition", "token"), populate_existing=True)
        assert before_shares == after.shares == Decimal("2")
        await session.rollback()


@pytest.mark.asyncio
async def test_legacy_double_projected_pair_drops_one_fill():
    import uuid
    from datetime import datetime, timezone
    from decimal import Decimal

    from sqlalchemy import func, select

    from app.accounting.projector import AccountingProjector
    from app.ingestion.writer import prepare_source_event_record, supersede_trade_pairs
    from app.storage.database import async_session_factory
    from app.storage.models import Position, PositionLedger, SourceEvent

    wallet = f"legacy-dedup-{uuid.uuid4()}"
    event = {
        "type": "TRADE",
        "side": "BUY",
        "timestamp": 1700000000,
        "size": "2",
        "usdc_size": "1",
        "condition_id": "condition",
        "token_id": "token",
        "transaction_hash": "tx",
        "outcome_index": 999,
        "event_slug": "",
    }
    async with async_session_factory() as session:
        key = dedup_key({**event, "wallet": wallet})
        for order, raw in enumerate((event, dict(event, outcome_index=0, event_slug="event")), 1):
            record = prepare_source_event_record(
                raw, f"id-{order}", wallet, "fixture", order, False, datetime.now(timezone.utc)
            )
            session.add(SourceEvent(**record, dedup_key=key))
        await session.flush()
        projector = AccountingProjector()
        await projector.catch_up_wallet(session, wallet)
        position = await session.get(Position, (wallet, "condition", "token"))
        assert position.shares == Decimal("4")
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PositionLedger)
                .where(PositionLedger.wallet == wallet)
            )
            == 2
        )
        assert len(await supersede_trade_pairs(session, wallet, key)) == 1
        assert (await projector.catch_up_wallet(session, wallet)).is_recompute
        position = await session.get(
            Position, (wallet, "condition", "token"), populate_existing=True
        )
        assert position.shares == Decimal("2")
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PositionLedger)
                .where(PositionLedger.wallet == wallet)
            )
            == 1
        )
        await session.rollback()
