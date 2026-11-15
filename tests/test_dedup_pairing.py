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


@pytest.mark.parametrize(
    "value",
    [0, "", None, "1.50", "1.5E+2", "0.000001", 100, "123456789012345678901234567890.123456789"],
)
def test_frozen_migration_normalization_matches_runtime(value):
    import importlib

    from app.ingestion.identifiers import normalize_decimal_str

    migration = importlib.import_module("app.storage.migrations.versions.0013_source_event_dedup")
    assert migration._normalize(value) == normalize_decimal_str(value)


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


@pytest.mark.asyncio
async def test_legacy_duplicate_then_sell_recompute_pnl():
    import uuid
    from datetime import datetime, timezone
    from decimal import Decimal, localcontext

    from sqlalchemy import select

    from app.accounting.projector import AccountingProjector
    from app.ingestion.writer import prepare_source_event_record, supersede_trade_pairs
    from app.storage.database import async_session_factory
    from app.storage.models import Position, PositionLedger, SourceEvent, WalletTotal

    wallet = f"pnl-dedup-{uuid.uuid4()}"
    buy = {
        "type": "TRADE",
        "side": "BUY",
        "timestamp": 1700000000,
        "size": "10",
        "usdc_size": "4",
        "condition_id": "c",
        "token_id": "t",
        "transaction_hash": "b",
        "outcome_index": 999,
        "event_slug": "",
    }
    sell = {
        **buy,
        "side": "SELL",
        "timestamp": 1700000001,
        "size": "5",
        "usdc_size": "3",
        "transaction_hash": "s",
        "outcome_index": 0,
        "event_slug": "market",
    }
    async with async_session_factory() as session:
        key = dedup_key({**buy, "wallet": wallet})
        for order, raw in enumerate(
            (buy, dict(buy, outcome_index=0, event_slug="market"), sell), 1
        ):
            record = prepare_source_event_record(
                raw, f"event-{order}", wallet, "fixture", order, False, datetime.now(timezone.utc)
            )
            session.add(SourceEvent(**record, dedup_key=dedup_key(record)))
        await session.flush()
        projector = AccountingProjector()
        await projector.catch_up_wallet(session, wallet)
        total = await session.get(WalletTotal, wallet)
        before_pnl = total.total_realized_pnl
        before = dict(
            (
                await session.execute(
                    select(PositionLedger.source_event_id, PositionLedger.realized_pnl_delta).where(
                        PositionLedger.wallet == wallet
                    )
                )
            ).all()
        )
        await supersede_trade_pairs(session, wallet, key)
        await projector.catch_up_wallet(session, wallet)
        position = await session.get(Position, (wallet, "c", "t"), populate_existing=True)
        total = await session.get(WalletTotal, wallet, populate_existing=True)
        after = dict(
            (
                await session.execute(
                    select(PositionLedger.source_event_id, PositionLedger.realized_pnl_delta).where(
                        PositionLedger.wallet == wallet
                    )
                )
            ).all()
        )
        with localcontext() as ctx:
            ctx.prec = 28
            cost = Decimal("4") / Decimal("10")
            assert position.shares == Decimal("10") - Decimal("5")
            assert position.avg_cost_basis == cost
            assert total.total_realized_pnl == Decimal("3") - Decimal("5") * cost
            change = sum(
                (
                    after.get(i, Decimal(0)) - before.get(i, Decimal(0))
                    for i in before.keys() | after.keys()
                ),
                Decimal(0),
            )
            assert abs(total.total_realized_pnl - before_pnl - change) <= Decimal("1e-12")
        await session.rollback()
