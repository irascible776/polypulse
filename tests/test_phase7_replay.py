"""Database cases require migration 0024 on disposable test shards; never run on live data."""

import asyncio
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.accounting.engine import compute_condition_events
from app.accounting.executor import ConditionExecutor
from app.accounting.projector import AccountingProjector
from app.config import get_settings
from app.ingestion.writer import write_event_batch
from app.storage.database import wallet_session_factory
from app.storage.models import (
    ConditionReplayDirty,
    Position,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    WalletTotal,
)


def raw(condition, when, side="BUY", **changes):
    return {
        "type": "TRADE",
        "condition_id": condition,
        "token_id": condition + "-token",
        "timestamp": when,
        "side": side,
        "size": "3",
        "usdc_size": "1",
        "transaction_hash": f"{condition}-{when}-{side}",
        "outcome_index": 0,
        "event_slug": "test",
        **changes,
    }


async def economic_state(session, wallet):
    state = {}
    for model in (Position, PositionLedger, PositionLedgerReject, WalletTotal):
        ignored = {
            "id",
            "created_at",
            "updated_at",
            "is_recompute",
            "recompute_count",
            "publication_revision",
            "last_recompute_at",
        }
        columns = [column for column in model.__table__.columns if column.name not in ignored]
        rows = (await session.execute(select(*columns).where(model.wallet == wallet))).all()
        state[model.__tablename__] = sorted((tuple(row) for row in rows), key=repr)
    return state


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_condition", [None, "b", "new"])
async def test_local_correction_matches_rebuild_and_preserves_untouched(pending_condition):
    wallet = "0x" + uuid4().hex + "0" * 8
    async with wallet_session_factory(wallet)() as session:
        try:
            projector = AccountingProjector(condition_replay=True)
            await write_event_batch(
                session,
                wallet,
                wallet,
                [(raw(c, 100), f"{wallet}-{c}-buy") for c in ("a", "b", "untouched")],
            )
            await projector.catch_up_wallet(session, wallet)
            await write_event_batch(
                session,
                wallet,
                wallet,
                [(raw(c, 200, "SELL"), f"{wallet}-{c}-sell") for c in ("a", "b", "untouched")],
            )
            await projector.catch_up_wallet(session, wallet)
            before = await session.get(Position, (wallet, "untouched", "untouched-token"))
            untouched_time = before.updated_at
            revision = (await session.get(WalletTotal, wallet)).publication_revision
            arrivals = [(raw("a", 150), f"{wallet}-late")]
            if pending_condition:
                arrivals.append((raw(pending_condition, 250), f"{wallet}-pending"))
            await write_event_batch(session, wallet, wallet, arrivals)
            result = await projector.catch_up_wallet(session, wallet)
            assert result.conditions_replayed == (2 if pending_condition else 1)
            assert result.events_processed < result.aggregation_rows_read
            assert (
                await session.get(Position, (wallet, "untouched", "untouched-token"))
            ).updated_at == untouched_time
            assert (await session.get(WalletTotal, wallet)).publication_revision > revision
            assert not (
                await session.scalars(
                    select(ConditionReplayDirty).where(ConditionReplayDirty.wallet == wallet)
                )
            ).all()
            corrected = await economic_state(session, wallet)
            await AccountingProjector().recompute_wallet(session, wallet)
            assert await economic_state(session, wallet) == corrected
        finally:
            await session.rollback()


@pytest.mark.asyncio
async def test_supersession_replays_original_boundary_and_is_idempotent():
    wallet = "0x" + uuid4().hex + "0" * 8
    async with wallet_session_factory(wallet)() as session:
        try:
            projector = AccountingProjector(condition_replay=True)
            await write_event_batch(session, wallet, wallet, [(raw("a", 100), f"{wallet}-first")])
            await projector.catch_up_wallet(session, wallet)
            original = raw("a", 200, outcome_index=999, event_slug="")
            await write_event_batch(session, wallet, wallet, [(original, f"{wallet}-old")])
            await projector.catch_up_wallet(session, wallet)
            await write_event_batch(
                session,
                wallet,
                wallet,
                [(dict(original, outcome_index=0, event_slug="enriched"), f"{wallet}-new")],
            )
            result = await projector.catch_up_wallet(session, wallet)
            assert result.conditions_replayed == 1
            corrected = await economic_state(session, wallet)
            assert (await projector.catch_up_wallet(session, wallet)).events_processed == 0
            assert await economic_state(session, wallet) == corrected
            await AccountingProjector().recompute_wallet(session, wallet)
            assert await economic_state(session, wallet) == corrected
        finally:
            await session.rollback()


@pytest.mark.asyncio
async def test_supersession_refreshes_cursor_cached_before_another_owner_committed():
    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    async with factory() as stale:
        async with stale.begin():
            await write_event_batch(stale, wallet, wallet, [(raw("a", 100), wallet + "-first")])
            await AccountingProjector().catch_up_wallet(stale, wallet)
        cached = await stale.get(ProjectorCursor, wallet)
        assert cached.last_applied_api_order == 1
        await stale.commit()
        original = raw("a", 200, outcome_index=999, event_slug="")
        async with factory() as other, other.begin():
            await write_event_batch(other, wallet, wallet, [(original, wallet + "-old")])
            await AccountingProjector().catch_up_wallet(other, wallet)
        assert cached.last_applied_api_order == 1
        async with stale.begin():
            await write_event_batch(
                stale,
                wallet,
                wallet,
                [(dict(original, outcome_index=0, event_slug="enriched"), wallet + "-new")],
            )
            assert (await stale.get(WalletTotal, wallet)).recompute_needed
            assert cached.last_applied_api_order == 2
            await AccountingProjector(condition_replay=True).catch_up_wallet(stale, wallet)
            corrected = await economic_state(stale, wallet)
            await AccountingProjector().recompute_wallet(stale, wallet)
            assert await economic_state(stale, wallet) == corrected


@pytest.mark.asyncio
async def test_attempt_timeout_rolls_back_partial_publication(monkeypatch):
    import app.accounting.projector as module

    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    async with factory() as session, session.begin():
        await write_event_batch(session, wallet, wallet, [(raw("a", 100), wallet + "-first")])
    async with factory() as session:
        before = await economic_state(session, wallet)
    original = module.store_checkpoint
    reached = asyncio.Event()

    async def stall(*args, **kwargs):
        await original(*args, **kwargs)
        reached.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(module, "store_checkpoint", stall)
    monkeypatch.setattr(get_settings(), "accounting_attempt_timeout_seconds", 0.2)
    with pytest.raises(TimeoutError):
        async with factory() as session, session.begin():
            await AccountingProjector().catch_up_wallet(session, wallet)
    assert reached.is_set()
    async with factory() as session:
        assert await economic_state(session, wallet) == before
    monkeypatch.setattr(module, "store_checkpoint", original)
    monkeypatch.setattr(get_settings(), "accounting_attempt_timeout_seconds", 120)
    async with factory() as session, session.begin():
        assert (await AccountingProjector().catch_up_wallet(session, wallet)).events_processed == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "wrong_wallet", "wrong_token", "stale", "duplicate"])
async def test_bad_condition_result_cannot_publish(fault):
    class BadExecutor:
        async def compute(self, wallet, work, now, timeout):
            results = []
            for condition, events, positions in work:
                effects = compute_condition_events(wallet, condition, events, positions, now)
                effect = effects[0]
                if fault == "missing":
                    effects = []
                elif fault == "duplicate":
                    effects.append(effect)
                elif fault == "wrong_wallet":
                    effects[0] = replace(effect, event=replace(effect.event, wallet="other"))
                elif fault == "wrong_token":
                    effects[0] = replace(effect, event=replace(effect.event, token_id="other"))
                else:
                    effects[0] = replace(
                        effect, position=replace(effect.position, last_applied_api_order=999)
                    )
                results.append(("attempt", wallet, condition, effects, 1, 1, 2))
            return results

    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    async with factory() as session, session.begin():
        await write_event_batch(
            session, wallet, wallet, [(raw(c, 100), wallet + c) for c in ("a", "b")]
        )
    async with factory() as session:
        before = await economic_state(session, wallet)
    with pytest.raises((RuntimeError, ValueError)):
        async with factory() as session, session.begin():
            await AccountingProjector(executor=BadExecutor()).catch_up_wallet(session, wallet)
    async with factory() as session:
        assert await economic_state(session, wallet) == before


@pytest.mark.asyncio
async def test_real_process_projection_matches_serial_rebuild():
    wallet = "0x" + uuid4().hex + "0" * 8
    executor = ConditionExecutor(2)
    try:
        async with wallet_session_factory(wallet)() as session:
            await write_event_batch(
                session,
                wallet,
                wallet,
                [
                    (raw(c, ts, side), f"{wallet}-{c}-{ts}")
                    for ts, side in ((100, "BUY"), (200, "SELL"))
                    for c in ("a", "b")
                ],
            )
            await AccountingProjector(executor=executor).catch_up_wallet(session, wallet)
            parallel = await economic_state(session, wallet)
            await AccountingProjector().recompute_wallet(session, wallet)
            assert await economic_state(session, wallet) == parallel
            await session.rollback()
    finally:
        await executor.close()


@pytest.mark.asyncio
async def test_spaced_checkpoints_load_only_selected_conditions_and_replay_correctly(monkeypatch):
    from sqlalchemy import event

    from app.storage.models import AccountingCheckpoint

    monkeypatch.setattr(get_settings(), "accounting_checkpoint_interval", 3)
    wallet = "0x" + uuid4().hex + "0" * 8
    factory = wallet_session_factory(wallet)
    projector = AccountingProjector(condition_replay=True)
    async with factory() as session, session.begin():
        await write_event_batch(
            session, wallet, wallet, [(raw(c, 100), wallet + c) for c in ("a", "b")]
        )
        await projector.catch_up_wallet(session, wallet)
    async with factory() as session, session.begin():
        await write_event_batch(session, wallet, wallet, [(raw("a", 200), wallet + "-200")])
        loaded = []

        def track(session, instance):
            if isinstance(instance, Position):
                loaded.append(instance.condition_id)

        event.listen(session.sync_session, "loaded_as_persistent", track)
        await projector.catch_up_wallet(session, wallet)
        event.remove(session.sync_session, "loaded_as_persistent", track)
        assert loaded == ["a"]
        snapshots = (
            await session.scalars(
                select(AccountingCheckpoint).where(AccountingCheckpoint.wallet == wallet)
            )
        ).all()
        assert [s.last_api_order for s in snapshots] == [2]
        await write_event_batch(
            session,
            wallet,
            wallet,
            [(raw("a", ts), f"{wallet}-{ts}") for ts in (250, 300)],
        )
        await projector.catch_up_wallet(session, wallet)
        snapshots = (
            await session.scalars(
                select(AccountingCheckpoint)
                .where(AccountingCheckpoint.wallet == wallet)
                .order_by(AccountingCheckpoint.last_api_order)
            )
        ).all()
        assert [s.last_api_order for s in snapshots] == [2, 5]
        assert {p["condition_id"] for p in snapshots[-1].positions} == {"a", "b"}
        await write_event_batch(session, wallet, wallet, [(raw("a", 150), wallet + "-late")])
        result = await projector.catch_up_wallet(session, wallet)
        assert result.conditions_replayed == 1
        corrected = await economic_state(session, wallet)
        await projector.recompute_wallet(session, wallet)
        assert await economic_state(session, wallet) == corrected
        latest = await session.scalar(
            select(AccountingCheckpoint)
            .where(AccountingCheckpoint.wallet == wallet)
            .order_by(AccountingCheckpoint.boundary_time.desc())
            .limit(1)
        )
        assert {p["condition_id"] for p in latest.positions} == {"a", "b"}


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_worker_selective_replay_independent_of_process_pool(monkeypatch, enabled):
    import app.worker as module
    from app.ingestion.source import PageResult

    wallet = "0x" + uuid4().hex + "0" * 8
    settings = get_settings()
    monkeypatch.setattr(settings, "accounting_parallel_processes", 0)
    monkeypatch.setattr(settings, "accounting_condition_replay_wallets", wallet if enabled else "")
    factory = wallet_session_factory(wallet)
    async with factory() as session, session.begin():
        for ts in (100, 200):
            await write_event_batch(
                session,
                wallet,
                "data_api",
                [(raw(c, ts), f"{wallet}-{c}-{ts}") for c in ("a", "b")],
            )
            await AccountingProjector().catch_up_wallet(session, wallet)
        await write_event_batch(session, wallet, "data_api", [(raw("a", 150), wallet + "-late")])
    summaries = []

    class ObservedProjector(AccountingProjector):
        async def catch_up_wallet(self, *args, **kwargs):
            result = await super().catch_up_wallet(*args, **kwargs)
            summaries.append(result)
            return result

    class Source:
        source_tag = "data_api"

        async def fetch_page(self, **kwargs):
            return PageResult(events=[], next_cursor=None, has_more=False)

        async def close(self):
            pass

    monkeypatch.setattr(module, "AccountingProjector", ObservedProjector)
    worker = module.Worker()
    await worker.source.close()
    worker.source = Source()
    try:
        assert worker.accounting_executor is None
        outcome = await worker.wallet_cycle(wallet)
        assert "error" not in outcome
        assert summaries[0].conditions_replayed == (1 if enabled else 0)
        async with factory() as session:
            corrected = await economic_state(session, wallet)
            await AccountingProjector().recompute_wallet(session, wallet)
            assert await economic_state(session, wallet) == corrected
            await session.rollback()
    finally:
        await worker.close()
