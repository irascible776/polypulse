import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.accounting.checkpoints import configure_transaction, notify_publication
from app.accounting.checkpoints import save_checkpoint as store_checkpoint
from app.accounting.deadline import bounded_projection
from app.accounting.engine import advance_position, compute_condition_events, resolve_redeem_token
from app.accounting.executor import ConditionExecutor
from app.accounting.types import AccountingEvent, PositionState
from app.config import get_settings
from app.storage.models import (
    AccountingCheckpoint,
    ConditionCheckpoint,
    ConditionReplayDirty,
    Position,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    SourceEvent,
    WalletTotal,
)


@dataclass
class ProjectorRunSummary:
    wallet: str
    events_processed: int
    trades_applied: int
    redeems_applied: int
    unsupported_count: int
    rejects_count: int
    last_applied_api_order: int
    is_recompute: bool
    aggregation_rows_read: int = 0
    conditions_replayed: int = 0


class AccountingProjector:
    """Build wallet positions and ledger entries deterministically."""

    def __init__(
        self,
        executor: ConditionExecutor | None = None,
        timeout: int = 120,
        condition_replay: bool = False,
    ):
        self.executor = executor
        self.timeout = timeout
        self.condition_replay = condition_replay

    async def get_or_create_wallet_total(
        self, session: AsyncSession, wallet: str, for_update: bool = True
    ) -> WalletTotal:
        """Fetch or initialize the wallet_totals row with an exclusive lock."""
        await configure_transaction(session)
        await session.execute(
            insert(WalletTotal)
            .values(
                wallet=wallet,
                total_realized_pnl=Decimal("0"),
                total_trade_count=0,
                total_redeem_count=0,
                unsupported_event_count=0,
                reject_count=0,
                last_applied_api_order=0,
                recompute_count=0,
                recompute_needed=False,
                updated_at=datetime.now(timezone.utc),
            )
            .on_conflict_do_nothing(index_elements=["wallet"])
        )
        stmt = select(WalletTotal).where(WalletTotal.wallet == wallet)
        if for_update:
            stmt = stmt.with_for_update()

        result = await session.execute(stmt.execution_options(populate_existing=True))
        return result.scalar_one()

    async def get_or_create_cursor(self, session: AsyncSession, wallet: str) -> ProjectorCursor:
        """Fetch or initialize the projector cursor."""
        stmt = select(ProjectorCursor).where(ProjectorCursor.wallet == wallet)
        result = await session.execute(stmt.execution_options(populate_existing=True))
        cursor = result.scalar_one_or_none()
        if cursor is None:
            cursor = ProjectorCursor(
                wallet=wallet,
                last_applied_api_order=0,
                updated_at=datetime.now(timezone.utc),
            )
            session.add(cursor)
            await session.flush()
        return cursor

    @bounded_projection
    async def catch_up_wallet(
        self, session: AsyncSession, wallet: str, batch_size: int = 5000
    ) -> ProjectorRunSummary:
        """
        Incrementally catches up a wallet's positions from its saved projector cursor.
        Protected by an exclusive row lock on wallet_totals.
        """
        # 1. Acquire exclusive per-wallet lock
        wallet_total = await self.get_or_create_wallet_total(session, wallet, for_update=True)
        if wallet_total.recompute_needed:
            return await self.replay_from_checkpoint(session, wallet, wallet_total)
        cursor = await self.get_or_create_cursor(session, wallet)

        start_order = cursor.last_applied_api_order

        # 2. Fetch new source events in strict api_order
        stmt = (
            select(SourceEvent)
            .where(
                SourceEvent.wallet == wallet,
                SourceEvent.api_order > start_order,
                SourceEvent.superseded_by.is_(None),
            )
            .order_by(SourceEvent.api_order.asc())
            .limit(batch_size)
        )
        events_res = await session.execute(stmt)
        source_events = events_res.scalars().all()

        if not source_events:
            covered = await session.scalar(
                select(func.coalesce(func.max(SourceEvent.api_order), 0)).where(
                    SourceEvent.wallet == wallet
                )
            )
            if covered > start_order:
                cursor.last_applied_api_order = wallet_total.last_applied_api_order = covered
                cursor.updated_at = wallet_total.updated_at = datetime.now(timezone.utc)
                wallet_total.publication_revision += 1
                await session.flush()
                await notify_publication(session, wallet)
            return ProjectorRunSummary(
                wallet=wallet,
                events_processed=0,
                trades_applied=0,
                redeems_applied=0,
                unsupported_count=0,
                rejects_count=0,
                last_applied_api_order=max(start_order, covered),
                is_recompute=False,
            )

        return await self._apply_event_batch(
            session=session,
            wallet=wallet,
            wallet_total=wallet_total,
            cursor=cursor,
            source_events=source_events,
            is_recompute=False,
        )

    async def replay_from_checkpoint(
        self, session: AsyncSession, wallet: str, total: WalletTotal
    ) -> ProjectorRunSummary:
        if total.recompute_from_time is None or total.recompute_from_order is None:
            return await self.recompute_wallet(session, wallet)
        dirty = (total.recompute_from_time, total.recompute_from_order)
        checkpoint = await session.scalar(
            select(AccountingCheckpoint)
            .where(
                AccountingCheckpoint.wallet == wallet,
                tuple_(AccountingCheckpoint.boundary_time, AccountingCheckpoint.boundary_order)
                < dirty,
            )
            .order_by(
                AccountingCheckpoint.boundary_time.desc(),
                AccountingCheckpoint.boundary_order.desc(),
            )
            .limit(1)
        )
        if checkpoint is None:
            return await self.recompute_wallet(session, wallet)

        if self.condition_replay:
            selective = await self._replay_conditions(session, wallet, total, checkpoint)
            if selective is not None:
                return selective
            logging.getLogger("accounting").info(
                "condition_replay_fallback wallet=%s reason=incompatible_or_missing_metadata",
                wallet,
            )

        boundary = (checkpoint.boundary_time, checkpoint.boundary_order)
        await session.execute(delete(Position).where(Position.wallet == wallet))
        await session.execute(
            delete(ConditionCheckpoint).where(ConditionCheckpoint.wallet == wallet)
        )
        await session.execute(
            delete(ConditionReplayDirty).where(ConditionReplayDirty.wallet == wallet)
        )
        for model in (PositionLedger, PositionLedgerReject):
            await session.execute(
                delete(model).where(
                    model.wallet == wallet,
                    tuple_(model.event_time, model.api_order) > boundary,
                )
            )
        await session.execute(
            delete(AccountingCheckpoint).where(
                AccountingCheckpoint.wallet == wallet,
                tuple_(AccountingCheckpoint.boundary_time, AccountingCheckpoint.boundary_order)
                > boundary,
            )
        )
        for position in checkpoint.positions:
            session.add(
                Position(
                    wallet=wallet,
                    condition_id=position["condition_id"],
                    token_id=position["token_id"],
                    outcome_index=position["outcome_index"],
                    shares=Decimal(position["shares"]),
                    avg_cost_basis=Decimal(position["avg_cost_basis"]),
                    realized_pnl=Decimal(position["realized_pnl"]),
                    last_applied_api_order=position["last_applied_api_order"],
                    updated_at=datetime.fromisoformat(position["updated_at"]),
                )
            )
        for key, value in checkpoint.totals.items():
            setattr(total, key, Decimal(value) if key == "total_realized_pnl" else value)
        total.recompute_count += 1
        total.publication_revision += 1
        total.recompute_needed = False
        total.recompute_from_time = None
        total.recompute_from_order = None
        total.last_recompute_at = datetime.now(timezone.utc)
        total.last_applied_api_order = checkpoint.last_api_order
        cursor = await self.get_or_create_cursor(session, wallet)
        cursor.last_applied_api_order = checkpoint.last_api_order
        await session.flush()
        return await self._replay_batches(session, wallet, total, cursor, boundary)

    async def _replay_conditions(self, session, wallet, total, checkpoint):
        """Replay affected positions; fold aggregate deltas in exact reference order.

        The common wallet checkpoint is deliberately conservative: all restored
        condition snapshots must describe that same boundary. No partial result
        commits, and no global watermark advances independently of this transaction.
        """
        boundary = (checkpoint.boundary_time, checkpoint.boundary_order)
        markers = (
            await session.scalars(
                select(ConditionReplayDirty)
                .where(ConditionReplayDirty.wallet == wallet)
                .limit(5001)
            )
        ).all()
        if not markers or len(markers) > 5000 or any(not marker.condition_id for marker in markers):
            return None
        if min((m.boundary_time, m.boundary_order) for m in markers) != (
            total.recompute_from_time,
            total.recompute_from_order,
        ):
            return None
        cursor = await self.get_or_create_cursor(session, wallet)
        # Include normally ordered arrivals waiting alongside the corrections.
        pending = set(
            (
                await session.scalars(
                    select(SourceEvent.condition_id)
                    .where(
                        SourceEvent.wallet == wallet,
                        SourceEvent.api_order > cursor.last_applied_api_order,
                        SourceEvent.superseded_by.is_(None),
                    )
                    .distinct()
                    .limit(5001)
                )
            ).all()
        )
        conditions = {marker.condition_id for marker in markers} | pending
        if None in conditions or "" in conditions or len(conditions) > 5000:
            return None
        snapshots = (
            await session.scalars(
                select(ConditionCheckpoint).where(
                    ConditionCheckpoint.wallet == wallet,
                    ConditionCheckpoint.condition_id.in_(conditions),
                    ConditionCheckpoint.boundary_time == boundary[0],
                    ConditionCheckpoint.boundary_order == boundary[1],
                )
            )
        ).all()
        by_condition = {snapshot.condition_id: snapshot for snapshot in snapshots}
        baseline = {}
        for position in checkpoint.positions:
            baseline.setdefault(position["condition_id"], []).append(position)
        for condition in conditions:
            saved = by_condition.get(condition)
            if condition in baseline and (
                saved is None or saved.format_version != 1 or saved.positions != baseline[condition]
            ):
                return None
            if saved is not None and saved.format_version != 1:
                return None

        # All fallback checks precede mutation.
        await session.execute(
            delete(Position).where(Position.wallet == wallet, Position.condition_id.in_(conditions))
        )
        for model in (PositionLedger, PositionLedgerReject):
            await session.execute(
                delete(model).where(
                    model.wallet == wallet,
                    model.condition_id.in_(conditions),
                    tuple_(model.event_time, model.api_order) > boundary,
                )
            )
        for model in (AccountingCheckpoint, ConditionCheckpoint):
            await session.execute(
                delete(model).where(
                    model.wallet == wallet,
                    tuple_(model.boundary_time, model.boundary_order) > boundary,
                )
            )
        for condition in conditions:
            for position in baseline.get(condition, []):
                values = dict(position)
                for key in ("shares", "avg_cost_basis", "realized_pnl"):
                    values[key] = Decimal(values[key])
                values["updated_at"] = datetime.fromisoformat(values["updated_at"])
                session.add(Position(wallet=wallet, **values))
        await session.flush()
        result = ProjectorRunSummary(wallet, 0, 0, 0, 0, 0, 0, True)
        after = boundary
        while True:
            events = (
                await session.scalars(
                    select(SourceEvent)
                    .where(
                        SourceEvent.wallet == wallet,
                        SourceEvent.condition_id.in_(conditions),
                        SourceEvent.superseded_by.is_(None),
                        tuple_(SourceEvent.event_time, SourceEvent.api_order) > after,
                    )
                    .order_by(SourceEvent.event_time, SourceEvent.api_order)
                    .limit(5000)
                )
            ).all()
            if not events:
                break
            batch = await self._apply_event_batch(
                session, wallet, total, cursor, events, True, save_checkpoint=False
            )
            for field in (
                "events_processed",
                "trades_applied",
                "redeems_applied",
                "unsupported_count",
                "rejects_count",
            ):
                setattr(result, field, getattr(result, field) + getattr(batch, field))
            after = (events[-1].event_time, events[-1].api_order)

        # Python Decimal folding preserves the reference's rounding order. SQL SUM
        # or adding pre-summed condition PnL would not have that property.
        for key, value in checkpoint.totals.items():
            setattr(total, key, Decimal(value) if key == "total_realized_pnl" else value)
        stmt = (
            select(
                SourceEvent.event_type,
                SourceEvent.event_time,
                SourceEvent.api_order,
                PositionLedger.id,
                PositionLedger.realized_pnl_delta,
                PositionLedgerReject.id,
            )
            .select_from(SourceEvent)
            .outerjoin(
                PositionLedger,
                (
                    (PositionLedger.wallet == SourceEvent.wallet)
                    & (PositionLedger.source_event_id == SourceEvent.source_event_id)
                ),
            )
            .outerjoin(
                PositionLedgerReject,
                (
                    (PositionLedgerReject.wallet == SourceEvent.wallet)
                    & (PositionLedgerReject.source_event_id == SourceEvent.source_event_id)
                ),
            )
            .where(
                SourceEvent.wallet == wallet,
                SourceEvent.superseded_by.is_(None),
                tuple_(SourceEvent.event_time, SourceEvent.api_order) > boundary,
            )
            .order_by(SourceEvent.event_time, SourceEvent.api_order)
        )
        rows = await session.stream(stmt.execution_options(yield_per=1000))
        final_boundary = boundary
        previous = None
        async for kind, when, order, ledger_id, delta, reject_id in rows:
            if order == previous:
                raise RuntimeError("Duplicate accounting disposition during aggregate replay")
            previous = order
            result.aggregation_rows_read += 1
            final_boundary = (when, order)
            if kind.upper() not in ("TRADE", "REDEEM"):
                if ledger_id is not None or reject_id is not None:
                    raise RuntimeError("Unsupported event has a mutating disposition")
                total.unsupported_event_count += 1
            elif (ledger_id is None) == (reject_id is None):
                raise RuntimeError("Missing or conflicting accounting disposition")
            elif reject_id is not None:
                total.reject_count += 1
            else:
                total.total_realized_pnl += delta
                if kind.upper() == "TRADE":
                    total.total_trade_count += 1
                else:
                    total.total_redeem_count += 1
        # Superseded observations count as covered, without economic effects.
        last_order = await session.scalar(
            select(func.coalesce(func.max(SourceEvent.api_order), 0)).where(
                SourceEvent.wallet == wallet
            )
        )
        now = datetime.now(timezone.utc)
        cursor.last_applied_api_order = total.last_applied_api_order = last_order
        cursor.updated_at = total.updated_at = now
        total.recompute_count += 1
        total.publication_revision += 1
        total.last_recompute_at = now
        total.recompute_needed = False
        total.recompute_from_time = total.recompute_from_order = None
        await session.execute(
            delete(ConditionReplayDirty).where(ConditionReplayDirty.wallet == wallet)
        )
        if final_boundary > boundary:
            positions = (
                await session.scalars(select(Position).where(Position.wallet == wallet))
            ).all()
            await store_checkpoint(session, wallet, total, final_boundary, positions)
        await session.flush()
        result.last_applied_api_order = last_order
        result.conditions_replayed = len(conditions)
        await notify_publication(session, wallet)
        logging.getLogger("accounting").info(
            "condition_replay wallet=%s conditions=%s events=%s aggregation_rows=%s",
            wallet,
            result.conditions_replayed,
            result.events_processed,
            result.aggregation_rows_read,
        )
        return result

    @bounded_projection
    async def recompute_wallet(self, session: AsyncSession, wallet: str) -> ProjectorRunSummary:
        """
        Performs a full deterministic recompute of a wallet from zero state.
        Deletes derived positions and position_ledger, and replays all source_events.
        """
        # 1. Lock wallet row
        wallet_total = await self.get_or_create_wallet_total(session, wallet, for_update=True)
        cursor = await self.get_or_create_cursor(session, wallet)

        # 2. Clear existing derived tables for this wallet
        await session.execute(delete(Position).where(Position.wallet == wallet))
        await session.execute(
            delete(ConditionCheckpoint).where(ConditionCheckpoint.wallet == wallet)
        )
        await session.execute(
            delete(ConditionReplayDirty).where(ConditionReplayDirty.wallet == wallet)
        )
        await session.execute(delete(PositionLedger).where(PositionLedger.wallet == wallet))
        await session.execute(
            delete(PositionLedgerReject).where(PositionLedgerReject.wallet == wallet)
        )
        await session.execute(
            delete(AccountingCheckpoint).where(AccountingCheckpoint.wallet == wallet)
        )

        # Reset cumulative totals
        wallet_total.total_realized_pnl = Decimal("0")
        wallet_total.total_trade_count = 0
        wallet_total.total_redeem_count = 0
        wallet_total.unsupported_event_count = 0
        wallet_total.reject_count = 0
        wallet_total.recompute_count += 1
        wallet_total.publication_revision += 1
        wallet_total.recompute_needed = False
        wallet_total.recompute_from_time = None
        wallet_total.recompute_from_order = None
        wallet_total.last_recompute_at = datetime.now(timezone.utc)
        wallet_total.last_applied_api_order = 0
        cursor.last_applied_api_order = 0
        await session.flush()

        return await self._replay_batches(session, wallet, wallet_total, cursor, None)

    async def _replay_batches(
        self, session, wallet, wallet_total, cursor, boundary, batch_size=5000
    ) -> ProjectorRunSummary:
        result = ProjectorRunSummary(wallet, 0, 0, 0, 0, 0, cursor.last_applied_api_order, True)
        while True:
            stmt = (
                select(SourceEvent)
                .where(SourceEvent.wallet == wallet, SourceEvent.superseded_by.is_(None))
                .order_by(SourceEvent.event_time, SourceEvent.api_order)
                .limit(batch_size)
            )
            if boundary is not None:
                stmt = stmt.where(tuple_(SourceEvent.event_time, SourceEvent.api_order) > boundary)
            events = (await session.execute(stmt)).scalars().all()
            if not events:
                covered = await session.scalar(
                    select(func.coalesce(func.max(SourceEvent.api_order), 0)).where(
                        SourceEvent.wallet == wallet
                    )
                )
                cursor.last_applied_api_order = wallet_total.last_applied_api_order = covered
                cursor.updated_at = wallet_total.updated_at = datetime.now(timezone.utc)
                result.last_applied_api_order = covered
                await session.flush()
                await notify_publication(session, wallet)
                return result
            batch = await self._apply_event_batch(
                session, wallet, wallet_total, cursor, events, True
            )
            result.events_processed += batch.events_processed
            result.trades_applied += batch.trades_applied
            result.redeems_applied += batch.redeems_applied
            result.unsupported_count += batch.unsupported_count
            result.rejects_count += batch.rejects_count
            result.last_applied_api_order = batch.last_applied_api_order
            boundary = (events[-1].event_time, events[-1].api_order)

    async def _apply_event_batch(
        self,
        session: AsyncSession,
        wallet: str,
        wallet_total: WalletTotal,
        cursor: ProjectorCursor,
        source_events: list[SourceEvent],
        is_recompute: bool,
        *,
        save_checkpoint: bool = True,
    ) -> ProjectorRunSummary:
        """Internal helper to apply a batch of events with state caching."""
        # Redemption can depend on every token in a selected condition, but never
        # on unrelated conditions. Full state is loaded only when saving a snapshot.
        pos_stmt = select(Position).where(
            Position.wallet == wallet,
            Position.condition_id.in_({event.condition_id or "" for event in source_events}),
        )
        pos_res = await session.execute(pos_stmt.execution_options(populate_existing=True))
        active_positions: dict[tuple[str, str], PositionState] = {}
        existing_positions: dict[tuple[str, str], Position] = {}
        for p in pos_res.scalars().all():
            key = (p.condition_id, p.token_id)
            existing_positions[key] = p
            active_positions[key] = PositionState(
                wallet=p.wallet,
                condition_id=p.condition_id,
                token_id=p.token_id,
                outcome_index=p.outcome_index,
                shares=p.shares,
                avg_cost_basis=p.avg_cost_basis,
                realized_pnl=p.realized_pnl,
                last_applied_api_order=p.last_applied_api_order,
                updated_at=p.updated_at,
            )

        trades_applied = 0
        redeems_applied = 0
        unsupported_count = 0
        rejects_count = 0
        now_ts = datetime.now(timezone.utc)

        ledger_rows_to_insert: list[PositionLedger] = []
        reject_rows_to_insert: list[PositionLedgerReject] = []

        condition_events: dict[str, list[AccountingEvent]] = {}
        for se in source_events:
            if se.wallet != wallet:
                raise RuntimeError("Selected event belongs to another wallet")
            if se.event_type.upper() not in ("TRADE", "REDEEM"):
                continue
            event = AccountingEvent(
                source_event_id=se.source_event_id,
                api_order=se.api_order,
                event_type=se.event_type,
                event_time=se.event_time,
                wallet=wallet,
                condition_id=se.condition_id,
                token_id=se.token_id,
                outcome_index=se.outcome_index,
                side=se.side,
                size=se.size,
                usdc_size=se.usdc_size,
                price_reported=se.price_reported,
                cash_price_gap=se.cash_price_gap,
                raw=se.raw,
            )
            condition_events.setdefault(se.condition_id or "", []).append(event)
        effects = {}
        work = []
        positions_by_condition: dict[str, dict[tuple[str, str], PositionState]] = {}
        for key, state in active_positions.items():
            positions_by_condition.setdefault(key[0], {})[key] = state
        for condition_id, events in condition_events.items():
            condition_positions = positions_by_condition.get(condition_id, {})
            work.append((condition_id, events, condition_positions))
        if self.executor is None or len(work) < 2 or "" in condition_events:
            if self.executor is not None:
                logging.getLogger("accounting").info(
                    "condition_serial wallet=%s conditions=%s unclassified=%s",
                    wallet,
                    len(work),
                    "" in condition_events,
                )
            batches = [
                compute_condition_events(wallet, condition_id, events, positions, now_ts)
                for condition_id, events, positions in work
            ]
        else:
            results = await self.executor.compute(wallet, work, now_ts, self.timeout)
            batches = [result[3] for result in results]
            logging.getLogger("accounting").info(
                "condition_compute wallet=%s tasks=%s first_process_intervals=%s",
                wallet,
                len(results),
                [(r[4], r[5], r[6]) for r in results[:16]],
            )
        for (condition_id, events, _), batch in zip(work, batches, strict=True):
            validation_positions = dict(positions_by_condition.get(condition_id, {}))
            expected = [event.source_event_id for event in events]
            if [effect.event.source_event_id for effect in batch] != expected:
                raise RuntimeError("Condition effects do not match the selected events")
            for selected, effect in zip(events, batch, strict=True):
                if (
                    effect.event.wallet != wallet
                    or (effect.event.condition_id or "") != condition_id
                ):
                    raise RuntimeError(
                        "Condition effect belongs to a different wallet or condition"
                    )
                if (
                    replace(
                        effect.event, condition_id=selected.condition_id, token_id=selected.token_id
                    )
                    != selected
                ):
                    raise RuntimeError("Condition effect does not match its source event")
                if effect.event.source_event_id in effects:
                    raise RuntimeError("Duplicate condition effect")
                expected_token = selected.token_id
                if selected.event_type.upper() == "REDEEM" and not expected_token:
                    resolved_token, _ = resolve_redeem_token(
                        condition_id, selected.outcome_index, validation_positions
                    )
                    if resolved_token is not None:
                        expected_token = resolved_token
                if effect.event.token_id != expected_token:
                    raise RuntimeError("Condition effect changed the selected token")
                result = effect.result
                if result.is_unsupported or (result.success is None) == (result.reject is None):
                    raise RuntimeError("Condition effect must have exactly one disposition")
                disposition = result.success or result.reject
                if (
                    disposition.wallet != wallet
                    or (disposition.condition_id or "") != condition_id
                    or disposition.token_id != effect.event.token_id
                ):
                    raise RuntimeError("Condition disposition has incorrect ownership")
                if result.success is not None:
                    success = result.success
                    amounts = (
                        success.shares_before,
                        success.shares_delta,
                        success.shares_after,
                        success.avg_cost_before,
                        success.avg_cost_after,
                        success.cash_delta,
                        success.realized_pnl_delta,
                    )
                    if any(
                        not isinstance(value, Decimal) or not value.is_finite() for value in amounts
                    ):
                        raise RuntimeError("Condition effect contains invalid financial amounts")
                    if (
                        success.shares_after < 0
                        or success.shares_before + success.shares_delta != success.shares_after
                    ):
                        raise RuntimeError("Condition effect violates share conservation")
                    key = (condition_id, effect.event.token_id)
                    prior = validation_positions.get(key)
                    if result.success.shares_before != (
                        prior.shares if prior else Decimal(0)
                    ) or result.success.avg_cost_before != (
                        prior.avg_cost_basis if prior else Decimal(0)
                    ):
                        raise RuntimeError("Condition effect uses stale position state")
                    expected_position = advance_position(
                        prior, effect.event, result.success, now_ts
                    )
                    if effect.position != expected_position:
                        raise RuntimeError("Condition position does not match its ledger effect")
                    validation_positions[key] = effect.position
                elif effect.position is not None:
                    raise RuntimeError("Rejected event cannot change a position")
                effects[effect.event.source_event_id] = effect

        last_order = cursor.last_applied_api_order
        touched_positions = set()

        for se in source_events:
            event_type = se.event_type.upper()
            last_order = max(last_order, se.api_order)

            if event_type not in ("TRADE", "REDEEM"):
                unsupported_count += 1
                wallet_total.unsupported_event_count += 1
                continue

            effect = effects[se.source_event_id]
            acct_event, result, next_position = effect.event, effect.result, effect.position
            resolved_condition_id = acct_event.condition_id
            resolved_token_id = acct_event.token_id
            pos_key = (resolved_condition_id, resolved_token_id)

            if result.reject is not None:
                rejects_count += 1
                reject_rows_to_insert.append(
                    PositionLedgerReject(
                        wallet=wallet,
                        source_event_id=se.source_event_id,
                        api_order=se.api_order,
                        event_type=se.event_type,
                        event_time=se.event_time,
                        condition_id=resolved_condition_id,
                        token_id=resolved_token_id,
                        size=se.size,
                        usdc_size=se.usdc_size,
                        reject_reason=result.reject.reject_reason,
                        created_at=now_ts,
                    )
                )
                continue

            if result.success is not None:
                suc = result.success
                if event_type == "TRADE":
                    trades_applied += 1
                    wallet_total.total_trade_count += 1
                elif event_type == "REDEEM":
                    redeems_applied += 1
                    wallet_total.total_redeem_count += 1

                wallet_total.total_realized_pnl += suc.realized_pnl_delta

                active_positions[pos_key] = next_position
                touched_positions.add(pos_key)

                ledger_rows_to_insert.append(
                    PositionLedger(
                        wallet=wallet,
                        condition_id=resolved_condition_id,
                        token_id=resolved_token_id,
                        source_event_id=se.source_event_id,
                        api_order=se.api_order,
                        event_type=se.event_type,
                        event_time=se.event_time,
                        shares_before=suc.shares_before,
                        shares_delta=suc.shares_delta,
                        shares_after=suc.shares_after,
                        avg_cost_before=suc.avg_cost_before,
                        avg_cost_after=suc.avg_cost_after,
                        cash_delta=suc.cash_delta,
                        realized_pnl_delta=suc.realized_pnl_delta,
                        is_recompute=is_recompute,
                        cash_price_gap=suc.cash_price_gap,
                        created_at=now_ts,
                    )
                )

        # Batch insert audit ledger & reject rows
        if ledger_rows_to_insert:
            session.add_all(ledger_rows_to_insert)
        if reject_rows_to_insert:
            session.add_all(reject_rows_to_insert)
        wallet_total.reject_count += rejects_count

        # Upsert active positions to database
        for (cid, tid), st in active_positions.items():
            if (cid, tid) not in touched_positions:
                continue
            existing_pos = existing_positions.get((cid, tid))

            if existing_pos:
                existing_pos.shares = st.shares
                existing_pos.avg_cost_basis = st.avg_cost_basis
                existing_pos.realized_pnl = st.realized_pnl
                existing_pos.outcome_index = st.outcome_index
                existing_pos.last_applied_api_order = st.last_applied_api_order
                existing_pos.updated_at = now_ts
            else:
                session.add(
                    Position(
                        wallet=wallet,
                        condition_id=cid,
                        token_id=tid,
                        outcome_index=st.outcome_index,
                        shares=st.shares,
                        avg_cost_basis=st.avg_cost_basis,
                        realized_pnl=st.realized_pnl,
                        last_applied_api_order=st.last_applied_api_order,
                        updated_at=now_ts,
                    )
                )

        # Advance cursors and totals
        wallet_total.last_applied_api_order = last_order
        wallet_total.publication_revision += 1
        wallet_total.updated_at = now_ts
        cursor.last_applied_api_order = last_order
        cursor.updated_at = now_ts

        await session.flush()

        if source_events and save_checkpoint:
            previous_order = await session.scalar(
                select(func.max(AccountingCheckpoint.last_api_order)).where(
                    AccountingCheckpoint.wallet == wallet
                )
            )
            if (
                is_recompute
                or previous_order is None
                or last_order - previous_order >= get_settings().accounting_checkpoint_interval
            ):
                boundary = max(source_events, key=lambda event: (event.event_time, event.api_order))
                positions = (
                    await session.scalars(
                        select(Position)
                        .where(Position.wallet == wallet)
                        .execution_options(populate_existing=True)
                    )
                ).all()
                await store_checkpoint(
                    session,
                    wallet,
                    wallet_total,
                    (boundary.event_time, boundary.api_order),
                    positions,
                )

        await notify_publication(session, wallet)
        return ProjectorRunSummary(
            wallet=wallet,
            events_processed=len(source_events),
            trades_applied=trades_applied,
            redeems_applied=redeems_applied,
            unsupported_count=unsupported_count,
            rejects_count=rejects_count,
            last_applied_api_order=last_order,
            is_recompute=is_recompute,
        )
