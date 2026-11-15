from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.accounting.engine import apply_event
from app.accounting.types import AccountingEvent, PositionState
from app.storage.models import (
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


class AccountingProjector:
    """
    Projector that deterministically applies source_events in api_order
    to build wallet positions, real-time PnL, and audit ledger entries.
    """

    async def get_or_create_wallet_total(
        self, session: AsyncSession, wallet: str, for_update: bool = True
    ) -> WalletTotal:
        """Fetch or initialize the wallet_totals row with an exclusive lock."""
        stmt = select(WalletTotal).where(WalletTotal.wallet == wallet)
        if for_update:
            stmt = stmt.with_for_update()

        result = await session.execute(stmt)
        record = result.scalar_one_or_none()
        if record is None:
            record = WalletTotal(
                wallet=wallet,
                total_realized_pnl=Decimal("0"),
                total_trade_count=0,
                total_redeem_count=0,
                unsupported_event_count=0,
                reject_count=0,
                last_applied_api_order=0,
                last_recompute_at=None,
                recompute_count=0,
                recompute_needed=False,
                updated_at=datetime.now(timezone.utc),
            )
            session.add(record)
            await session.flush()
        return record

    async def get_or_create_cursor(self, session: AsyncSession, wallet: str) -> ProjectorCursor:
        """Fetch or initialize the projector cursor."""
        stmt = select(ProjectorCursor).where(ProjectorCursor.wallet == wallet)
        result = await session.execute(stmt)
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
            return await self.recompute_wallet(session, wallet)
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
            return ProjectorRunSummary(
                wallet=wallet,
                events_processed=0,
                trades_applied=0,
                redeems_applied=0,
                unsupported_count=0,
                rejects_count=0,
                last_applied_api_order=start_order,
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
        await session.execute(delete(PositionLedger).where(PositionLedger.wallet == wallet))
        await session.execute(
            delete(PositionLedgerReject).where(PositionLedgerReject.wallet == wallet)
        )

        # Reset cumulative totals
        wallet_total.total_realized_pnl = Decimal("0")
        wallet_total.total_trade_count = 0
        wallet_total.total_redeem_count = 0
        wallet_total.unsupported_event_count = 0
        wallet_total.reject_count = 0
        wallet_total.recompute_count += 1
        wallet_total.recompute_needed = False
        wallet_total.last_recompute_at = datetime.now(timezone.utc)
        wallet_total.last_applied_api_order = 0
        cursor.last_applied_api_order = 0
        await session.flush()

        # 3. Fetch all source_events from beginning
        stmt = (
            select(SourceEvent)
            .where(SourceEvent.wallet == wallet, SourceEvent.superseded_by.is_(None))
            .order_by(SourceEvent.event_time.asc(), SourceEvent.api_order.asc())
        )
        events_res = await session.execute(stmt)
        source_events = events_res.scalars().all()

        if not source_events:
            return ProjectorRunSummary(
                wallet=wallet,
                events_processed=0,
                trades_applied=0,
                redeems_applied=0,
                unsupported_count=0,
                rejects_count=0,
                last_applied_api_order=0,
                is_recompute=True,
            )

        return await self._apply_event_batch(
            session=session,
            wallet=wallet,
            wallet_total=wallet_total,
            cursor=cursor,
            source_events=source_events,
            is_recompute=True,
        )

    async def _apply_event_batch(
        self,
        session: AsyncSession,
        wallet: str,
        wallet_total: WalletTotal,
        cursor: ProjectorCursor,
        source_events: list[SourceEvent],
        is_recompute: bool,
    ) -> ProjectorRunSummary:
        """Internal helper to apply a batch of events with state caching."""
        # Pre-load existing positions for this wallet into memory cache
        pos_stmt = select(Position).where(Position.wallet == wallet)
        pos_res = await session.execute(pos_stmt)
        active_positions: dict[tuple[str, str], PositionState] = {}
        for p in pos_res.scalars().all():
            key = (p.condition_id, p.token_id)
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

        last_order = cursor.last_applied_api_order

        for se in source_events:
            event_type = se.event_type.upper()
            last_order = max(last_order, se.api_order)

            if event_type not in ("TRADE", "REDEEM"):
                unsupported_count += 1
                wallet_total.unsupported_event_count += 1
                continue

            # Deterministic token_id resolution per Decision Note 003
            resolved_token_id = se.token_id
            resolved_condition_id = se.condition_id or ""

            if not resolved_token_id and event_type == "REDEEM":
                # Lookup open position for (wallet, condition_id, outcome_index)
                matching_keys = [
                    (cid, tid)
                    for (cid, tid), st in active_positions.items()
                    if cid == resolved_condition_id
                    and (se.outcome_index is None or st.outcome_index == se.outcome_index)
                    and st.shares > Decimal("0")
                ]
                if len(matching_keys) == 1:
                    resolved_token_id = matching_keys[0][1]
                elif len(matching_keys) > 1:
                    # Ambiguous
                    rejects_count += 1
                    reject_rows_to_insert.append(
                        PositionLedgerReject(
                            wallet=wallet,
                            source_event_id=se.source_event_id,
                            api_order=se.api_order,
                            event_type=se.event_type,
                            event_time=se.event_time,
                            condition_id=se.condition_id,
                            token_id=se.token_id,
                            size=se.size,
                            usdc_size=se.usdc_size,
                            reject_reason="AMBIGUOUS_REDEEM_TOKEN",
                            created_at=now_ts,
                        )
                    )
                    continue
                else:
                    rejects_count += 1
                    reject_rows_to_insert.append(
                        PositionLedgerReject(
                            wallet=wallet,
                            source_event_id=se.source_event_id,
                            api_order=se.api_order,
                            event_type=se.event_type,
                            event_time=se.event_time,
                            condition_id=se.condition_id,
                            token_id=se.token_id,
                            size=se.size,
                            usdc_size=se.usdc_size,
                            reject_reason="UNMATCHED_REDEEM_TOKEN",
                            created_at=now_ts,
                        )
                    )
                    continue

            pos_key = (resolved_condition_id, resolved_token_id)
            current_state = active_positions.get(pos_key)

            acct_event = AccountingEvent(
                source_event_id=se.source_event_id,
                api_order=se.api_order,
                event_type=se.event_type,
                event_time=se.event_time,
                wallet=wallet,
                condition_id=resolved_condition_id,
                token_id=resolved_token_id,
                outcome_index=se.outcome_index,
                side=se.side,
                size=se.size,
                usdc_size=se.usdc_size,
                price_reported=se.price_reported,
                cash_price_gap=se.cash_price_gap,
                raw=se.raw,
            )

            result = apply_event(current_state, acct_event)

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

                prior_realized_pnl = current_state.realized_pnl if current_state else Decimal("0")
                new_token_realized_pnl = prior_realized_pnl + suc.realized_pnl_delta

                out_idx = se.outcome_index
                if out_idx is None and current_state:
                    out_idx = current_state.outcome_index

                # Update in-memory state
                active_positions[pos_key] = PositionState(
                    wallet=wallet,
                    condition_id=resolved_condition_id,
                    token_id=resolved_token_id,
                    outcome_index=out_idx,
                    shares=suc.shares_after,
                    avg_cost_basis=suc.avg_cost_after,
                    realized_pnl=new_token_realized_pnl,
                    last_applied_api_order=se.api_order,
                    updated_at=now_ts,
                )

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
            pos_stmt = select(Position).where(
                Position.wallet == wallet,
                Position.condition_id == cid,
                Position.token_id == tid,
            )
            existing_pos_res = await session.execute(pos_stmt)
            existing_pos = existing_pos_res.scalar_one_or_none()

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
        wallet_total.updated_at = now_ts
        cursor.last_applied_api_order = last_order
        cursor.updated_at = now_ts

        await session.flush()

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
