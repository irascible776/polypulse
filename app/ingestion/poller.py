import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.ingestion.identifiers import EventIdAssigner
from app.ingestion.source import EventSource
from app.ingestion.writer import get_or_create_cursor_locked, write_event_batch

logger = logging.getLogger("ingestion_poller")


def record_committed_supersedes(events: list[dict]) -> None:
    """Append live supersedes only after their ingestion transaction commits."""
    if not events:
        return
    try:
        path = Path("reports/supersede_events.log")
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            for event in events:
                payload = {"timestamp": datetime.now(timezone.utc).isoformat(), **event}
                os.write(fd, (json.dumps(payload, separators=(",", ":")) + "\n").encode())
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        logger.exception("Unable to record committed supersede events")


@dataclass
class DetectionLagMetrics:
    """Tracks detection lag (observed_at - event_time) for newly inserted events."""

    lags_seconds: list[float] = field(default_factory=list)
    total_new_events: int = 0
    total_late_events: int = 0
    reconcile_sweeps_count: int = 0
    reconcile_new_events_count: int = 0
    last_api_status_age_seconds: float | None = None
    last_api_status_blocks_behind: int | None = None

    def record_event_lag(self, lag_seconds: float, is_late: bool = False) -> None:
        self.total_new_events += 1
        if is_late:
            self.total_late_events += 1
        self.lags_seconds.append(lag_seconds)
        if len(self.lags_seconds) > 1000:
            self.lags_seconds.pop(0)

    @property
    def lag_p50(self) -> float:
        if not self.lags_seconds:
            return 0.0
        s = sorted(self.lags_seconds)
        return s[len(s) // 2]

    @property
    def lag_p95(self) -> float:
        if not self.lags_seconds:
            return 0.0
        s = sorted(self.lags_seconds)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]

    @property
    def lag_p99(self) -> float:
        if not self.lags_seconds:
            return 0.0
        s = sorted(self.lags_seconds)
        idx = int(len(s) * 0.99)
        return s[min(idx, len(s) - 1)]


class LivePoller:
    """
    Continuous live poller for tracked wallets.
    Waits for backfill_state = complete before commencing.
    Features:
    - Adaptive polling interval (default 5s, backs off up to 30s, speeds up on activity)
    - 60-second overlap window to prevent silent event loss at block boundaries
    - Detection lag tracking (observed_at - event_time) over the last 1,000 new events
    - Background status polling of /v2/status (age_seconds and blocks behind)
    - Reconcile sweep every 10-15 minutes re-walking the last hour
    """

    def __init__(
        self,
        wallet: str,
        source: EventSource,
        session_factory: async_sessionmaker[AsyncSession],
        base_poll_interval: float = 5.0,
        min_poll_interval: float = 2.0,
        max_poll_interval: float = 30.0,
        overlap_seconds: int = 60,
        reconcile_interval_seconds: float = 720.0,  # 12 minutes
    ) -> None:
        self.wallet = wallet.lower()
        self.source = source
        self.session_factory = session_factory
        self.base_poll_interval = base_poll_interval
        self.current_poll_interval = base_poll_interval
        self.min_poll_interval = min_poll_interval
        self.max_poll_interval = max_poll_interval
        self.overlap_seconds = overlap_seconds
        self.reconcile_interval_seconds = reconcile_interval_seconds
        self.metrics = DetectionLagMetrics()
        self.poll_count = 0
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        logger.info("Live poller initialized for wallet %s", self.wallet)

        # 1. Wait until backfill is complete
        while not self._stop_event.is_set():
            async with self.session_factory() as session:
                cursor = await get_or_create_cursor_locked(
                    session, self.wallet, self.source.source_tag
                )
                is_complete = cursor.backfill_state == "complete"
                await session.commit()

            if is_complete:
                logger.info(
                    "Backfill complete detected for %s. Commencing live polling.", self.wallet
                )
                break
            await asyncio.sleep(1.0)

        last_reconcile_time = time.time()
        last_status_time = 0.0

        # 2. Polling loop
        while not self._stop_event.is_set():
            now = time.time()

            # Optional /v2/status polling every 60 seconds
            if now - last_status_time >= 60.0:
                last_status_time = now
                status_meta = await self.source.fetch_status()
                if status_meta and "data" in status_meta:
                    d = status_meta["data"]
                    self.metrics.last_api_status_age_seconds = float(d.get("age_seconds", 0))

            # Periodic reconcile sweep every 10-15 minutes
            if now - last_reconcile_time >= self.reconcile_interval_seconds:
                last_reconcile_time = now
                await self._execute_reconcile_sweep()

            # Execute normal live poll cycle
            inserted_count = await self._execute_poll_cycle()

            # Adapt poll interval
            if inserted_count > 0:
                self.current_poll_interval = self.min_poll_interval
            else:
                self.current_poll_interval = min(
                    self.max_poll_interval, self.current_poll_interval * 1.3
                )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.current_poll_interval)
            except asyncio.TimeoutError:
                pass

    async def _execute_poll_cycle(self, update_last_success: bool = True) -> int:
        """Query events with 60s overlap and persist."""
        self.poll_count += 1
        async with self.session_factory() as session:
            cursor_rec = await get_or_create_cursor_locked(
                session, self.wallet, self.source.source_tag
            )
            last_ev_time = cursor_rec.last_event_time
            await session.commit()

        if last_ev_time is not None:
            start_ts = max(1, int(last_ev_time.timestamp()) - self.overlap_seconds)
        else:
            start_ts = 1

        assigner = EventIdAssigner(wallet=self.wallet)
        cursor = None
        start = start_ts
        cycle_inserted = 0

        while not self._stop_event.is_set():
            try:
                page_res = await self.source.fetch_page(
                    wallet=self.wallet,
                    cursor=cursor,
                    start=start,
                    limit=500,
                    sort_direction="ASC",
                )
            except Exception as err:
                logger.error(
                    "Live poll failed for wallet %s: %s: %s", self.wallet, type(err).__name__, err
                )
                break

            events = page_res.events
            has_more = page_res.has_more and bool(events)
            is_final = not has_more

            assigned = assigner.assign_ids(events, is_final=is_final)
            observed_now = datetime.now(timezone.utc)
            supersede_events: list[dict] = []

            async with self.session_factory() as session:
                async with session.begin():
                    ins, _ = await write_event_batch(
                        session=session,
                        wallet=self.wallet,
                        source=self.source.source_tag,
                        events_with_ids=assigned,
                        next_cursor=page_res.next_cursor,
                        is_backfill_complete=True,
                        now=observed_now,
                        on_new_event=self.metrics.record_event_lag,
                        update_last_success=update_last_success,
                        monitor_events=supersede_events
                        if self.source.source_tag == "data_api"
                        else None,
                    )
                    cycle_inserted += ins

            record_committed_supersedes(supersede_events)

            if is_final:
                break

            cursor = page_res.next_cursor
            start = None

        return cycle_inserted

    async def _execute_reconcile_sweep(self) -> None:
        """Re-walk the last 1 hour of events to detect any missed events (expected 0)."""
        self.metrics.reconcile_sweeps_count += 1
        now_ts = int(time.time())
        one_hour_ago = max(1, now_ts - 3600)
        logger.info("Executing reconcile sweep for wallet %s from ts %d", self.wallet, one_hour_ago)

        assigner = EventIdAssigner(wallet=self.wallet)
        cursor = None
        start = one_hour_ago
        reconcile_new = 0

        while not self._stop_event.is_set():
            try:
                page_res = await self.source.fetch_page(
                    wallet=self.wallet,
                    cursor=cursor,
                    start=start,
                    limit=500,
                    sort_direction="ASC",
                )
            except Exception as err:
                logger.error(
                    "Reconcile sweep failed for %s: %s: %s", self.wallet, type(err).__name__, err
                )
                break

            events = page_res.events
            has_more = page_res.has_more and bool(events)
            is_final = not has_more

            assigned = assigner.assign_ids(events, is_final=is_final)
            supersede_events = []
            async with self.session_factory() as session:
                async with session.begin():
                    ins, _ = await write_event_batch(
                        session=session,
                        wallet=self.wallet,
                        source=self.source.source_tag,
                        events_with_ids=assigned,
                        next_cursor=None,
                        is_backfill_complete=True,
                        monitor_events=supersede_events
                        if self.source.source_tag == "data_api"
                        else None,
                    )
                    reconcile_new += ins

            record_committed_supersedes(supersede_events)

            if is_final:
                break
            cursor = page_res.next_cursor
            start = None

        self.metrics.reconcile_new_events_count += reconcile_new
        logger.info(
            "Reconcile sweep completed for %s. New events found: %d (expected 0)",
            self.wallet,
            reconcile_new,
        )
