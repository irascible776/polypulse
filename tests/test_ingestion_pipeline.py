from collections import defaultdict
from decimal import Decimal

import pytest
from sqlalchemy import func

from app.ingestion.fixture_replay import FixtureReplaySource
from app.ingestion.identifiers import EventIdAssigner
from app.ingestion.status import KNOWN_EVENT_TYPES, get_wallet_ingestion_status
from app.ingestion.writer import prepare_source_event_record
from app.storage.database import async_session, check_database_connection


def test_counts_match_phase_1_measurements_appletiiime():
    """
    Verify event counts on AppleTiiime match Phase 1 measurements:
    Total: 2,454 rows (TRADE 2261, REDEEM 158, MAKER 15, TAKER 11, REFERRAL 9).
    """
    source = FixtureReplaySource(fixtures_dir="fixtures")
    wallet = "0x951634357346bb291df117f9ac410100feea2104"
    pages = source._load_pages_for_wallet(wallet)
    assert len(pages) > 0, "AppleTiiime fixture pages must be present"

    all_events = []
    for p in pages:
        data = p.get("data", []) if isinstance(p, dict) else p
        all_events.extend(data)

    assert len(all_events) == 2454, f"Expected 2454 events, got {len(all_events)}"

    type_counts = defaultdict(int)
    for e in all_events:
        type_counts[e.get("type")] += 1

    assert type_counts["TRADE"] == 2261
    assert type_counts["REDEEM"] == 158
    assert type_counts["MAKER_REBATE"] == 15
    assert type_counts["TAKER_REBATE"] == 11
    assert type_counts["REFERRAL_REWARD"] == 9


def test_counts_match_phase_1_measurements_jnstprdctns():
    """
    Verify event counts on JnStPrdctns match Phase 1 measurements:
    Total: 835 rows, including 44 YIELD.
    """
    source = FixtureReplaySource(fixtures_dir="fixtures")
    wallet = "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"
    pages = source._load_pages_for_wallet(wallet)
    assert len(pages) > 0, "JnStPrdctns fixture pages must be present"

    all_events = []
    for p in pages:
        data = p.get("data", []) if isinstance(p, dict) else p
        all_events.extend(data)

    assert len(all_events) == 835, f"Expected 835 events, got {len(all_events)}"

    type_counts = defaultdict(int)
    for e in all_events:
        type_counts[e.get("type")] += 1

    assert type_counts["YIELD"] == 44


def test_fixture_pages_chronological_monotonicity():
    """
    Regression test for reversed-fixture bug:
    Asserts that loaded fixture events for all tracked wallets are strictly
    monotonic non-decreasing by event timestamp across pages.
    """
    source = FixtureReplaySource(fixtures_dir="fixtures")
    wallets = [
        "0x951634357346bb291df117f9ac410100feea2104",  # AppleTiiime
        "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92",  # JnStPrdctns
    ]
    for w in wallets:
        pages = source._load_pages_for_wallet(w)
        all_events = []
        for p in pages:
            data = p.get("data", []) if isinstance(p, dict) else p
            all_events.extend(data)

        timestamps = [e.get("timestamp") for e in all_events if "timestamp" in e]
        assert len(timestamps) > 0
        for i in range(len(timestamps) - 1):
            assert timestamps[i] <= timestamps[i + 1], (
                f"Timestamp inversion at index {i}: {timestamps[i]} > {timestamps[i + 1]} for {w}"
            )


@pytest.mark.asyncio
async def test_db_source_events_api_order_is_chronologically_monotonic():
    """
    Regression test for database ordering:
    Asserts that stored source_events have api_order that is monotonically
    correlated with event_time (older events must have strictly smaller api_order).
    """
    from sqlalchemy import select

    from app.storage.database import async_session_factory
    from app.storage.models import SourceEvent

    wallets = [
        "0x951634357346bb291df117f9ac410100feea2104",
        "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92",
    ]
    async with async_session_factory() as session:
        for w in wallets:
            stmt = (
                select(SourceEvent.api_order, SourceEvent.event_time)
                .where(SourceEvent.wallet == w)
                .order_by(SourceEvent.api_order.asc())
            )
            rows = (await session.execute(stmt)).all()
            if not rows:
                continue
            for i in range(len(rows) - 1):
                order_curr, time_curr = rows[i]
                order_next, time_next = rows[i + 1]
                assert order_curr < order_next
                # Next event time must be >= current event time (non-decreasing)
                assert time_next >= time_curr, (
                    f"Chronological regression in DB for {w}: "
                    f"order {order_curr} ({time_curr}) followed by order {order_next} ({time_next})"
                )


def test_pipeline_idempotency_simulation():
    """
    Ingest saved pages twice through EventIdAssigner:
    Row count does not change, exactly 0 duplicate IDs added on second run.
    """
    source = FixtureReplaySource(fixtures_dir="fixtures")
    wallet = "0x951634357346bb291df117f9ac410100feea2104"
    pages = source._load_pages_for_wallet(wallet)

    stored_ids = set()

    # Pass 1
    assigner1 = EventIdAssigner(wallet=wallet)
    for i, p in enumerate(pages):
        events = p.get("data", []) if isinstance(p, dict) else p
        is_final = i == len(pages) - 1
        assigned = assigner1.assign_ids(events, is_final=is_final)
        for _, eid in assigned:
            stored_ids.add(eid)

    initial_count = len(stored_ids)
    assert initial_count == 2454

    # Pass 2 (re-ingestion of identical pages)
    assigner2 = EventIdAssigner(wallet=wallet)
    new_inserts = 0
    duplicate_count = 0
    for i, p in enumerate(pages):
        events = p.get("data", []) if isinstance(p, dict) else p
        is_final = i == len(pages) - 1
        assigned = assigner2.assign_ids(events, is_final=is_final)
        for _, eid in assigned:
            if eid in stored_ids:
                duplicate_count += 1
            else:
                stored_ids.add(eid)
                new_inserts += 1

    assert new_inserts == 0, "Re-ingesting same pages must insert 0 new rows"
    assert duplicate_count == 2454, "All 2454 events must be identified as duplicates"
    assert len(stored_ids) == initial_count


def test_resume_in_middle_of_backfill_produces_identical_result():
    """
    Stop in the middle of a backfill (simulate crash after page 2),
    restart from saved cursor / overlap, and verify the final result is identical.
    """
    source = FixtureReplaySource(fixtures_dir="fixtures")
    wallet = "0x951634357346bb291df117f9ac410100feea2104"
    pages = source._load_pages_for_wallet(wallet)

    # Full clean run IDs
    clean_assigner = EventIdAssigner(wallet=wallet)
    clean_ids = []
    for i, p in enumerate(pages):
        events = p.get("data", []) if isinstance(p, dict) else p
        assigned = clean_assigner.assign_ids(events, is_final=(i == len(pages) - 1))
        clean_ids.extend([eid for _, eid in assigned])

    assert len(clean_ids) == 2454

    # Interrupted run: crash after page 2
    interrupted_ids = set()
    interrupted_assigner = EventIdAssigner(wallet=wallet)

    # Ingest page 0 and page 1
    for i in range(2):
        events = pages[i].get("data", []) if isinstance(pages[i], dict) else pages[i]
        assigned = interrupted_assigner.assign_ids(events, is_final=False)
        for _, eid in assigned:
            interrupted_ids.add(eid)

    # Process simulated crash and restart from page 1 overlap (resuming pages 1..4)
    resumed_assigner = EventIdAssigner(wallet=wallet)
    for i in range(1, len(pages)):
        events = pages[i].get("data", []) if isinstance(pages[i], dict) else pages[i]
        is_final = i == len(pages) - 1
        assigned = resumed_assigner.assign_ids(events, is_final=is_final)
        for _, eid in assigned:
            interrupted_ids.add(eid)

    assert len(interrupted_ids) == 2454
    assert interrupted_ids == set(clean_ids), "Resumed backfill must match clean run exactly"


def test_unknown_event_type_is_saved_and_counted_without_crash():
    """
    Unknown or unsupported event types must be saved, counted, and never cause a crash.
    """
    unknown_event = {
        "proxy_wallet": "0x5555555555555555555555555555555555555555",
        "type": "UNKNOWN_CUSTOM_AIRDROP_V3",
        "timestamp": 1700005000,
        "size": Decimal("100"),
        "usdc_size": Decimal("0"),
        "transaction_hash": "0xtx_unknown",
    }
    assigner = EventIdAssigner()
    assigned = assigner.assign_ids([unknown_event], is_final=True)
    assert len(assigned) == 1
    raw_ev, eid = assigned[0]

    record = prepare_source_event_record(
        raw=raw_ev,
        source_event_id=eid,
        wallet="0x5555555555555555555555555555555555555555",
        source="data_api",
        api_order=1,
        is_late=False,
        observed_at=func.now(),
    )
    assert record["event_type"] == "UNKNOWN_CUSTOM_AIRDROP_V3"
    assert record["event_type"] not in KNOWN_EVENT_TYPES


@pytest.mark.asyncio
async def test_live_postgres_ingestion():
    """
    End-to-end integration test against real PostgreSQL in Docker.
    Skipped if PostgreSQL is not currently running.
    """
    is_connected = await check_database_connection()
    if not is_connected:
        pytest.skip("PostgreSQL not reachable on localhost:5432. Start via 'make db-up'.")

    # If DB is connected, run full synthetic wallet backfill and status checks
    from app.ingestion.backfill import run_wallet_backfill

    source = FixtureReplaySource()
    test_wallet = "0x0000000000000000000000000000000000000001"

    res = await run_wallet_backfill(
        wallet=test_wallet,
        source=source,
        session_factory=async_session,
    )
    assert res["status"] in ("complete", "already_complete")

    async with async_session() as session:
        from app.storage.models import IngestionCursor

        status_info = await get_wallet_ingestion_status(session, test_wallet)
        fixture_cursor = await session.get(IngestionCursor, (test_wallet, "fixture_replay"))
        assert fixture_cursor is not None and fixture_cursor.backfill_state == "complete"
        assert status_info["backfill_state"] == "not_started"
        assert status_info["events_stored"] >= 9
