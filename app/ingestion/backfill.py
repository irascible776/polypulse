import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.ingestion.data_api import InvalidCursorError
from app.ingestion.identifiers import EventIdAssigner
from app.ingestion.source import EventSource
from app.ingestion.writer import get_or_create_cursor_locked, write_event_batch

logger = logging.getLogger("ingestion_backfill")


async def run_wallet_backfill(
    wallet: str,
    source: EventSource,
    session_factory: async_sessionmaker[AsyncSession],
    max_pages: int = 1000,
) -> dict[str, Any]:
    """
    Execute full historical backfill for a single wallet using an EventSource.
    Follows: start=1, ascending, 500 per page.
    Saves each page's events in one transaction with INSERT ... ON CONFLICT DO NOTHING.
    Updates ingestion_cursors in the SAME transaction.
    Resumes seamlessly after interruptions using saved cursor or last_event_time - 1s fallback.
    Marks backfill_state = complete on final page.
    """
    w = wallet.lower()
    source_tag = source.source_tag
    logger.info("Starting backfill for wallet %s from source %s", w, source_tag)

    # 1. Inspect existing cursor state
    async with session_factory() as session:
        cursor_record = await get_or_create_cursor_locked(session, w, source_tag)
        if cursor_record.backfill_state == "complete":
            logger.info("Wallet %s backfill is already COMPLETE. Skipping.", w)
            return {
                "wallet": w,
                "status": "already_complete",
                "total_inserted": 0,
                "total_duplicates": 0,
                "pages": 0,
            }

        saved_cursor = cursor_record.last_cursor
        last_event_time = cursor_record.last_event_time
        await session.commit()

    assigner = EventIdAssigner(wallet=w)
    cursor = saved_cursor
    start = None

    if not cursor:
        if last_event_time is not None:
            # Fallback restart from last flushed second - 1 (inclusive check confirmed)
            start = max(1, int(last_event_time.timestamp()) - 1)
            logger.info("Resuming backfill for %s from start timestamp %d", w, start)
        else:
            start = 1
            logger.info("Starting initial backfill for %s with start=1", w)
    else:
        logger.info("Resuming backfill for %s using saved cursor: %s...", w, cursor[:20])

    page_count = 0
    total_inserted = 0
    total_duplicates = 0
    seen_cursors: set[str] = set()

    while page_count < max_pages:
        page_count += 1

        # Check for repeated cursor loop
        if cursor:
            if cursor in seen_cursors:
                logger.error("Repeated cursor detected (%s) for wallet %s. Stopping.", cursor, w)
                break
            seen_cursors.add(cursor)

        try:
            page_res = await source.fetch_page(
                wallet=w,
                cursor=cursor,
                start=start,
                limit=500,
                sort_direction="ASC",
            )
        except InvalidCursorError as err:
            logger.warning(
                "Saved cursor rejected for wallet %s (%s). Falling back to start timestamp.",
                w,
                err,
            )
            # Re-read last event time from DB
            async with session_factory() as session:
                c = await get_or_create_cursor_locked(session, w, source_tag)
                let = c.last_event_time
                await session.commit()

            start = max(1, int(let.timestamp()) - 1) if let else 1
            cursor = None
            page_res = await source.fetch_page(
                wallet=w,
                cursor=None,
                start=start,
                limit=500,
                sort_direction="ASC",
            )

        events = page_res.events
        has_more = page_res.has_more and bool(events)
        next_cursor = page_res.next_cursor
        is_final = not has_more

        # Process IDs with page holdback logic
        assigned = assigner.assign_ids(events, is_final=is_final)

        # Write to database in a single atomic transaction
        async with session_factory() as session:
            async with session.begin():
                ins, dups = await write_event_batch(
                    session=session,
                    wallet=w,
                    source=source_tag,
                    events_with_ids=assigned,
                    next_cursor=next_cursor,
                    is_backfill_complete=is_final,
                )
                total_inserted += ins
                total_duplicates += dups

        logger.info(
            "Wallet %s page %d: %d events processed (%d inserted, %d duplicates). has_more=%s",
            w,
            page_count,
            len(assigned),
            ins,
            dups,
            has_more,
        )

        if is_final:
            logger.info(
                "Backfill COMPLETE for %s. Total pages: %d, Total inserted: %d, Duplicates: %d",
                w,
                page_count,
                total_inserted,
                total_duplicates,
            )
            break

        cursor = next_cursor
        start = None  # Cursors supersede start parameter

    return {
        "wallet": w,
        "status": "complete" if is_final else "max_pages_reached",
        "total_inserted": total_inserted,
        "total_duplicates": total_duplicates,
        "pages": page_count,
    }
