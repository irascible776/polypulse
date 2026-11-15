import asyncio
from collections import defaultdict

from sqlalchemy import func, select, text

from app.ingestion.fixture_replay import FixtureReplaySource
from app.ingestion.identifiers import EventIdAssigner
from app.ingestion.writer import write_event_batch
from app.storage.database import async_session
from app.storage.models import SourceEvent
from scripts.guard_test_database import main as guard_database

WALLETS = [
    ("AppleTiiime", "0x951634357346bb291df117f9ac410100feea2104"),
    ("JnStPrdctns", "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"),
]


async def query_db_counts() -> dict[str, dict[str, int]]:
    async with async_session() as session:
        stmt = (
            select(SourceEvent.wallet, SourceEvent.event_type, func.count(SourceEvent.id))
            .where(SourceEvent.source == "fixture_replay")
            .group_by(SourceEvent.wallet, SourceEvent.event_type)
        )
        res = await session.execute(stmt)
        results = defaultdict(lambda: defaultdict(int))
        for wallet, ev_type, count in res.all():
            results[wallet][ev_type] = count
        return results


async def replay_pass(pass_number: int) -> dict[str, tuple[int, int]]:
    source = FixtureReplaySource(fixtures_dir="fixtures")
    pass_stats = {}

    for label, wallet in WALLETS:
        pages = source._load_pages_for_wallet(wallet)
        assigner = EventIdAssigner(wallet=wallet)
        total_inserted = 0
        total_duplicates = 0

        for i, page in enumerate(pages):
            events = page.get("data", []) if isinstance(page, dict) else page
            is_final = i == len(pages) - 1
            assigned = assigner.assign_ids(events, is_final=is_final)

            async with async_session() as session:
                inserted, dupes = await write_event_batch(
                    session=session,
                    wallet=wallet,
                    source="fixture_replay",
                    events_with_ids=assigned,
                    is_backfill_complete=is_final,
                )
                await session.commit()
                total_inserted += inserted
                total_duplicates += dupes

        pass_stats[label] = (total_inserted, total_duplicates)
        print(
            f"Pass {pass_number} - {label}: "
            f"Inserted={total_inserted}, Duplicates={total_duplicates}"
        )

    return pass_stats


async def main():
    if guard_database():
        raise SystemExit(1)
    # 0. Clean slate for fixture_replay source
    async with async_session() as session:
        await session.execute(text("DELETE FROM source_events WHERE source = 'fixture_replay'"))
        await session.execute(text("DELETE FROM ingestion_cursors WHERE source = 'fixture_replay'"))
        await session.commit()
    print("Database reset for source='fixture_replay'.")

    # 1. Pass 1
    print("\n=== STARTING PASS 1 ===")
    await replay_pass(1)
    counts_pass1 = await query_db_counts()
    print("\n--- DATABASE ROW COUNTS AFTER PASS 1 ---")
    for label, wallet in WALLETS:
        w_counts = counts_pass1[wallet.lower()]
        total = sum(w_counts.values())
        print(f"\nWallet: {label} ({wallet}) -> Total: {total}")
        for ev_type, cnt in sorted(w_counts.items()):
            print(f"  {ev_type}: {cnt}")

    # 2. Pass 2
    print("\n=== STARTING PASS 2 (IDEMPOTENCY REPLAY) ===")
    await replay_pass(2)
    counts_pass2 = await query_db_counts()
    print("\n--- DATABASE ROW COUNTS AFTER PASS 2 ---")
    for label, wallet in WALLETS:
        w_counts = counts_pass2[wallet.lower()]
        total = sum(w_counts.values())
        print(f"\nWallet: {label} ({wallet}) -> Total: {total}")
        for ev_type, cnt in sorted(w_counts.items()):
            print(f"  {ev_type}: {cnt}")

    # 3. Verification assertions
    print("\n=== VERIFYING EXPECTATIONS ===")
    # AppleTiiime
    w1_c1 = counts_pass1[WALLETS[0][1].lower()]
    w1_c2 = counts_pass2[WALLETS[0][1].lower()]
    assert sum(w1_c1.values()) == 2454, f"Pass 1 AppleTiiime total {sum(w1_c1.values())} != 2454"
    assert sum(w1_c2.values()) == 2454, f"Pass 2 AppleTiiime total {sum(w1_c2.values())} != 2454"
    assert w1_c1 == w1_c2, "AppleTiiime counts changed between pass 1 and pass 2!"
    assert w1_c2["TRADE"] == 2261
    assert w1_c2["REDEEM"] == 158
    assert w1_c2["MAKER_REBATE"] == 15
    assert w1_c2["TAKER_REBATE"] == 11
    assert w1_c2["REFERRAL_REWARD"] == 9

    # JnStPrdctns
    w2_c1 = counts_pass1[WALLETS[1][1].lower()]
    w2_c2 = counts_pass2[WALLETS[1][1].lower()]
    assert sum(w2_c1.values()) == 835, f"Pass 1 JnStPrdctns total {sum(w2_c1.values())} != 835"
    assert sum(w2_c2.values()) == 835, f"Pass 2 JnStPrdctns total {sum(w2_c2.values())} != 835"
    assert w2_c1 == w2_c2, "JnStPrdctns counts changed between pass 1 and pass 2!"
    assert w2_c2["YIELD"] == 44

    print(
        "ALL EXPECTATIONS VERIFIED! Row counts per type per wallet are exact and 100% idempotent."
    )


if __name__ == "__main__":
    asyncio.run(main())
