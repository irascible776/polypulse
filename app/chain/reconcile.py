"""Wallet-level comparison against Polygon at a fixed block."""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select

from app.chain.decoder import decode_log
from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from app.storage.database import validate_wallet_shard, wallet_session_factory
from app.storage.models import ChainEvent, IngestionCursor, Position, PositionLedger, SourceEvent

logger = logging.getLogger("reconcile_chain")


async def cache_receipt(client: PolygonRpcClient, wallet: str, tx: str) -> int:
    """Fetch receipt block and persist only decoded CTF logs involving the wallet."""
    from scripts.pull_chain_slice import save_chain_events

    receipt = await client.call("eth_getTransactionReceipt", [tx])
    if not receipt or not receipt.get("blockNumber"):
        raise ValueError("transaction receipt unavailable")
    block_number = int(receipt["blockNumber"], 16)
    block = await client.call("eth_getBlockByNumber", [receipt["blockNumber"], False])
    if not block or not block.get("timestamp"):
        raise ValueError("receipt block timestamp unavailable")
    events = []
    for log in receipt.get("logs", []):
        if log.get("address", "").lower() == CTF_CONTRACT.lower():
            events.extend(decode_log(dict(log, blockTimestamp=block["timestamp"]), wallet))
    await save_chain_events(events)
    return block_number


WALLETS = [
    ("AppleTiiime", "0x951634357346bb291df117f9ac410100feea2104"),
    ("JnStPrdctns", "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"),
]

# Addresses from https://docs.polymarket.com/resources/contracts.
POLYMARKET_CONTRACTS = {
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",
    "0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
    "0xada100db00ca00073811820692005400218fce1f",
    "0xada2005600dec949baf300f4c6120000bdb6eaab",
    "0xa1200000d0002264c9a1698e001292d00e1b00af",
}
ZERO_ADDRESS = "0x" + "0" * 40


def classify_delta(
    delta: Decimal,
    post_poll_net: Decimal,
    unknown_block: bool,
    post_reference: Decimal = Decimal(0),
) -> str:
    if unknown_block:
        return "UNKNOWN"
    if delta == 0:
        return "MATCH"
    if (post_poll_net != 0 and delta == -post_poll_net) or (
        post_reference != 0 and delta == -post_reference
    ):
        return "IN_FLIGHT"
    return "MISMATCH"


def ledger_at_reference(entries, reference_time):
    current, later = {}, {}
    for entry in entries:
        if entry.event_time <= reference_time:
            bucket = current
        elif entry.event_time <= reference_time + timedelta(seconds=120):
            bucket = later
        else:
            continue
        bucket[entry.token_id] = bucket.get(entry.token_id, Decimal(0)) + entry.shares_delta
    return current, later


def print_ingestion_freshness(last_success: datetime | None, reference_time: datetime) -> None:
    print(f"Data API last successful poll: {last_success}; reference block time: {reference_time}")
    if last_success is not None and (reference_time - last_success).total_seconds() > 900:
        print(
            "WARNING: data_api last successful poll is more than 15 minutes behind "
            "the reference block"
        )


async def reconcile_wallet(
    client: PolygonRpcClient,
    label: str,
    wallet: str,
    reference_block: int,
    reference_time: datetime,
    session_factory=None,
) -> dict:
    if session_factory is None:
        session_factory = wallet_session_factory(wallet)
    async with session_factory() as session:
        await validate_wallet_shard(session, wallet)
        positions = (
            (await session.execute(select(Position).where(Position.wallet == wallet)))
            .scalars()
            .all()
        )
        chain = (
            (await session.execute(select(ChainEvent).where(ChainEvent.wallet == wallet)))
            .scalars()
            .all()
        )
        stmt = (
            select(PositionLedger, SourceEvent.transaction_hash)
            .join(
                SourceEvent,
                (SourceEvent.wallet == PositionLedger.wallet)
                & (SourceEvent.source_event_id == PositionLedger.source_event_id),
            )
            .where(PositionLedger.wallet == wallet)
        )
        ledger = (await session.execute(stmt)).all()
        cutoff = await session.scalar(
            select(func.max(SourceEvent.event_time)).where(SourceEvent.wallet == wallet)
        )
        last_success = await session.scalar(
            select(IngestionCursor.last_success_at).where(
                IngestionCursor.wallet == wallet, IngestionCursor.source == "data_api"
            )
        )

    results = []
    perfect_matches = 0
    query_failures = 0
    positions_by_token = {p.token_id: p for p in positions}
    tokens = set(positions_by_token) | {e.token_id for e in chain if e.token_id}
    ledger_by_token, in_flight = ledger_at_reference([entry for entry, _ in ledger], reference_time)
    unknown = {}
    net = {}
    post_poll = {}
    for event in chain:
        if event.block_number > reference_block or not event.token_id:
            continue
        change = (event.amount if event.to_address == wallet else Decimal("0")) - (
            event.amount if event.from_address == wallet else Decimal("0")
        )
        net[event.token_id] = net.get(event.token_id, Decimal("0")) + change
        if last_success is not None and event.block_time > last_success:
            post_poll[event.token_id] = post_poll.get(event.token_id, Decimal("0")) + change
    total_checked = len(tokens)

    print("\n=======================================================")
    print(f"RECONCILING ON-CHAIN VS OFF-CHAIN: {label} ({wallet})")
    print("=======================================================")
    print(f"Checking {total_checked} known tokens at reference block {reference_block}")
    print(f"Source events cutoff: {cutoff}; reference block time: {reference_time}")
    print_ingestion_freshness(last_success, reference_time)

    ordered = sorted(tokens)
    balances = {}
    try:
        for start in range(0, len(ordered), 100):
            batch = ordered[start : start + 100]
            try:
                values = await client.get_balances_of_batch(
                    CTF_CONTRACT, wallet, batch, reference_block
                )
                balances.update(zip(batch, values, strict=True))
            except Exception as exc:
                if "historical state" in str(exc).lower():
                    raise
                logger.error("balanceOfBatch failed: %s", exc)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print(
            f"INTERRUPTED {label}: MATCH=0 MISMATCH=0 UNKNOWN={len(ordered)} "
            f"({len(balances)} balances retrieved but not classified)",
            flush=True,
        )
        return {
            "wallet": wallet,
            "label": label,
            "total_checked": len(ordered),
            "perfect_matches": 0,
            "query_failures": len(ordered),
            "mismatches": 0,
            "in_flight_count": 0,
            "results": [],
        }

    mismatches = 0
    in_flight_count = 0
    for token_id in ordered:
        try:
            if token_id not in balances:
                raise RuntimeError("balance not available")
            on_chain_shares = balances[token_id]
            off_chain = ledger_by_token.get(token_id, Decimal("0"))
            delta = off_chain - on_chain_shares
            status = classify_delta(
                delta,
                post_poll.get(token_id, Decimal("0")),
                token_id in unknown,
                in_flight.get(token_id, Decimal("0")),
            )
            is_match = status == "MATCH"
            if status == "MATCH":
                perfect_matches += 1
            elif status == "UNKNOWN":
                query_failures += 1
            elif status == "IN_FLIGHT":
                in_flight_count += 1
            else:
                mismatches += 1

            results.append(
                {
                    "token_id": token_id,
                    "off_chain_shares": off_chain,
                    "on_chain_shares": on_chain_shares,
                    "delta": delta,
                    "is_match": is_match,
                    "status": status,
                }
            )

            if not is_match or (
                token_id not in positions_by_token
                and (on_chain_shares != 0 or net.get(token_id, Decimal("0")) != 0)
            ):
                explanation = (
                    "post-poll (expected until ingestion catches up)"
                    if status == "IN_FLIGHT"
                    else "unexplained by post-cutoff net"
                )
                print(
                    f"Token {token_id}: status={status} "
                    f"off_chain={off_chain} on_chain={on_chain_shares} "
                    f"delta={delta} chain_net={net.get(token_id, Decimal('0'))} "
                    f"in_flight={in_flight.get(token_id, Decimal('0'))} "
                    f"unknown_block={unknown.get(token_id, Decimal('0'))} "
                    f"position_exists={token_id in positions_by_token} explanation={explanation}"
                )
                for event in chain:
                    if event.token_id != token_id or event.block_number > reference_block:
                        continue
                    counterparty = (
                        event.from_address if event.to_address == wallet else event.to_address
                    )
                    if counterparty == ZERO_ADDRESS:
                        cause = (
                            "mint_from_zero"
                            if event.from_address == ZERO_ADDRESS
                            else "burn_to_zero"
                        )
                    elif counterparty in POLYMARKET_CONTRACTS:
                        cause = "polymarket_contract"
                    elif counterparty:
                        cause = "other_address_unverified"
                    else:
                        cause = "unexplained"
                    print(
                        f"  EVENT token={token_id} tx={event.transaction_hash} "
                        f"log={event.log_index} batch={event.batch_index} "
                        f"event={event.event_name} from={event.from_address} to={event.to_address} "
                        f"amount={event.amount} cause={cause}"
                    )
        except Exception as e:
            query_failures += 1
            results.append({"token_id": token_id, "status": "UNKNOWN", "error": str(e)})
            logger.error("Failed to query on-chain balance for %s: %s", token_id, e)

    print(
        f"Summary for {label}: MATCH={perfect_matches} IN_FLIGHT={in_flight_count} "
        f"MISMATCH={mismatches} "
        f"UNKNOWN={query_failures} total={total_checked}. "
        "Receipt-only coverage is incomplete until the wallet-topic scan."
    )

    return {
        "wallet": wallet,
        "label": label,
        "total_checked": total_checked,
        "perfect_matches": perfect_matches,
        "query_failures": query_failures,
        "mismatches": mismatches,
        "in_flight_count": in_flight_count,
        "results": results,
    }
