"""Incrementally scan wallet transfer logs from the last completed ranges."""

import json
import tempfile
from pathlib import Path

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.chain.decoder import TRANSFER_BATCH_TOPIC, TRANSFER_SINGLE_TOPIC, decode_log
from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from app.storage.database import validate_wallet_shard, wallet_engine, wallet_session_factory
from app.storage.models import ChainScanCursor, SourceEvent
from scripts.pull_chain_slice import save_chain_events
from scripts.scan_chain_slice import CHECKPOINT, Scanner, first_block


async def scan_tail(wallet: str, end: int, session_factory=None, db_engine=None) -> dict:
    if session_factory is None:
        session_factory = wallet_session_factory(wallet)
    if db_engine is None:
        db_engine = wallet_engine(wallet)
    database = db_engine.url.database
    if not database or not database.replace("_", "").isalnum():
        raise ValueError("database name cannot be used for a scan checkpoint")
    checkpoint = CHECKPOINT.with_name(f"tail_checkpoint_{database}.json")
    state = json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    base = json.loads(CHECKPOINT.read_text()) if CHECKPOINT.exists() else {}
    jn_path = CHECKPOINT.with_name("jn_tail_checkpoint.json")
    jn = json.loads(jn_path.read_text()) if jn_path.exists() else {}
    jn_wallet = "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"
    client = PolygonRpcClient()
    inserted = 0
    initial_block = None
    with tempfile.TemporaryDirectory(prefix="polypulse-chain-scan-") as tempdir:
        scanner = Scanner(checkpoint=Path(tempdir) / "scan.json")
        async with httpx.AsyncClient(timeout=40, headers={"User-Agent": "PolyPulse/1.0"}) as http:
            for direction in ("from", "to"):
                key = f"{wallet}:{direction}"
                async with session_factory() as session:
                    await validate_wallet_shard(session, wallet)
                    cursor = await session.get(ChainScanCursor, (wallet, direction))
                    previous = cursor.last_block if cursor else state.get(key)
                if previous is None:
                    previous = base.get(key)
                    if previous is None and wallet == jn_wallet:
                        previous = jn.get(direction)
                    if previous is None:
                        if initial_block is None:
                            async with session_factory() as session:
                                earliest = await session.scalar(
                                    select(func.min(SourceEvent.event_time)).where(
                                        SourceEvent.wallet == wallet
                                    )
                                )
                            if earliest is None:
                                raise ValueError(f"no source events or scan checkpoint for {key}")
                            initial_block = await first_block(
                                client, int(earliest.timestamp()), end
                            )
                        previous = initial_block - 1
                async with session_factory() as session:
                    async with session.begin():
                        stmt = insert(ChainScanCursor).values(
                            wallet=wallet, direction=direction, last_block=previous
                        )
                        previous = await session.scalar(
                            stmt.on_conflict_do_update(
                                index_elements=["wallet", "direction"],
                                set_={
                                    "last_block": func.greatest(
                                        ChainScanCursor.last_block, previous
                                    )
                                },
                            ).returning(ChainScanCursor.last_block)
                        )
                start = previous + 1
                topic = "0x" + wallet[2:].rjust(64, "0")
                while start <= end:
                    last = min(start + 9999, end)
                    logs = await scanner.get_logs(
                        http,
                        {
                            "fromBlock": hex(start),
                            "toBlock": hex(last),
                            "address": CTF_CONTRACT,
                            "topics": [
                                [TRANSFER_SINGLE_TOPIC, TRANSFER_BATCH_TOPIC],
                                None,
                                topic if direction == "from" else None,
                                topic if direction == "to" else None,
                            ],
                        },
                    )
                    await scanner.prefetch_timestamps(http, logs)
                    events = []
                    for log in logs:
                        timestamp = await scanner.timestamp(client, int(log["blockNumber"], 16))
                        events.extend(decode_log(dict(log, blockTimestamp=timestamp), wallet))
                    async with session_factory() as session:
                        async with session.begin():
                            inserted += await save_chain_events(events, session)
                            stmt = insert(ChainScanCursor).values(
                                wallet=wallet, direction=direction, last_block=last
                            )
                            await session.execute(
                                stmt.on_conflict_do_update(
                                    index_elements=["wallet", "direction"],
                                    set_={
                                        "last_block": func.greatest(
                                            ChainScanCursor.last_block, last
                                        )
                                    },
                                )
                            )
                    start = last + 1
    return {"calls": scanner.calls, "errors": scanner.errors, "inserted": inserted}
