import asyncio
import os
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import DBAPIError

from app.chain import tail
from app.chain.reconcile import reconcile_wallet
from app.ingestion.source import PageResult
from app.main import app
from app.storage.database import (
    async_session_factory,
    engine,
    secondary_engine,
    secondary_session_factory,
)
from app.storage.models import (
    AccountingCheckpoint,
    ChainEvent,
    ChainScanCursor,
    IngestionCursor,
    Position,
    PositionLedger,
    PositionLedgerReject,
    ProjectorCursor,
    SourceEvent,
    TrackedWallet,
    WalletTotal,
)
from app.storage.sharding import wallet_bucket
from app.worker import Worker
from scripts.pull_chain_slice import save_chain_events


@pytest.mark.asyncio
async def test_chain_entry_points_route_to_wallet_shard(tmp_path, monkeypatch):
    if os.environ.get("POLYPULSE_RUN_SHARD_TESTS") != "1" or secondary_engine is None:
        pytest.skip("requires explicitly enabled, initialized scratch shards")
    wallet = "0x" + uuid4().hex + "0" * 8
    while wallet_bucket(wallet) < 8:
        wallet = "0x" + uuid4().hex + "0" * 8
    event = SimpleNamespace(
        wallet=wallet,
        block_number=1,
        block_time=datetime.now(timezone.utc),
        transaction_hash="0x" + uuid4().hex,
        log_index=0,
        batch_index=0,
        raw_log={"blockHash": "0x123"},
        contract_address="0x123",
        event_name="TransferSingle",
        condition_id=None,
        token_id=None,
        from_address=wallet,
        to_address="0x123",
        amount=Decimal("1"),
    )
    checkpoint = tmp_path / "chain_scan_checkpoint.json"
    checkpoint.write_text("{}")
    monkeypatch.setattr(tail, "CHECKPOINT", checkpoint)
    monkeypatch.setattr(tail, "first_block", AsyncMock(return_value=1))
    monkeypatch.setattr(tail.Scanner, "get_logs", AsyncMock(return_value=[]))
    monkeypatch.setattr(tail.Scanner, "prefetch_timestamps", AsyncMock())
    try:
        assert await save_chain_events([event]) == 1
        async with secondary_session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(ChainEvent).where(ChainEvent.wallet == wallet)
                )
                == 1
            )
        async with async_session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(ChainEvent).where(ChainEvent.wallet == wallet)
                )
                == 0
            )
        result = await reconcile_wallet(AsyncMock(), "test", wallet, 1, event.block_time)
        assert result["total_checked"] == 0
        checkpoint.write_text('{"' + wallet + ':from": 1, "' + wallet + ':to": 1}')
        await tail.scan_tail(wallet, 2)
        async with secondary_session_factory() as session:
            assert await session.get(ChainScanCursor, (wallet, "from")) is not None
    finally:
        async with secondary_session_factory() as session:
            for model in (ChainScanCursor, ChainEvent):
                await session.execute(delete(model).where(model.wallet == wallet))
            await session.commit()


@pytest.mark.asyncio
async def test_copied_shard_rejects_wallet_writes_until_activated():
    if os.environ.get("POLYPULSE_RUN_SHARD_TESTS") != "1" or secondary_engine is None:
        pytest.skip("requires explicitly enabled, initialized scratch shards")
    wallet = "0x" + uuid4().hex + "0" * 8
    async with secondary_session_factory() as session:
        await session.execute(
            text("UPDATE wallet_shard_settings SET shard_id = NULL, migration_state = 'copied'")
        )
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                session.add(TrackedWallet(address=wallet))
                await session.flush()
        await session.rollback()


@pytest.mark.asyncio
async def test_workers_and_api_keep_wallets_on_their_shards():
    if os.environ.get("POLYPULSE_RUN_SHARD_TESTS") != "1" or secondary_engine is None:
        pytest.skip("requires explicitly enabled, initialized scratch shards")
    wallets = {}
    while len(wallets) < 2:
        wallet = "0x" + uuid4().hex + "0" * 8
        wallets.setdefault(0 if wallet_bucket(wallet) < 8 else 1, wallet)

    class Source:
        source_tag = "data_api"

        async def fetch_page(self, wallet, **kwargs):
            return PageResult(
                events=[
                    {
                        "proxy_wallet": wallet,
                        "type": "TRADE",
                        "side": "BUY",
                        "timestamp": 1700000000,
                        "size": "1",
                        "usdc_size": "0.5",
                        "price": "0.5",
                        "condition_id": wallet,
                        "token_id": wallet,
                        "transaction_hash": wallet,
                    }
                ],
                next_cursor=None,
                has_more=False,
            )

        async def close(self):
            pass

    factories = (async_session_factory, secondary_session_factory)
    workers = [
        Worker(session_factory=factories[0], db_engine=engine, shard_id=0),
        Worker(
            session_factory=factories[1],
            db_engine=secondary_engine,
            shard_id=1,
            market_sessions=async_session_factory,
        ),
    ]
    try:
        for shard, worker in enumerate(workers):
            await worker.source.close()
            worker.source = Source()
            async with factories[shard]() as session:
                session.add(TrackedWallet(address=wallets[shard]))
                await session.commit()
            worker.active_wallets = lambda shard=shard: asyncio.sleep(0, result=[wallets[shard]])
        await asyncio.gather(*(worker.poll_cycle() for worker in workers))
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for shard, wallet in wallets.items():
                response = await client.get(f"/wallets/{wallet}/positions")
                assert response.status_code == 200
                assert [item["token_id"] for item in response.json()["positions"]] == [wallet]
                async with factories[1 - shard]() as other:
                    assert (
                        await other.scalar(
                            select(func.count())
                            .select_from(SourceEvent)
                            .where(SourceEvent.wallet == wallet)
                        )
                        == 0
                    )
    finally:
        for worker in workers:
            await worker.close()
        for shard, factory in enumerate(factories):
            async with factory() as session:
                for model in (
                    AccountingCheckpoint,
                    PositionLedger,
                    PositionLedgerReject,
                    Position,
                    ProjectorCursor,
                    WalletTotal,
                    IngestionCursor,
                    SourceEvent,
                ):
                    await session.execute(delete(model).where(model.wallet == wallets[shard]))
                await session.execute(
                    delete(TrackedWallet).where(TrackedWallet.address == wallets[shard])
                )
                await session.commit()
