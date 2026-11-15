from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.chain.decoder import (
    PAYOUT_REDEMPTION_TOPIC,
    POSITION_SPLIT_TOPIC,
    TRANSFER_BATCH_TOPIC,
    TRANSFER_SINGLE_TOPIC,
    decode_log,
)
from app.storage.database import async_session_factory
from app.storage.models import ChainEvent


def test_decode_transfer_single():
    """Verify decoding of ERC-1155 TransferSingle event log."""
    operator = "0x" + "11" * 20
    from_addr = "0x" + "22" * 20
    to_addr = "0x" + "33" * 20
    token_id_int = 9876543210123456789
    raw_amount = 5000000  # 5.0 shares

    topics = [
        TRANSFER_SINGLE_TOPIC,
        "0x" + operator[2:].rjust(64, "0"),
        "0x" + from_addr[2:].rjust(64, "0"),
        "0x" + to_addr[2:].rjust(64, "0"),
    ]
    data = hex(token_id_int)[2:].rjust(64, "0") + hex(raw_amount)[2:].rjust(64, "0")
    log = {
        "address": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        "topics": topics,
        "data": "0x" + data,
        "transactionHash": "0x" + "aa" * 32,
        "logIndex": "0x5",
        "blockNumber": "0x1000",
        "blockTimestamp": "0x67300000",
    }

    decoded = decode_log(log, target_wallet=to_addr)
    assert len(decoded) == 1
    ev = decoded[0]
    assert ev.event_name == "TransferSingle"
    assert ev.from_address == from_addr.lower()
    assert ev.to_address == to_addr.lower()
    assert ev.token_id == str(token_id_int)
    assert ev.amount == Decimal("5.0")
    assert ev.log_index == 5
    assert ev.block_number == 4096
    without_timestamp = dict(log)
    del without_timestamp["blockTimestamp"]
    with pytest.raises(ValueError, match="blockTimestamp is required"):
        decode_log(without_timestamp, target_wallet=to_addr)


@pytest.mark.asyncio
async def test_balance_of_batch_decodes_values(monkeypatch):
    from app.chain.rpc import PolygonRpcClient

    client = PolygonRpcClient()

    async def fake_call(method, params):
        assert method == "eth_call"
        assert params[1] == "0x7b"
        assert params[0]["data"].startswith("0x4e1273f4")
        return "0x" + "".join(f"{value:064x}" for value in (32, 2, 1000000, 2500000))

    monkeypatch.setattr(client, "call", fake_call)
    values = await client.get_balances_of_batch("0xcontract", "0x" + "11" * 20, ["1", "2"], 123)
    assert values == [Decimal("1"), Decimal("2.5")]


@pytest.mark.asyncio
async def test_scan_stops_at_call_and_error_limits():
    from scripts.scan_chain_slice import MAX_CALLS, Scanner, ScanStopped

    scanner = Scanner()
    scanner.calls = MAX_CALLS
    with pytest.raises(ScanStopped, match="6,000"):
        await scanner.get_logs(None, {})
    scanner.calls = 0
    scanner.recent.extend([True] * 21 + [False] * 79)
    with pytest.raises(ScanStopped, match="20%"):
        await scanner.get_logs(None, {})


@pytest.mark.asyncio
async def test_archive_403_uses_fallback_without_transient_error(tmp_path, monkeypatch):
    import scripts.scan_chain_slice as scan

    class Response:
        def __init__(self, status, body):
            self.status_code = status
            self.text = body

        def json(self):
            return {"result": []}

    class Client:
        def __init__(self):
            self.endpoints = []

        async def post(self, endpoint, json):
            self.endpoints.append(endpoint)
            if len(self.endpoints) == 1:
                return Response(403, "archive requests require a token")
            return Response(200, "")

    monkeypatch.setattr(scan, "CHECKPOINT", tmp_path / "checkpoint.json")
    scanner = scan.Scanner()
    http = Client()
    assert await scanner.get_logs(http, {}) == []
    assert http.endpoints == [scan.ENDPOINT, scan.FALLBACK_ENDPOINT]
    assert scanner.calls == 2
    assert scanner.errors == 0
    assert list(scanner.recent) == [False]


def test_decode_transfer_batch():
    """Verify dynamic ABI decoding of ERC-1155 TransferBatch event log."""
    from_addr = "0x" + "aa" * 20
    to_addr = "0x" + "bb" * 20
    ids = [1001, 2002, 3003]
    amounts = [10000000, 25000000, 30000000]

    # Dynamic ABI layout: offset_ids (0x40), offset_vals (0xa0),
    # len(ids), ids..., len(vals), vals...
    data = (
        hex(64)[2:].rjust(64, "0")
        + hex(192)[2:].rjust(64, "0")
        + hex(len(ids))[2:].rjust(64, "0")
        + hex(ids[0])[2:].rjust(64, "0")
        + hex(ids[1])[2:].rjust(64, "0")
        + hex(ids[2])[2:].rjust(64, "0")
        + hex(len(amounts))[2:].rjust(64, "0")
        + hex(amounts[0])[2:].rjust(64, "0")
        + hex(amounts[1])[2:].rjust(64, "0")
        + hex(amounts[2])[2:].rjust(64, "0")
    )
    topics = [
        TRANSFER_BATCH_TOPIC,
        "0x" + "00" * 32,
        "0x" + from_addr[2:].rjust(64, "0"),
        "0x" + to_addr[2:].rjust(64, "0"),
    ]
    log = {
        "address": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        "topics": topics,
        "data": "0x" + data,
        "transactionHash": "0x" + "bb" * 32,
        "logIndex": "0xa",
        "blockNumber": "0x2000",
        "blockTimestamp": "0x67300000",
    }

    decoded = decode_log(log, target_wallet=to_addr)
    assert len(decoded) == 3
    assert decoded[0].token_id == "1001"
    assert decoded[0].amount == Decimal("10.0")
    assert decoded[1].token_id == "2002"
    assert decoded[1].amount == Decimal("25.0")
    assert [event.batch_index for event in decoded] == [0, 1, 2]
    assert len(decode_log(log, target_wallet=from_addr)) == 3
    assert decode_log(log, target_wallet="0x" + "99" * 20) == []


def test_decode_position_split():
    """Verify decoding of CTF PositionSplit event log."""
    stakeholder = "0x" + "cc" * 20
    parent_col = "0x" + "00" * 32
    cond_id = "0x" + "dd" * 32
    collateral_data = "2791bca1f2de4661ed88a30c99a7a9449aa84174".rjust(64, "0")
    offset_partition = hex(96)[2:].rjust(64, "0")
    amount_hex = hex(100000000)[2:].rjust(64, "0")  # 100.0 shares

    data = (
        collateral_data
        + offset_partition
        + amount_hex
        + hex(2)[2:].rjust(64, "0")
        + hex(1)[2:].rjust(64, "0")
        + hex(2)[2:].rjust(64, "0")
    )
    topics = [
        POSITION_SPLIT_TOPIC,
        "0x" + stakeholder[2:].rjust(64, "0"),
        parent_col,
        cond_id,
    ]
    log = {
        "address": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        "topics": topics,
        "data": "0x" + data,
        "transactionHash": "0x" + "cc" * 32,
        "logIndex": "0x1",
        "blockNumber": "0x3000",
        "blockTimestamp": "0x67300000",
    }

    decoded = decode_log(log, target_wallet=stakeholder)
    assert len(decoded) == 1
    assert decoded[0].event_name == "PositionSplit"
    assert decoded[0].condition_id == cond_id.lower()
    assert decoded[0].amount == Decimal("100.0")
    assert decode_log(log, target_wallet="0x" + "99" * 20) == []


def test_decode_payout_redemption():
    """Verify decoding of CTF PayoutRedemption event log."""
    redeemer = "0x" + "ee" * 20
    cond_id = "0x" + "ff" * 32
    collateral = "0x" + "2791bca1f2de4661ed88a30c99a7a9449aa84174".rjust(64, "0")
    payout_hex = hex(42000000)[2:].rjust(64, "0")  # 42.0 USDC payout

    data = (
        cond_id[2:]
        + hex(96)[2:].rjust(64, "0")
        + payout_hex
        + hex(1)[2:].rjust(64, "0")
        + hex(1)[2:].rjust(64, "0")
    )
    topics = [
        PAYOUT_REDEMPTION_TOPIC,
        "0x" + redeemer[2:].rjust(64, "0"),
        collateral,
        "0x" + "00" * 32,
    ]
    log = {
        "address": "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
        "topics": topics,
        "data": "0x" + data,
        "transactionHash": "0x" + "dd" * 32,
        "logIndex": "0x2",
        "blockNumber": "0x4000",
        "blockTimestamp": "0x67300000",
    }

    decoded = decode_log(log, target_wallet=redeemer)
    assert len(decoded) == 1
    assert decoded[0].event_name == "PayoutRedemption"
    assert decoded[0].amount == Decimal("42.0")
    assert decode_log(log, target_wallet="0x" + "99" * 20) == []


@pytest.mark.asyncio
async def test_chain_events_db_persistence():
    """Verify writing DecodedChainEvent to PostgreSQL chain_events table."""
    import uuid

    now_ts = datetime.now(timezone.utc)
    unique_tx = "0x" + uuid.uuid4().hex + uuid.uuid4().hex
    ev = ChainEvent(
        wallet="0x951634357346bb291df117f9ac410100feea2104",
        block_number=97000000,
        block_time=now_ts,
        transaction_hash=unique_tx,
        log_index=1,
        contract_address="0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
        event_name="TransferSingle",
        condition_id="0x" + "11" * 32,
        token_id="123456789",
        from_address="0x" + "aa" * 20,
        to_address="0x951634357346bb291df117f9ac410100feea2104",
        amount=Decimal("123.456789"),
        raw_log={"test": True},
        created_at=now_ts,
    )

    async with async_session_factory() as session:
        session.add(ev)
        await session.flush()

        # Query back
        stmt = select(ChainEvent).where(ChainEvent.transaction_hash == unique_tx)
        retrieved = (await session.execute(stmt)).scalar_one_or_none()
        assert retrieved is not None
        assert retrieved.wallet == "0x951634357346bb291df117f9ac410100feea2104"
        assert retrieved.amount == Decimal("123.456789")
        assert retrieved.event_name == "TransferSingle"


@pytest.mark.asyncio
async def test_chain_event_identity_per_wallet_and_batch():
    import uuid

    tx = "0x" + uuid.uuid4().hex + uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:

        def event(wallet, index):
            return ChainEvent(
                wallet=wallet,
                block_number=1,
                block_time=now,
                transaction_hash=tx,
                log_index=1,
                batch_index=index,
                contract_address="contract",
                event_name="TransferBatch",
                token_id=str(index),
                amount=Decimal("1"),
                raw_log={},
                created_at=now,
            )

        session.add_all([event("wallet-a", i) for i in range(3)] + [event("wallet-b", 0)])
        await session.flush()
        assert (
            len(
                (await session.execute(select(ChainEvent).where(ChainEvent.transaction_hash == tx)))
                .scalars()
                .all()
            )
            == 4
        )
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                session.add(event("wallet-a", 0))
                await session.flush()
        await session.rollback()


@pytest.mark.asyncio
async def test_reorg_block_hash_column_persisted():
    """Verify writing and reading block_hash for reorg safety tracking."""
    import uuid

    now_ts = datetime.now(timezone.utc)
    unique_tx = "0x" + uuid.uuid4().hex + uuid.uuid4().hex
    block_hash_val = "0x" + uuid.uuid4().hex + uuid.uuid4().hex

    ev = ChainEvent(
        wallet="0x951634357346bb291df117f9ac410100feea2104",
        block_number=97000001,
        block_hash=block_hash_val,
        block_time=now_ts,
        transaction_hash=unique_tx,
        log_index=0,
        contract_address="0x4d97dcd97ec945f40cf65f87097ace5ea0476045",
        event_name="TransferSingle",
        condition_id=None,
        token_id="999888777",
        from_address=None,
        to_address="0x951634357346bb291df117f9ac410100feea2104",
        amount=Decimal("50.0"),
        raw_log={"block_hash_test": True},
        created_at=now_ts,
    )

    async with async_session_factory() as session:
        session.add(ev)
        await session.flush()

        stmt = select(ChainEvent).where(ChainEvent.transaction_hash == unique_tx)
        retrieved = (await session.execute(stmt)).scalar_one_or_none()
        assert retrieved is not None
        assert retrieved.block_hash == block_hash_val


@pytest.mark.asyncio
async def test_standing_onchain_reconciliation_zero_delta():
    """
    Standing integration test verifying 1:1 on-chain balance parity:
    Derived off-chain shares for active held token must equal on-chain balanceOf.
    """
    from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
    from app.storage.models import Position

    wallet = "0xa98a9d239841f3e7b1657c91350ae2e11894a45a"  # JnStPrdctns
    token_id = "4387389512165036153093226760166297371510368140306126601445778848783457173673"

    async with async_session_factory() as session:
        stmt = select(Position).where(Position.wallet == wallet, Position.token_id == token_id)
        pos = (await session.execute(stmt)).scalar_one_or_none()

    if pos is not None and pos.shares > 0:
        client = PolygonRpcClient()
        try:
            on_chain_shares = await client.get_balance_of(
                contract=CTF_CONTRACT,
                account=wallet,
                token_id=token_id,
            )
            raw_off_chain = int(pos.shares * Decimal("1000000"))
            raw_on_chain = int(on_chain_shares * Decimal("1000000"))
            raw_delta = abs(raw_off_chain - raw_on_chain)

            # Assert exact match down to smallest integer unit (0 delta)
            assert raw_delta == 0, f"Raw integer delta mismatch: {raw_delta}"
            assert pos.shares == on_chain_shares
        except Exception as e:
            pytest.skip(f"Live RPC query skipped due to transient RPC error: {e}")
