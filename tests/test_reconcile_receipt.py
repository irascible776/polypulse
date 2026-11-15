from unittest.mock import AsyncMock

import pytest

from app.chain.decoder import TRANSFER_SINGLE_TOPIC
from app.chain.reconcile import cache_receipt
from app.chain.rpc import CTF_CONTRACT


@pytest.mark.asyncio
async def test_receipt_block_and_wallet_log_are_cached(monkeypatch):
    wallet = "0x" + "11" * 20
    tx = "0x" + "22" * 32
    log = {
        "address": CTF_CONTRACT,
        "transactionHash": tx,
        "logIndex": "0x0",
        "blockNumber": "0x64",
        "blockHash": "0x" + "33" * 32,
        "topics": [
            TRANSFER_SINGLE_TOPIC,
            "0x" + "00" * 32,
            "0x" + "00" * 32,
            "0x" + wallet[2:].rjust(64, "0"),
        ],
        "data": "0x" + f"{123:064x}{1000000:064x}",
    }
    client = AsyncMock()
    client.call.side_effect = [{"blockNumber": "0x64", "logs": [log]}, {"timestamp": "0x67300000"}]
    save = AsyncMock(return_value=1)
    monkeypatch.setattr("scripts.pull_chain_slice.save_chain_events", save)
    assert await cache_receipt(client, wallet, tx) == 100
    events = save.await_args.args[0]
    assert len(events) == 1 and events[0].wallet == wallet and events[0].token_id == "123"


@pytest.mark.asyncio
async def test_missing_receipt_remains_unknown():
    client = AsyncMock()
    client.call.return_value = None
    with pytest.raises(ValueError, match="receipt unavailable"):
        await cache_receipt(client, "0x" + "11" * 20, "0x" + "22" * 32)
