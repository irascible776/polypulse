"""Tracked-wallet snapshots, notification coalescing, and listener resync."""

import asyncio
import re
from datetime import datetime

import pytest
from sqlalchemy import text
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from app.main import app
from app.serving.websocket import ws_manager
from app.storage.database import async_session_factory

WALLET = "0x951634357346bb291df117f9ac410100feea2104"


def check_timestamps(value):
    if isinstance(value, dict):
        for item in value.values():
            check_timestamps(item)
    elif isinstance(value, list):
        for item in value:
            check_timestamps(item)
    elif isinstance(value, str) and re.match(r"^\d{4}-\d\d-\d\dT\d\d:\d\d", value):
        assert value.endswith("Z"), value
        assert datetime.fromisoformat(value).utcoffset().total_seconds() == 0


def check_money(value):
    if isinstance(value, dict):
        for key, item in value.items():
            financial = key in {
                "price",
                "shares",
                "bid",
                "ask",
                "cost_basis_total",
                "unrealized_priced",
                "estimated_unrealized",
                "total_pnl_excl_unpriced",
            } or key.endswith(("_pnl", "_price", "_shares", "_basis", "_value", "_delta"))
            if financial and item is not None:
                assert isinstance(item, str), (key, item)
            check_money(item)
    elif isinstance(value, list):
        for item in value:
            check_money(item)


async def send_burst():
    import json

    async with async_session_factory() as session:
        async with session.begin():
            for ref in ("1", "2", "3"):
                payload = json.dumps({"type": "price_update", "wallet": WALLET, "ref": ref})
                await session.execute(
                    text("select pg_notify('polypulse_events', :payload)"), {"payload": payload}
                )


async def restart_listener():
    async with async_session_factory() as session:
        result = await session.execute(
            text(
                "select pg_terminate_backend(pid) from pg_stat_activity "
                "where application_name='polypulse_listener' and datname=current_database()"
            )
        )
        assert result.scalars().all() == [True]


def test_websocket_tracking_snapshot_notifications_and_resync():
    with TestClient(app) as client:
        client.portal.call(asyncio.wait_for, ws_manager._listening.wait(), 5)
        with pytest.raises(WebSocketDisconnect) as rejected:
            with client.websocket_connect("/ws/wallets/0x" + "f" * 40):
                pass
        assert rejected.value.code == 4403
        for suffix in ("positions", "pnl", "trades"):
            response = client.get(f"/wallets/{WALLET}/{suffix}")
            assert response.status_code == 200
            check_timestamps(response.json())
            check_money(response.json())
        check_timestamps(client.get("/health").json())
        with client.websocket_connect(f"/ws/wallets/{WALLET.upper().replace('0X', '0x')}") as ws:
            snapshot = ws.receive_json()
            assert snapshot["type"] == "snapshot" and snapshot["seq"] == 1
            assert snapshot["wallet"] == WALLET
            check_timestamps(snapshot)
            check_money(snapshot)
            client.portal.call(send_burst)
            push = ws.receive_json()
            assert push["type"] == "price_update" and push["seq"] == 2
            assert push["payload"]["ref"] == "3"
            check_timestamps(push)
            check_money(push)
            client.portal.call(restart_listener)
            resync = ws.receive_json()
            assert resync["type"] == "snapshot" and resync["seq"] == 3
            check_timestamps(resync)
