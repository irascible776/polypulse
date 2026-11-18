"""Part 1 verification: URL normalization, CORS/WS origin, fleet heartbeats, headers, health."""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import update

from app.config import Settings, normalize_database_url
from app.main import app
from app.serving.fleet import delete_heartbeat, get_fleet_status, upsert_heartbeat
from app.storage.database import async_session_factory
from app.storage.models import ServiceHeartbeat
from app.worker import Worker, _start_health_server


def test_normalize_database_url_drivers_and_dedup_sslmode():
    sa_url, sa_args = normalize_database_url(
        "postgres://postgres.abc:secret@aws-0-us-east-1.pooler.supabase.com:5432/postgres?sslmode=prefer&keepalives=0",
        driver="postgresql+psycopg",
        sslmode="require",
    )
    assert sa_url.startswith("postgresql+psycopg://postgres.abc:secret@")
    assert "sslmode=" not in sa_url
    assert "keepalives=" not in sa_url
    assert sa_args["sslmode"] == "require"
    assert sa_args["keepalives"] == 1
    assert sa_args["keepalives_idle"] == 30

    pg_url, pg_args = normalize_database_url(
        "postgresql+psycopg://user:pw@localhost:5432/polypulse?sslmode=verify-full",
        driver="postgresql",
    )
    assert pg_url == "postgresql://user:pw@localhost:5432/polypulse"
    assert pg_args["sslmode"] == "verify-full"


def test_origin_allowlist_and_regex(monkeypatch):
    monkeypatch.setenv("CORS_ORIGINS", "https://polypulse.vercel.app,http://localhost:3000/")
    monkeypatch.setenv("CORS_ORIGIN_REGEX", r"^https://polypulse-[a-z0-9-]+\.vercel\.app$")
    monkeypatch.setenv("WS_ALLOW_EMPTY_ORIGIN", "false")
    s = Settings()
    assert s.is_origin_allowed("https://polypulse.vercel.app")
    assert s.is_origin_allowed("http://localhost:3000")
    assert s.is_origin_allowed("https://polypulse-pr42.vercel.app")
    assert not s.is_origin_allowed("https://evil.example.com")
    assert not s.is_origin_allowed(None)


@pytest.mark.asyncio
async def test_instance_headers_present():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health", headers={"X-Request-Id": "req-abc123"})
        assert resp.status_code == 200
        assert resp.headers.get("X-Instance-Id")
        assert resp.headers.get("X-Request-Id") == "req-abc123"
        assert resp.headers.get("X-Response-Time-Ms")
        body = resp.json()
        assert body["instance_id"] == resp.headers["X-Instance-Id"]


@pytest.mark.asyncio
async def test_fleet_heartbeats_bucket_map_and_split_claim():
    now = datetime.now(timezone.utc)
    ids = ["test-api-1", "test-worker-1", "test-worker-2", "test-stale-worker"]
    try:
        assert await upsert_heartbeat(
            instance_id="test-api-1",
            role="api",
            started_at=now,
            ws_connections=7,
            listener_ok=True,
            latency_p50_ms=11.2,
            latency_p95_ms=28.4,
            latency_p99_ms=41.0,
        )
        assert await upsert_heartbeat(
            instance_id="test-worker-1",
            role="worker",
            started_at=now,
            buckets_held=[0, 1, 2, 3],
        )
        assert await upsert_heartbeat(
            instance_id="test-worker-2",
            role="worker",
            started_at=now,
            buckets_held=[3, 8, 9],
        )
        assert await upsert_heartbeat(
            instance_id="test-stale-worker",
            role="worker",
            started_at=now - timedelta(minutes=10),
            buckets_held=[15],
        )
        async with async_session_factory() as session:
            async with session.begin():
                await session.execute(
                    update(ServiceHeartbeat)
                    .where(ServiceHeartbeat.instance_id == "test-stale-worker")
                    .values(last_seen_at=now - timedelta(minutes=2))
                )

        fleet = await get_fleet_status()
        assert len(fleet["buckets"]) == 16
        assert 3 in fleet["summary"]["split_claim_buckets"]
        b3 = fleet["buckets"][3]
        assert b3["split_claim"] is True
        assert b3["owners"] == ["test-worker-1", "test-worker-2"]
        # Stale worker's bucket 15 must not be shown as actively owned
        assert fleet["buckets"][15]["owner"] is None
        stale_row = next(i for i in fleet["instances"] if i["instance_id"] == "test-stale-worker")
        assert stale_row["status"] == "stale"

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            resp = await client.get("/fleet")
            assert resp.status_code == 200
            assert len(resp.json()["buckets"]) == 16
    finally:
        for iid in ids:
            await delete_heartbeat(iid)


@pytest.mark.asyncio
async def test_worker_health_port_server():
    stop = asyncio.Event()
    worker = Worker()
    worker.held_buckets.update({2, 7})
    server = await _start_health_server(0, "worker-test-1", stop, [worker])
    port = server.sockets[0].getsockname()[1]
    try:

        async def raw_get(path: str) -> tuple[int, dict]:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            await writer.drain()
            data = await reader.read()
            writer.close()
            await writer.wait_closed()
            head, _, body = data.partition(b"\r\n\r\n")
            status_code = int(head.splitlines()[0].split()[1])
            return status_code, json.loads(body.decode())

        code, payload = await raw_get("/health")
        assert code == 200
        assert payload["status"] == "ok"
        assert payload["buckets_held"] == [2, 7]

        code, _ = await raw_get("/ready")
        assert code == 200

        stop.set()
        code, payload = await raw_get("/ready")
        assert code == 503
        assert payload["status"] == "draining"

        code, _ = await raw_get("/unknown")
        assert code == 404
    finally:
        server.close()
        await server.wait_closed()
        await worker.close()
