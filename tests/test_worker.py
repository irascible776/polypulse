import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import delete, func, select

from app.ingestion.source import PageResult
from app.storage.database import async_session_factory
from app.storage.models import MarketPrice, MarketResolution, SourceEvent
from app.valuation.pricing import PriceResolver
from app.worker import Worker


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "book,expected_live,not_found",
    [
        ({"bids": [{"price": "0.45"}], "asks": [{"price": "0.55"}]}, 1, 0),
        ({"bids": [{"price": "0.45"}], "asks": []}, 0, 0),
        (None, 0, 1),
        ({"bids": [{"price": "0.2"}], "asks": [{"price": "0.8"}]}, 1, 0),
    ],
)
async def test_price_cycle_book_cases(book, expected_live, not_found):
    token = uuid.uuid4().hex
    condition = uuid.uuid4().hex

    def respond(request):
        if request.url.path == "/books":
            if book is None:
                return httpx.Response(404)
            return httpx.Response(200, json=[{"asset_id": token, **book}])
        if request.url.path == "/book":
            return httpx.Response(404)
        if request.url.path == "/last-trades-prices":
            return httpx.Response(200, json=[{"token_id": token, "price": "0.4"}])
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        worker = Worker(http=client)
        worker.open_tokens = AsyncMock(return_value=[(token, condition)])
        result = await worker.price_cycle()
        assert result["live_quote"] == expected_live
        assert result["last_trade"] == 1
        assert result["not_found"] == not_found
        assert (condition in worker.urgent_resolutions) is (book is None)
        assert result["errors"] == 0
        async with async_session_factory() as session:
            prices = (
                (await session.execute(select(MarketPrice).where(MarketPrice.token_id == token)))
                .scalars()
                .all()
            )
            assert len(prices) == expected_live + 1
            if book and book["asks"] and book["asks"][0]["price"] == "0.8":
                quote = await PriceResolver().get_mark_price(session, token)
                assert quote.source == "last_trade" and quote.price == Decimal("0.4")
            await session.execute(delete(MarketPrice).where(MarketPrice.token_id == token))
            await session.commit()
        await worker.close()


@pytest.mark.asyncio
async def test_price_cycle_continues_after_item_failure():
    bad, good = uuid.uuid4().hex, uuid.uuid4().hex

    def respond(request):
        if request.url.path == "/books":
            return httpx.Response(
                200,
                json=[
                    {"asset_id": bad, "bids": [{"price": "invalid"}], "asks": [{"price": "0.5"}]},
                    {"asset_id": good, "bids": [{"price": "0.4"}], "asks": [{"price": "0.6"}]},
                ],
            )
        if request.url.path == "/last-trades-prices":
            return httpx.Response(200, json=[{"token_id": t, "price": "0.4"} for t in (bad, good)])
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        worker = Worker(http=client)
        worker.open_tokens = AsyncMock(return_value=[(bad, "c1"), (good, "c2")])
        result = await worker.price_cycle()
        assert result["errors"] == 1 and result["live_quote"] == 1
        async with async_session_factory() as session:
            count = await session.scalar(
                select(func.count()).select_from(MarketPrice).where(MarketPrice.token_id == good)
            )
            assert count == 2
            await session.execute(delete(MarketPrice).where(MarketPrice.token_id.in_([bad, good])))
            await session.commit()
        await worker.close()


@pytest.mark.asyncio
async def test_empty_side_default_is_not_saved_as_a_trade():
    token = uuid.uuid4().hex

    def respond(request):
        if request.url.path == "/books":
            return httpx.Response(200, json=[{"asset_id": token, "bids": [], "asks": []}])
        if request.url.path == "/last-trades-prices":
            return httpx.Response(200, json=[{"token_id": token, "price": "0.5", "side": ""}])
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        worker = Worker(http=client)
        worker.open_tokens = AsyncMock(return_value=[(token, "condition")])
        result = await worker.price_cycle()
        assert result["last_trade"] == 0
        async with async_session_factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(MarketPrice)
                    .where(MarketPrice.token_id == token)
                )
                == 0
            )
        await worker.close()


@pytest.mark.asyncio
async def test_gamma_closed_unresolved_is_queued_for_resolution():
    token, condition = uuid.uuid4().hex, uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        session.add(
            MarketResolution(
                condition_id=condition,
                resolved=False,
                payout_numerators=[0, 0],
                payout_denominator=Decimal(0),
                gamma_closed=True,
                gamma_raw={},
                chain_block=1,
                checked_at=now,
                agreement=False,
            )
        )
        await session.commit()

    def respond(request):
        if request.url.path == "/books":
            return httpx.Response(200, json=[{"asset_id": token, "bids": [], "asks": []}])
        if request.url.path == "/last-trades-prices":
            return httpx.Response(200, json=[])
        raise AssertionError(request.url)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        worker = Worker(http=client)
        worker.open_tokens = AsyncMock(return_value=[(token, condition)])
        await worker.price_cycle()
        assert condition in worker.urgent_resolutions
        assert worker.resolution_wakeup.is_set()
        await worker.close()


@pytest.mark.asyncio
async def test_urgent_resolution_cycle_checks_queued_condition(monkeypatch):
    condition = "0x" + uuid.uuid4().hex * 2
    checked = AsyncMock(return_value={"condition_id": condition})
    saved = AsyncMock()
    monkeypatch.setattr("app.worker.check_resolution", checked)
    monkeypatch.setattr("app.worker.save_resolution", saved)
    worker = Worker()
    worker.queue_resolution(condition)
    result = await worker.resolution_cycle(urgent_only=True)
    assert result["upserted"] == 1
    assert result["errors"] == 0
    checked.assert_awaited_once_with(condition)
    assert not worker.urgent_resolutions
    await worker.close()


@pytest.mark.asyncio
async def test_wallet_advisory_lock_skips_concurrent_worker():
    wallet = "0x" + uuid.uuid4().hex + "0" * 8
    entered, release = asyncio.Event(), asyncio.Event()

    class Source:
        source_tag = "data_api"

        async def fetch_page(self, **kwargs):
            entered.set()
            await release.wait()
            event = {
                "proxy_wallet": wallet,
                "type": "TRADE",
                "side": "BUY",
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
                "size": "1",
                "usdc_size": "0.4",
                "price": "0.4",
                "condition_id": "test_condition",
                "token_id": "test_token",
                "transaction_hash": "0x" + "1" * 64,
            }
            return PageResult(events=[event], next_cursor=None, has_more=False)

        async def close(self):
            pass

    worker = Worker()
    await worker.source.close()
    worker.source = Source()
    first = asyncio.create_task(worker.wallet_cycle(wallet))
    await asyncio.wait_for(entered.wait(), timeout=10)
    second = await worker.wallet_cycle(wallet)
    assert second["skipped_lock"] is True
    release.set()
    result = await first
    assert result["inserted"] == 1
    async with async_session_factory() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(SourceEvent).where(SourceEvent.wallet == wallet)
            )
            == 1
        )
    await worker.close()
