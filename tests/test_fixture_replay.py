import pytest

from app.ingestion.fixture_replay import FixtureReplaySource


@pytest.mark.asyncio
async def test_fixture_replay_synthetic_wallet():
    source = FixtureReplaySource(fixtures_dir="fixtures")
    synthetic_wallet = "0x0000000000000000000000000000000000000001"

    res = await source.fetch_page(wallet=synthetic_wallet, limit=500)
    assert len(res.events) > 0
    assert res.status_code == 200

    types = {e["type"] for e in res.events}
    assert "TRADE" in types
    assert "REDEEM" in types
    assert "MAKER_REBATE" in types
    assert "TAKER_REBATE" in types
    assert "REFERRAL_REWARD" in types
    assert "YIELD" in types
    assert "MERGE" in types


@pytest.mark.asyncio
async def test_fixture_replay_manual_registration():
    source = FixtureReplaySource()
    test_wallet = "0x9999999999999999999999999999999999999999"

    page1 = {"data": [{"type": "TRADE", "size": 10}], "pagination": {"has_more": True}}
    page2 = {"data": [{"type": "REDEEM", "size": 20}], "pagination": {"has_more": False}}

    source.register_pages(test_wallet, [page1, page2])

    # Page 1
    res1 = await source.fetch_page(wallet=test_wallet)
    assert len(res1.events) == 1
    assert res1.events[0]["type"] == "TRADE"
    assert res1.has_more is True
    assert res1.next_cursor == "page:1"

    # Page 2
    res2 = await source.fetch_page(wallet=test_wallet, cursor=res1.next_cursor)
    assert len(res2.events) == 1
    assert res2.events[0]["type"] == "REDEEM"
    assert res2.has_more is False
    assert res2.next_cursor is None

    # Beyond end
    res3 = await source.fetch_page(wallet=test_wallet, cursor="page:2")
    assert len(res3.events) == 0
    assert res3.has_more is False
