import httpx

from scripts.spike_activity import (
    analyze_events,
    fetch_activity_history,
)


def test_cursor_loop_multi_page(tmp_path):
    # Mock responses for page 1 and page 2
    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        if not cursor:
            # Page 1
            return httpx.Response(
                200,
                json={
                    "data": [{"type": "TRADE", "size": 100, "timestamp": 1000}],
                    "pagination": {"has_more": True, "next_cursor": "cursor_page_2"},
                },
            )
        elif cursor == "cursor_page_2":
            # Page 2
            return httpx.Response(
                200,
                json={
                    "data": [{"type": "REDEEM", "size": 50, "timestamp": 1050}],
                    "pagination": {"has_more": False, "next_cursor": None},
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = fetch_activity_history(
        client=client,
        wallet="0x1111111111111111111111111111111111111111",
        base_url="http://mock-api",
        pause_seconds=0,
        fixtures_dir=str(tmp_path),
    )

    assert result["total_pages"] == 2
    assert result["total_events"] == 2
    assert result["events"][0]["type"] == "TRADE"
    assert result["events"][1]["type"] == "REDEEM"


def test_cursor_loop_empty_page(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [],
                "pagination": {"has_more": False, "next_cursor": None},
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = fetch_activity_history(
        client=client,
        wallet="0x1111111111111111111111111111111111111111",
        base_url="http://mock-api",
        pause_seconds=0,
        fixtures_dir=str(tmp_path),
    )

    assert result["total_pages"] == 1
    assert result["total_events"] == 0


def test_cursor_loop_retry_on_429(tmp_path, monkeypatch):
    # Ensure sleep is instantaneous in test
    monkeypatch.setattr("time.sleep", lambda s: None)

    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(429, headers={"Retry-After": "1"})
        return httpx.Response(
            200,
            json={
                "data": [{"type": "TRADE", "size": 10}],
                "pagination": {"has_more": False, "next_cursor": None},
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = fetch_activity_history(
        client=client,
        wallet="0x1111111111111111111111111111111111111111",
        base_url="http://mock-api",
        pause_seconds=0,
        fixtures_dir=str(tmp_path),
    )

    assert calls["count"] == 2
    assert result["total_events"] == 1


def test_cursor_loop_breaks_on_repeated_cursor(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [{"type": "TRADE", "size": 10}],
                "pagination": {"has_more": True, "next_cursor": "same_cursor_forever"},
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    result = fetch_activity_history(
        client=client,
        wallet="0x1111111111111111111111111111111111111111",
        base_url="http://mock-api",
        pause_seconds=0,
        fixtures_dir=str(tmp_path),
    )

    # Should fetch page 1, see next_cursor="same_cursor_forever", fetch page 2 with it,
    # then see next_cursor is repeated, and stop to prevent infinite loop
    assert result["total_pages"] == 2
    assert result["total_events"] == 2


def test_analyze_events_composite_uniqueness():
    events = [
        {
            "transaction_hash": "0xaaa",
            "type": "TRADE",
            "token_id": "123",
            "side": "BUY",
            "size": 100,
            "timestamp": 1700000000,
        },
        {
            "transaction_hash": "0xaaa",
            "type": "TRADE",
            "token_id": "123",
            "side": "BUY",
            "size": 50,  # Different fill size in same tx
            "timestamp": 1700000000,
        },
    ]
    analysis = analyze_events(events)
    assert analysis["total_tx_hashes"] == 2
    assert analysis["unique_tx_hashes"] == 1
    assert analysis["unique_ext_tuples"] == 2
