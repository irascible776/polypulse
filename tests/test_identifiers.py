from decimal import Decimal

from app.ingestion.identifiers import (
    EventIdAssigner,
    EventIdGenerator,
    compute_base_key,
    compute_source_event_id,
    normalize_decimal_str,
    parse_json_with_decimal,
)


def test_parse_json_with_decimal():
    raw_json = '{"size": 123.456, "price": 0.50, "usdc_size": 61.728}'
    parsed = parse_json_with_decimal(raw_json)
    assert isinstance(parsed["size"], Decimal)
    assert isinstance(parsed["price"], Decimal)
    assert isinstance(parsed["usdc_size"], Decimal)
    assert parsed["size"] == Decimal("123.456")


def test_profile_fields_do_not_affect_id():
    base_event = {
        "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
        "transaction_hash": "0x123abc",
        "type": "TRADE",
        "token_id": "456",
        "side": "BUY",
        "size": Decimal("100.0"),
        "usdc_size": Decimal("50.0"),
        "price": Decimal("0.50"),
        "timestamp": 1700000000,
        "name": "TraderOne",
        "bio": "Crypto enthusiast",
        "pseudonym": "SpeedyFox",
        "profile_image": "http://img1.png",
    }

    modified_profile = dict(base_event)
    modified_profile["name"] = "TraderRenamed"
    modified_profile["bio"] = "New bio"
    modified_profile["pseudonym"] = "SneakyWolf"
    modified_profile["profile_image"] = "http://img2.png"

    id_1 = compute_source_event_id(base_event)
    id_2 = compute_source_event_id(modified_profile)

    assert id_1 == id_2, "Profile modifications must not change the deterministic event id"


def test_base_key_and_occurrence_format():
    event = {
        "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
        "transaction_hash": "0xabc",
        "type": "TRADE",
        "condition_id": "0xcond",
        "token_id": "123",
        "side": "BUY",
        "outcome_index": 0,
        "size": Decimal("10"),
        "usdc_size": Decimal("5"),
        "price": Decimal("0.5"),
        "timestamp": 1700000000,
    }
    base_key = compute_base_key(event)
    assert len(base_key) == 64
    event_id_0 = compute_source_event_id(base_key, occurrence=0)
    event_id_1 = compute_source_event_id(base_key, occurrence=1)
    assert event_id_0 == f"{base_key}:0"
    assert event_id_1 == f"{base_key}:1"


# --- Required Tests for 2.0a ---


def test_2_0a_1_same_input_gives_same_ids_across_generator_reuse():
    """
    Test 1: the same input gives the same ids whether the generator is new or has been used before.
    """
    sample_batch = [
        {
            "proxy_wallet": "0x1111111111111111111111111111111111111111",
            "transaction_hash": "0xaaa",
            "type": "TRADE",
            "condition_id": "0xcond1",
            "token_id": "t1",
            "side": "BUY",
            "size": Decimal("100"),
            "usdc_size": Decimal("50"),
            "price": Decimal("0.5"),
            "timestamp": 1700000010,
        },
        {
            "proxy_wallet": "0x1111111111111111111111111111111111111111",
            "transaction_hash": "0xbbb",
            "type": "TRADE",
            "condition_id": "0xcond2",
            "token_id": "t2",
            "side": "SELL",
            "size": Decimal("200"),
            "usdc_size": Decimal("120"),
            "price": Decimal("0.6"),
            "timestamp": 1700000010,
        },
    ]

    # Generator 1 used once
    gen1 = EventIdAssigner()
    res1_first = gen1.assign_ids(sample_batch, is_final=True)
    ids_gen1_first = [ev_id for _, ev_id in res1_first]

    # Generator 1 used again on a different batch, then reused on sample_batch
    other_batch = [
        {
            "proxy_wallet": "0x1111111111111111111111111111111111111111",
            "transaction_hash": "0xccc",
            "type": "REDEEM",
            "timestamp": 1700000020,
        }
    ]
    gen1.assign_ids(other_batch, is_final=True)
    res1_second = gen1.assign_ids(sample_batch, is_final=True)
    ids_gen1_second = [ev_id for _, ev_id in res1_second]

    # Generator 2 (brand new)
    gen2 = EventIdGenerator()
    res2 = gen2.assign_ids(sample_batch, is_final=True)
    ids_gen2 = [ev_id for _, ev_id in res2]

    msg_reused = "Reused generator must produce identical IDs"
    assert ids_gen1_first == ids_gen1_second, msg_reused
    msg_new = "New generator must produce identical IDs as reused generator"
    assert ids_gen1_first == ids_gen2, msg_new


def test_2_0a_2_ingesting_same_batch_twice_adds_zero_rows():
    """
    Test 2: ingesting the same batch twice adds zero rows.
    """
    batch = [
        {
            "proxy_wallet": "0x2222222222222222222222222222222222222222",
            "transaction_hash": "0x100",
            "type": "TRADE",
            "timestamp": 1700000100,
            "size": Decimal("50"),
        },
        {
            "proxy_wallet": "0x2222222222222222222222222222222222222222",
            "transaction_hash": "0x101",
            "type": "TRADE",
            "timestamp": 1700000101,
            "size": Decimal("75"),
        },
    ]

    stored_rows: dict[str, dict] = {}

    def simulate_ingest(events: list[dict]) -> int:
        gen = EventIdAssigner()
        assigned = gen.assign_ids(events, is_final=True)
        inserted_count = 0
        for ev, ev_id in assigned:
            if ev_id not in stored_rows:
                stored_rows[ev_id] = ev
                inserted_count += 1
        return inserted_count

    first_insert = simulate_ingest(batch)
    assert first_insert == 2
    assert len(stored_rows) == 2

    # Second ingest of the exact same batch
    second_insert = simulate_ingest(batch)
    assert second_insert == 0, "Ingesting same batch twice must add zero rows"
    assert len(stored_rows) == 2


def test_2_0a_3_two_identical_fills_split_across_page_boundary():
    """
    Test 3: two identical fills split across a page boundary get ids 0 and 1.
    """
    fill_a = {
        "proxy_wallet": "0x3333333333333333333333333333333333333333",
        "transaction_hash": "0xshared_tx",
        "type": "TRADE",
        "token_id": "tok_123",
        "side": "BUY",
        "size": Decimal("50.0"),
        "usdc_size": Decimal("25.0"),
        "price": Decimal("0.50"),
        "timestamp": 1700000500,  # Exact same timestamp second
    }
    fill_b = dict(fill_a)

    prior_event = {
        "proxy_wallet": "0x3333333333333333333333333333333333333333",
        "transaction_hash": "0xprior",
        "type": "TRADE",
        "timestamp": 1700000499,
    }
    subsequent_event = {
        "proxy_wallet": "0x3333333333333333333333333333333333333333",
        "transaction_hash": "0xsubsequent",
        "type": "TRADE",
        "timestamp": 1700000501,
    }

    # Page 1 contains prior_event and fill_a (which is at timestamp 1700000500)
    page_1 = [prior_event, fill_a]
    # Page 2 contains fill_b (also at timestamp 1700000500) and subsequent_event
    page_2 = [fill_b, subsequent_event]

    assigner = EventIdAssigner()

    # Process page 1 (is_final=False) -> fill_a held back (trailing timestamp)
    res_page_1 = assigner.assign_ids(page_1, is_final=False)
    assert len(res_page_1) == 1
    assert res_page_1[0][0]["transaction_hash"] == "0xprior"

    # Process page 2 (is_final=True) -> fill_a and fill_b grouped together at 1700000500
    res_page_2 = assigner.assign_ids(page_2, is_final=True)
    assert len(res_page_2) == 3

    # Find the assigned IDs for fill_a and fill_b
    shared_matches = [ev_id for ev, ev_id in res_page_2 if ev["transaction_hash"] == "0xshared_tx"]
    assigned_fill_a = shared_matches[0]
    assigned_fill_b = shared_matches[1]

    base_key = compute_base_key(fill_a)
    assert assigned_fill_a == f"{base_key}:0", "First fill must receive occurrence 0"
    msg_fill_b = "Second fill across page boundary must receive occurrence 1"
    assert assigned_fill_b == f"{base_key}:1", msg_fill_b


def test_2_0a_4_overlap_refetch_adds_zero_rows():
    """
    Test 4: an overlap re-fetch of already-saved events adds zero rows.
    """
    events_day1 = [
        {
            "proxy_wallet": "0x4444444444444444444444444444444444444444",
            "transaction_hash": f"0xtx_{t}",
            "type": "TRADE",
            "timestamp": t,
            "size": Decimal("10"),
        }
        for t in range(1700001000, 1700001005)  # 5 events: 1000 to 1004
    ]

    # Database state
    stored_ids: set[str] = set()

    assigner_initial = EventIdAssigner()
    initial_res = assigner_initial.assign_ids(events_day1, is_final=True)
    for _, ev_id in initial_res:
        stored_ids.add(ev_id)
    assert len(stored_ids) == 5

    # Re-fetch with overlap: re-fetches from 1700001003 to 1700001007 (overlap on 1003 and 1004)
    overlap_events = [
        {
            "proxy_wallet": "0x4444444444444444444444444444444444444444",
            "transaction_hash": f"0xtx_{t}",
            "type": "TRADE",
            "timestamp": t,
            "size": Decimal("10"),
        }
        for t in range(1700001003, 1700001008)  # 5 events: 1003 to 1007
    ]

    assigner_overlap = EventIdAssigner()
    overlap_res = assigner_overlap.assign_ids(overlap_events, is_final=True)

    new_rows_count = 0
    duplicate_count = 0
    for _, ev_id in overlap_res:
        if ev_id in stored_ids:
            duplicate_count += 1
        else:
            stored_ids.add(ev_id)
            new_rows_count += 1

    assert duplicate_count == 2, "2 overlapping events must be detected as existing"
    assert new_rows_count == 3, "Only the 3 brand new events (1005, 1006, 1007) must be added"
    assert len(stored_ids) == 8


def test_2_0a_decimal_normalization_base_keys():
    """
    Test 1 for 2.0a: Normalize Decimal text before hashing (e.g. format(d.normalize(), 'f'),
    handle zero).
    Verify '0.50', '0.5', '100', '100.0', '1E+2' produce equal base keys.
    Also verify zero variants ('0', '0.0', '0.00') produce equal base keys.
    """
    assert normalize_decimal_str("0.50") == "0.5"
    assert normalize_decimal_str("0.5") == "0.5"
    assert normalize_decimal_str("100") == "100"
    assert normalize_decimal_str("100.0") == "100"
    assert normalize_decimal_str("1E+2") == "100"
    assert normalize_decimal_str("0") == "0"
    assert normalize_decimal_str("0.0") == "0"
    assert normalize_decimal_str("0.00") == "0"

    base_event_1 = {
        "proxy_wallet": "0x1234567890123456789012345678901234567890",
        "transaction_hash": "0xabc",
        "type": "TRADE",
        "price": Decimal("0.50"),
        "size": Decimal("100"),
        "usdc_size": Decimal("50.00"),
        "timestamp": 1700000000,
    }
    base_event_2 = {
        "proxy_wallet": "0x1234567890123456789012345678901234567890",
        "transaction_hash": "0xabc",
        "type": "TRADE",
        "price": Decimal("0.5"),
        "size": Decimal("100.0"),
        "usdc_size": Decimal("50"),
        "timestamp": 1700000000,
    }
    base_event_3 = {
        "proxy_wallet": "0x1234567890123456789012345678901234567890",
        "transaction_hash": "0xabc",
        "type": "TRADE",
        "price": "0.50",
        "size": "1E+2",
        "usdc_size": "50.0",
        "timestamp": 1700000000,
    }

    key_1 = compute_base_key(base_event_1)
    key_2 = compute_base_key(base_event_2)
    key_3 = compute_base_key(base_event_3)

    assert key_1 == key_2, "Decimal('0.50') and Decimal('0.5') must yield identical base key"
    assert key_1 == key_3, "'1E+2', '100.0', and '100' must yield identical base key"

    # Test zero handling
    zero_event_1 = {
        "proxy_wallet": "0x1234567890123456789012345678901234567890",
        "transaction_hash": "0xzero",
        "type": "REDEEM",
        "price": Decimal("0"),
        "size": Decimal("0.0"),
        "usdc_size": Decimal("0.00"),
        "timestamp": 1700000000,
    }
    zero_event_2 = {
        "proxy_wallet": "0x1234567890123456789012345678901234567890",
        "transaction_hash": "0xzero",
        "type": "REDEEM",
        "price": "0.000",
        "size": "0",
        "usdc_size": "0.0",
        "timestamp": 1700000000,
    }
    assert compute_base_key(zero_event_1) == compute_base_key(zero_event_2)


def test_2_0a_aborted_walk_with_reset_does_not_leak_held_events():
    """
    Test 2 for 2.0a: Held events must not leak between walks.
    Simulate an aborted walk holding trailing events, followed by reset() and a brand-new walk.
    """
    assigner = EventIdAssigner(wallet="0x1111111111111111111111111111111111111111")
    aborted_page = [
        {
            "proxy_wallet": "0x1111111111111111111111111111111111111111",
            "transaction_hash": "0xaborted_tx",
            "type": "TRADE",
            "timestamp": 1700000100,
            "size": Decimal("10"),
        }
    ]
    # Page is not final, so the event at timestamp 1700000100 is held back
    ready = assigner.assign_ids(aborted_page, is_final=False)
    assert len(ready) == 0
    assert len(assigner._held_events) == 1

    # Simulate aborting the walk and resetting assigner state
    assigner.reset()
    assert len(assigner._held_events) == 0

    # Start a brand new walk with completely different events
    new_walk_events = [
        {
            "proxy_wallet": "0x1111111111111111111111111111111111111111",
            "transaction_hash": "0xnew_tx",
            "type": "TRADE",
            "timestamp": 1700000200,
            "size": Decimal("20"),
        }
    ]
    new_ready = assigner.assign_ids(new_walk_events, is_final=True)
    assert len(new_ready) == 1
    ev, _ = new_ready[0]
    assert ev["transaction_hash"] == "0xnew_tx"
    # Ensure aborted_tx was not leaked into the new walk
    assert all(e["transaction_hash"] != "0xaborted_tx" for e, _ in new_ready)


def test_2_0a_mid_walk_page_2_not_final_released_on_page_3():
    """
    Test 3 for 2.0a: Mid-walk test where page 2 is also is_final=False and
    the held group spanning multiple pages is released on page 3.
    """
    assigner = EventIdAssigner(wallet="0x2222222222222222222222222222222222222222")

    # Identical fill template at timestamp 1700000500
    fill_common = {
        "proxy_wallet": "0x2222222222222222222222222222222222222222",
        "transaction_hash": "0xshared_tx",
        "type": "TRADE",
        "token_id": "tok_abc",
        "side": "BUY",
        "size": Decimal("50.0"),
        "usdc_size": Decimal("25.0"),
        "price": Decimal("0.50"),
        "timestamp": 1700000500,
    }

    # Page 1: has an earlier event (ts=499) and fill 1 (ts=500). is_final=False
    p1_ev0 = dict(fill_common, transaction_hash="0xearly", timestamp=1700000499)
    p1_ev1 = dict(fill_common, id="fill_1")  # ts=500
    p1 = [p1_ev0, p1_ev1]

    # Page 2: only contains fill 2 (ts=500). is_final=False (mid-walk!)
    p2_ev0 = dict(fill_common, id="fill_2")  # ts=500
    p2 = [p2_ev0]

    # Page 3: contains fill 3 (ts=500) and later event (ts=501). is_final=True
    p3_ev0 = dict(fill_common, id="fill_3")  # ts=500
    p3_ev1 = dict(fill_common, transaction_hash="0xlater", timestamp=1700000501)
    p3 = [p3_ev0, p3_ev1]

    # Process Page 1: early event released, fill 1 held back
    res_1 = assigner.assign_ids(p1, is_final=False)
    assert len(res_1) == 1
    assert res_1[0][0]["transaction_hash"] == "0xearly"
    assert len(assigner._held_events) == 1

    # Process Page 2: since page 2 ends at ts=500 and all events are at ts=500,
    # fill 1 and fill 2 remain held back! res_2 should be empty
    res_2 = assigner.assign_ids(p2, is_final=False)
    assert len(res_2) == 0
    assert len(assigner._held_events) == 2

    # Process Page 3: final page, releases fills 1, 2, 3 and later event
    res_3 = assigner.assign_ids(p3, is_final=True)
    assert len(res_3) == 4

    fills = [(ev, eid) for ev, eid in res_3 if ev.get("transaction_hash") == "0xshared_tx"]
    assert len(fills) == 3

    base_key = compute_base_key(fill_common)
    assert fills[0][1] == f"{base_key}:0"
    assert fills[1][1] == f"{base_key}:1"
    assert fills[2][1] == f"{base_key}:2"
