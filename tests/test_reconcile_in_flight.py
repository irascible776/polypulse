from decimal import Decimal

import pytest

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.chain.reconcile import classify_delta, ledger_at_reference


@pytest.mark.parametrize(
    "delta,post_poll_net,expected",
    [
        ("-12", "12", "IN_FLIGHT"),
        ("-12", "11", "MISMATCH"),
        ("-12", "-12", "MISMATCH"),
        ("0", "0", "MATCH"),
    ],
)
def test_post_poll_net_classification(delta, post_poll_net, expected):
    assert classify_delta(Decimal(delta), Decimal(post_poll_net), False) == expected


def test_ledger_reference_time_excludes_later_trade():
    now = datetime.now(timezone.utc)
    rows = [
        SimpleNamespace(token_id="t", event_time=now, shares_delta=Decimal("2")),
        SimpleNamespace(
            token_id="t", event_time=now + timedelta(seconds=1), shares_delta=Decimal("3")
        ),
    ]
    current, later = ledger_at_reference(rows, now)
    assert current["t"] == Decimal("2")
    assert later["t"] == Decimal("3")
    assert classify_delta(current["t"] - Decimal("2"), Decimal(0), False) == "MATCH"


def test_missing_before_reference_is_mismatch():
    assert classify_delta(Decimal("-2"), Decimal(0), False) == "MISMATCH"


@pytest.mark.parametrize(
    "delta,later,expected", [("-3", "3", "IN_FLIGHT"), ("-3", "2", "MISMATCH")]
)
def test_reference_boundary_requires_exact_explanation(delta, later, expected):
    assert classify_delta(Decimal(delta), Decimal(0), False, Decimal(later)) == expected
