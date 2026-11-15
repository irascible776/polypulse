from decimal import Decimal

import pytest

from app.chain.reconcile import classify_delta


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
