from datetime import datetime, timedelta, timezone

from scripts.reconcile_chain_slice import print_ingestion_freshness


def test_freshness_warning_uses_fifteen_minute_threshold(capsys):
    reference = datetime(2026, 11, 14, tzinfo=timezone.utc)
    for age, warning in ((None, False), (900, False), (901, True)):
        last_success = reference - timedelta(seconds=age) if age is not None else None
        print_ingestion_freshness(last_success, reference)
        output = capsys.readouterr().out
        assert str(last_success) in output
        assert str(reference) in output
        assert ("WARNING" in output) is warning
