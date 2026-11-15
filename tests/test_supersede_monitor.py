import json

from app.ingestion.poller import record_committed_supersedes


def test_record_committed_supersedes_appends_durable_json_lines(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    event = {
        "wallet": "wallet",
        "token_id": "token",
        "dedup_key": "fingerprint",
        "source_event_id": "source",
        "superseded_id": 1,
        "survivor_id": 2,
    }
    record_committed_supersedes([event])
    record_committed_supersedes([event])
    lines = (tmp_path / "reports" / "supersede_events.log").read_text().splitlines()
    assert len(lines) == 2
    for line in lines:
        decoded = json.loads(line)
        assert decoded["wallet"] == "wallet"
        assert decoded["token_id"] == "token"
        assert decoded["dedup_key"] == "fingerprint"
        assert decoded["timestamp"]


def test_no_events_does_not_create_a_log(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    record_committed_supersedes([])
    assert not (tmp_path / "reports" / "supersede_events.log").exists()
