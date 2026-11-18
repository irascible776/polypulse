# Phase 7 second-review fixes — 2026-11-16

Policy: planning=deep; context_depth=dependency; verification_depth=impact;
delegation=none. Scope: WebSocket recovery, selective replay controls, position
loading and checkpoint cadence. No schema, dependency or live configuration changes.

## Changed

| File | Result |
| --- | --- |
| `app/serving/websocket.py` | Retain failed refresh notifications, retry three times with increasing delays, bound reads/sends, close with 1013 on exhaustion, preserve newer notifications and stop without task resurrection. Failed listener resync schedules recovery. |
| `app/config.py`, `.env.example` | Add independent condition-replay wallet allowlist and checkpoint interval, defaulting to empty and 1 respectively. |
| `app/worker.py` | Selective replay works for the independent allowlist even with zero CPU workers; existing parallel opt-in remains compatible. |
| `app/accounting/projector.py` | Read selected conditions for computation; load full wallet positions only when a checkpoint is due. First and replay checkpoints remain complete. |
| `tests/test_phase7_websocket.py` | Transient/permanent read failures, notifications during reads and shutdown cancellation. |
| `tests/test_phase7_replay.py` | Real worker with serial selective replay on/off; spaced checkpoint state loading, complete snapshots and late-correction parity. |
| Implementation, progress and independent testing brief | Document controls, cost tradeoffs and added release drills. |

## Verification

Used the existing disposable PostgreSQL cluster at `/tmp/polypulse-phase7-pg`,
port 55437, database `polypulse_phase7_test`. No live database was used. Commands:

```sh
DATABASE_URL=postgresql+psycopg://postgres@127.0.0.1:55437/polypulse_phase7_test SECONDARY_DATABASE_URL='' UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync pytest tests/test_phase7_conditions.py tests/test_phase7_replay.py tests/test_phase7_snapshot_cache.py tests/test_phase7_websocket.py tests/test_phase7_failures.py tests/test_accounting_engine.py tests/test_accounting_properties.py tests/test_wallet_event_order.py tests/test_dedup_pairing.py tests/test_worker.py -q -x
DATABASE_URL=postgresql+psycopg://postgres@127.0.0.1:55437/polypulse_phase7_test SECONDARY_DATABASE_URL='' UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync pytest tests/test_phase7_replay.py -q -x
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff check .
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff format --check .
git diff --check
```

Results: **86 passed in 12.33s**. After strengthening the checkpoint interval
boundary assertion, the affected replay suite passed **15 tests in 4.52s**.
Lint passed; **175 files already formatted**; whitespace check exited 0.
The 15-test rerun is a subset, not 15 additional unique tests.

The spaced-checkpoint test observed only condition `a` loaded between snapshots;
at the next checkpoint, untouched condition `b` was also included in the complete
snapshot. A late correction across that interval matched full serial rebuild.
The worker integration cases exercised selective replay with no process pool,
both enabled and disabled, and compared economic state against rebuild.

VERDICT: PASS

## Limits and remaining work

`ACCOUNTING_CHECKPOINT_INTERVAL=1` preserves current cadence. Increasing it reduces
snapshot frequency but increases potential correction replay. Full snapshots and
an individual large condition remain proportional to their state size. This is
not a hard memory cap or a measured performance improvement. Full replay still
saves snapshots per replay batch. Global aggregate suffix scanning remains linear.

The earlier small benchmark predates these fixes; do not label its timings as
measurements of this revision. Independent two-shard concurrency, crash/restore,
proxy failover, ambiguous commit and large-workload gates remain as listed in the
testing brief. Split/merge economic support still requires independent evidence.

Assumptions: none introduced for economic behavior. Noticed but untouched: no
additional unrelated issue. No commit or push was made.
