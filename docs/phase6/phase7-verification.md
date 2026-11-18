# Phase 7 focused verification — 2026-11-16

Codex completed the audit follow-up and focused verification. Antigravity's
comprehensive independent end-to-end release pass is still required. No live
database was migrated and parallel accounting remains disabled by default.

## Changes in this follow-up

| File | Change |
| --- | --- |
| `app/ingestion/writer.py` | Refresh locked cursor and supersession objects instead of trusting cached ORM state. |
| `app/accounting/executor.py` | Retain and await process retirement when the requesting task is cancelled. |
| `app/accounting/deadline.py`, `app/accounting/projector.py` | Bound projection attempts; transaction owners roll back on expiry. |
| `app/accounting/checkpoints.py` | Retain the latest configured checkpoint boundaries atomically. |
| `app/worker.py` | Time out a wallet cycle, roll back and allow the next bucket member to run. |
| `app/config.py`, `.env.example` | Attempt/cycle deadlines and checkpoint count settings. |
| `tests/test_phase7_conditions.py`, `tests/test_phase7_replay.py` | Pool retirement, stale-cursor, malformed-output, timeout and process projection regressions. |
| `tests/test_phase7_failures.py`, `tests/test_phase7_websocket.py` | Database termination, atomic reads, retention fallback, bucket fairness and reordered snapshots. |
| `scripts/phase7_database_benchmark.py` | Guarded synthetic SQL ingestion/projection/correction workload with concurrent ASGI reads. |
| Phase 7 plan, progress, implementation and testing documents | Current evidence, limits and independent test handoff. |
| `phase7-split-receipt.json`, `phase7-database-smoke.jsonl` | Retrieved receipt and measured benchmark output. |

## Environment and isolation

Python 3.12.3, PostgreSQL 16.15, SQLAlchemy 2.1.8, psycopg 3.3.8,
FastAPI 0.162.4, pytest 9.2.0; Linux, 4 logical CPUs.
A fresh native PostgreSQL cluster at `/tmp/polypulse-phase7-pg`, TCP port 55437,
hosted `polypulse_phase7_test` and `polypulse_phase7_bench`. Both databases used
UTF8 and were migrated from empty through 0024. `SECONDARY_DATABASE_URL=''`
explicitly prevented connections to any configured secondary wallet database.
The locally installed `docker` command is a native-service wrapper, so it was
not used for test isolation. Tests used the existing locked dependencies.

All commands below used `UV_CACHE_DIR=/tmp/polypulse-uv-cache` and `uv run --no-sync`.
Database commands set `DATABASE_URL` to the applicable disposable database on
127.0.0.1:55437 and `SECONDARY_DATABASE_URL=''`.

## Executed checks

```sh
uv run --no-sync alembic upgrade head
uv run --no-sync pytest tests/test_phase7_conditions.py tests/test_phase7_replay.py tests/test_phase7_snapshot_cache.py tests/test_phase7_websocket.py tests/test_phase7_failures.py tests/test_accounting_engine.py tests/test_accounting_properties.py tests/test_wallet_event_order.py tests/test_dedup_pairing.py tests/test_worker.py -q -x
uv run --no-sync python -m scripts.phase7_database_benchmark --events 2000 --conditions 20 --batch-size 500 --processes 2 --repeats 1
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
git diff --check
```

Migrations succeeded on both scratch databases. Final focused suite:
**79 passed in 9.96s**. Ruff: **All checks passed!** Formatting: **174 files
already formatted**. Diff whitespace check: exit 0.

An initial test collection failed because a helper import omitted the `tests`
package prefix; it was corrected before the passing run. An overlong benchmark
string was wrapped before the final lint pass. Restricted-process execution
could not support the local database/process pool; the successful tests used
the permitted local execution environment. These were not production failures.

VERDICT: PASS

## Measured small benchmark

See [raw JSONL](phase7-database-smoke.jsonl). All three modes had identical economic
digests and matched full serial replay after correction. All 148 sampled API
responses were HTTP 200. The two-process mode recorded 11 overlapping task
intervals from two distinct child PIDs.

| Process count | Ingest + project seconds | Correction seconds | Full replay seconds | API p95 ms |
| --- | ---: | ---: | ---: | ---: |
| 0 | 3.332 | 0.112 | 0.953 | 383.2 |
| 1 | 4.336 | 0.097 | 1.233 | 139.9 |
| 2 | 4.306 | 0.097 | 1.027 | 362.3 |

Every correction computed **26 events in one condition**, scanned **501 rows**
for aggregate reconstruction, and matched a full **2,001-event** replay. Saved
position JSON occupied 24,611 bytes per wallet before full rebuild. Two-process
execution was slower overall at this size. This demonstrates overlap and limited
correction work on this dataset, not scalable throughput or a general speedup.

Limits: one repeat; 2,000 synthetic input events, not Polymarket public data;
single scratch shard; in-process ASGI, not proxy/network latency; resource peaks
are process-lifetime values; database bytes accumulate earlier mode runs;
full-replay timing includes the parity digest query. API sampling spans ingestion,
projection, correction and rebuild. No statistical performance conclusion follows.

## Remaining release gates

Use [the independent testing brief](phase7-testing-brief.md). Outstanding evidence
includes simultaneous multi-wallet/two-shard ownership and workers, proxy failover,
lost commit acknowledgements, all publication failure boundaries, real child
termination, backup/restore and rollout drills, repeated larger workloads,
single-condition performance control and the million-event ingestion gate.

Split/merge economic mutations remain unsupported. A real split receipt was
retrieved and inspected; its stakeholder is not either tracked wallet. The merge
receipt was unavailable from the providers tried. Golden before/after balances
and cost allocation are still missing; no accounting rule was fabricated.

Projection deadlines are cooperative. Snapshot count is bounded, but snapshot
bytes scale with positions. Large old corrections can still need full replay and
may exceed the configured deadline; they remain pending until a suitably budgeted
attempt succeeds. Global aggregate suffix reconstruction remains linear.

Assumptions: none used to invent economic behavior. Static code review and focused
tests cannot prove absence of all concurrency bugs. No commit or push was made.
