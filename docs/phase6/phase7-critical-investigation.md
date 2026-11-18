# Critical consistency investigation and testing split

Date: 2026-11-16. Policy: planning=deep; context_depth=dependency;
verification_depth=impact; delegation=Antigravity independent testing, explicitly
requested by the user. Codex owns investigation, implementation and verification.

1. [x] Prepare independent assignment with isolated resources and frozen source.
2. [x] Submit through the documented local Antigravity chat CLI.
3. [x] Investigate commit uncertainty, publication/ingestion contention, snapshot
   fallback and child failure with targeted reproductions.
4. [x] Run focused regressions and document measured outcomes and remaining gates.

## Handoff status

The CLI `antigravity chat --help` documents prompt submission. The assignment in
`phase7-antigravity-dispatch.md` was submitted using `antigravity chat --mode agent
--reuse-window --add-file` with that document and an explicit execution request.
The command exited 0. This proves submission, not that the agent has begun or
completed execution. No report was present at the last check of
`/tmp/polypulse-antigravity-phase7-report.md`.

Antigravity owns independent regressions, REST/WS and proxy failover, two-shard
routing, backup/restore, serial switch-back and repeatable load measurements.
It must not edit implementation. It uses a frozen copy and its own disposable
cluster, avoiding Codex's reserved cluster on port 55437. No benchmark from two
simultaneous workloads should be presented as isolated performance evidence.

## Codex evidence

Added `tests/test_phase7_critical.py` with seven cases:

| Case | Observed assertion |
| --- | --- |
| Connection lost before commit | Retry reads durable progress and applies one event once. |
| Commit succeeds but caller receives a connection error | Retry processes zero events; all derived rows, checkpoints, dirty markers, cursor and revision remain identical. |
| Late ingestion overlaps publication | PostgreSQL reports the writer waiting on a lock; after publication it records the correction, and selective replay matches full rebuild. |
| Missing condition snapshot | Wallet replay fallback produces the expected balances and exact Decimal PnL. |
| Unsupported snapshot version | Same fallback and economic parity. |
| Corrupted condition snapshot state | Same fallback and economic parity. |
| Actual test-owned child terminated | The assigned task is confirmed inside the child by a barrier marker, then killed; timeout retires the generation and fresh work succeeds. |

The checkpoint cases independently assert 3 remaining shares, average cost 0.5
and realized PnL -0.5, then compare against a full serial rebuild. Missing or bad
condition metadata is tested against an intact wallet checkpoint; this is not
proof of recovery from simultaneous corruption of both checkpoint representations.

Commit uncertainty is injected at the psycopg commit boundary: close the actual
connection before or after the real database commit, then raise OperationalError.
This tests the application's durable-state retry behavior. It is **not** a packet
proxy dropping a PostgreSQL COMMIT response. The child drill kills an assigned
task at a test barrier, not a specific instruction inside the arithmetic engine.

Initial harness failures were corrected: a synthetic connection error originally
left a live transaction on the connection, unlike the intended disconnect model;
the harness now actually closes it. A Decimal expected-value assertion initially
compared string scale and was corrected to exact Decimal numeric equality. These
were test-model/assertion defects, not confirmed new accounting defects.

The existing worker physical-session-loss and database-owner replacement cases
also ran in the regression suite. Code tracing confirms wallet writes stay on
the physical advisory-lock connection; the before-execute guard rejects a new
driver connection. Writer correction markers are recorded under the same wallet
total lock as publication, and PostgreSQL notifications are transactional.

## Executed verification

On the existing disposable `polypulse_phase7_test` database, port 55437:

```sh
DATABASE_URL=postgresql+psycopg://postgres@127.0.0.1:55437/polypulse_phase7_test SECONDARY_DATABASE_URL='' UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync pytest tests/test_phase7_critical.py tests/test_phase7_conditions.py tests/test_phase7_replay.py tests/test_phase7_snapshot_cache.py tests/test_phase7_websocket.py tests/test_phase7_failures.py tests/test_accounting_engine.py tests/test_accounting_properties.py tests/test_wallet_event_order.py tests/test_dedup_pairing.py tests/test_worker.py -q -x
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff check .
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff format --check .
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff check tests/test_phase7_critical.py
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff format --check tests/test_phase7_critical.py
git diff --check
```

Final suite: **93 passed in 23.29s**. Whole-tree lint and formatting passed
(178 Python files); the final Decimal assertion change was followed by successful
targeted lint/format checks. Diff whitespace passed. No app code, schema,
dependencies or live configuration changed during this investigation.

VERDICT: PASS

## Still open

Antigravity execution/results are not yet confirmed. Full independent two-shard
and proxy/restore testing, network-level commit-response loss, broader failure
interleavings and large repeated benchmarks remain release gates. Split/merge
accounting evidence and snapshot-size limitations remain unchanged. Seven passing
fault cases are not proof that every possible concurrency failure is handled.

Assumptions: none introduced for accounting behavior. Noticed but untouched:
no unrelated issue reported. No commit or push was made.
