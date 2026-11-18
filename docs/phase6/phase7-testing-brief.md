# Antigravity independent Phase 7 testing brief

Ready for independent execution after the focused Codex pass recorded in
[verification evidence](phase7-verification.md). Codex owns analysis,
implementation and initial verification. Antigravity
performs independent testing and returns evidence; this brief is not a request to
change production or to implement a competing design.

Use two disposable, separately named PostgreSQL databases and the current working
tree. Record the diff identity, dependency versions, CPU/RAM and database settings.
Never aim the existing integration suite at live wallet databases: some existing
tests commit rebuilds of known wallets. Apply migrations through 0024 to scratch
databases only and initialize their shard ownership using the existing procedure.
Do not print database credentials. Preserve production data and configuration.

## Prepared checks

Run the new cases first against disposable databases:

```sh
uv run pytest tests/test_phase7_conditions.py tests/test_phase7_replay.py tests/test_phase7_snapshot_cache.py tests/test_phase7_websocket.py tests/test_phase7_failures.py -q
```

They cover tokenless and ambiguous redemption, wrong-wallet/condition input,
process result parity and reuse, timeout disposal, selective replay with pending
arrivals, untouched-condition timestamps, supersession, full-rebuild parity and
idempotence, retained pool cleanup, stale session state, failed database owners,
atomic snapshot reads, replay timeout rollback, checkpoint retention and bucket
fairness. They do not replace the adversarial drills below.

Then run the accounting, ordering, enrichment, worker, serving/WS, shard ownership
and restore regressions appropriate to the configured disposable databases.
Report collected and executed tests, skips, failures and exact commands. Separate
fixture setup failures from implementation failures. Never translate a static
lint pass into a runtime pass.

## Required independent adversarial drills

1. Interleave two wallets on one shard and a third on the other. Overlap ingestion,
   parallel projection, manual rebuild and a second worker. Use barriers to force
   overlap; compare each wallet separately with a serial rebuild. Verify that a
   wrong-shard write and wrong-wallet payload roll back without cursor movement.
2. Return reordered, missing, duplicate, wrong-token, wrong-wallet, wrong-attempt
   and stale-position effects through an injected executor. Require transaction
   rollback with identical persisted positions, ledger, rejects, checkpoints,
   totals, revision and cursor. Exercise the legitimate tokenless resolution path.
3. Terminate a child while computing; cancel its coordinator; exhaust its timeout.
   Confirm children are reaped, waiting owners roll back, a new pool progresses,
   and other wallets cannot accidentally publish the failed owner's results.
4. Terminate the advisory-lock PostgreSQL backend both between transactions and
   during computation. Let a replacement owner proceed. Require zero stale writes
   from the old attempt, including a reconnect through the existing connection.
5. Inject failure between ledger insertion, position update, checkpoint save,
   cursor update and commit. Also drop commit acknowledgement. Reconnect and read
   durable progress before retrying; require one economic application only.
6. Deliver multiple late corrections to different conditions, a late event in
   the same input batch, timestamp ties, a correction older than all checkpoints,
   corrupted/version-mismatched condition snapshots and enrichment crossing a
   checkpoint. Include pending normal events in both existing and new conditions,
   unsupported events, rejects and superseded trailing orders. Verify counts and
   exact Decimal results, not only shares. Verify unrelated ledger IDs stay intact.
7. Read REST and WS repeatedly while correction publication is blocked. Each
   response must contain one committed snapshot. Complete old reads after new
   reads and reorder notifications. Revision must not decrease. Reconnect without
   a notification and verify current state. Verify same-order PnL corrections.
8. Disable parallelism and restart after a committed batch and interrupted replay.
   Compare the serial path and rebuild. Check shard backup, copy, ownership guards
   and restore include both new tables. Keep old writers quiescent during rollout.
9. Inject transient and sustained WebSocket state-read failures. Require delivery
   after recovery or explicit close code 1013 after three failed attempts. Overlap
   notifications with an in-flight read and shutdown with a scheduled retry.
10. Run selective corrections with `ACCOUNTING_PARALLEL_PROCESSES=0` and only
    `ACCOUNTING_CONDITION_REPLAY_WALLETS` configured. Compare with full rebuild.
    Increase `ACCOUNTING_CHECKPOINT_INTERVAL`, force corrections between snapshot
    boundaries and verify snapshots still contain untouched conditions. Measure
    snapshot peak memory separately from batches that do not save checkpoints.

## Measurements

The prepared CPU benchmark uses synthetic events, **not Polymarket public data**:

```sh
uv run python -m scripts.phase7_compute_benchmark --events 100000 --conditions 100 --processes 2 --repeats 3
uv run python -m scripts.phase7_compute_benchmark --events 100000 --conditions 1 --processes 2 --repeats 3
```

It compares serial, one-process and multi-process effects, emits a deterministic
digest and observed process intervals. It excludes SQL, ingestion and API latency;
do not label its rate end-to-end throughput. Record whether distinct child PIDs
actually overlap. A single-condition control is expected to lack useful parallelism.

Use the Phase 6 scratch-shard workload separately for its million-event ingestion
gate. For Phase 7 database measurements use interleaved conditions on a hot wallet,
including a one-condition control. Record projection and transaction duration,
condition events replayed, aggregation rows scanned, checkpoint bytes, parent and
child memory, database size and concurrent API p95. Compare identical event sets
and repeat runs; include warm-up and process-start overhead explicitly. The
current Phase 6 workload assigns one condition per wallet, so its numbers alone
cannot demonstrate within-wallet speedup.

The database workload is now implemented. On an empty, migrated, disposable
database named exactly `polypulse_phase7_bench`, with `SECONDARY_DATABASE_URL=''`:

```sh
uv run python -m scripts.phase7_database_benchmark --events 100000 --conditions 100 --batch-size 1000 --processes 2 --repeats 3
```

Save output, then use a fresh empty benchmark database for the same command with
`--conditions 1`. The script refuses a populated database and leaves rows for
inspection. Its API samples use in-process ASGI, so proxy/network failover still
requires the separate two-API deployment tests. Memory peaks are process-lifetime
values and database size accumulates prior runs; they are not per-mode deltas.

Return: exact commands and environment, assertions and actual results, failures
with minimal reproduction, measured distributions, and remaining gates. Do not
enable parallelism by default or claim split/merge economic support.
