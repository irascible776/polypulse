# Phase 7 implementation tracking

Policy: planning=deep; context_depth=dependency; verification_depth=impact;
delegation=none. Reasons: accounting state, concurrent publication, additive schema.
The user authorized focused Codex runtime testing after the audit; Antigravity
will perform the comprehensive independent end-to-end pass afterward.

## Audit follow-up checklist

Second review follow-up (same policy; no schema changes):

1. [x] Retain failed WebSocket refreshes, bound retries, explicitly close on exhaustion.
2. [x] Add independent selective-replay opt-in and configurable checkpoint spacing.
3. [x] Load only selected conditions between full checkpoints; preserve full snapshot state.
4. [x] Verify regressions and update evidence; comprehensive release testing remains open.

Second-review results: [86-test pass and remaining limits](phase7-review-fixes.md).

1. [x] Refresh locked ingestion state and make process cleanup cancellation-safe.
2. [x] Bound wallet scheduling and projection attempts without publishing partial replay.
3. [x] Add adversarial regression cases and a database benchmark.
4. [x] Run focused tests on disposable databases; fix failures and record evidence.
5. [x] Record split/merge evidence gaps and prepare the independent handoff.

1. [x] Implement correction markers, versioned condition snapshots and selective replay.
2. [x] Bound parallel submissions and validate returned state before persistence.
3. [x] Review local split/merge evidence; retain unsupported disposition per the evidence gate.
4. [x] Prepare regression cases and independent testing instructions; run static checks.

These boxes track implementation work, not runtime acceptance. No parity,
throughput, failover or migration pass is claimed by checking an implementation item.

Decision: store correction markers in the ingestion transaction and retain wallet
checkpoints as the exact aggregate baseline. Recompute affected conditions only,
then fold all suffix ledger effects in original order. This reduces position
calculation, but the aggregate suffix scan remains linear. Missing or incompatible
snapshots fall back to wallet replay. Migrations were subsequently applied to disposable databases only.

## Files changed in the continuation

| File | Change |
| --- | --- |
| `app/accounting/checkpoints.py` | Transaction timeouts, commit-bound notifications, snapshot storage and earliest dirty markers. |
| `app/accounting/executor.py` | Bounded spawned CPU workers, attempt identities, generation termination and reuse. |
| `app/accounting/projector.py` | Selective correction replay, aggregate folding, complete progress and strict effect validation. |
| `app/accounting/engine.py` | Validate incoming position map keys against their declared ownership. |
| `app/ingestion/writer.py` | Record late/superseded condition boundaries under the accounting lock. |
| `app/storage/models.py` | Condition snapshot and dirty-marker models plus source lookup index. |
| `app/storage/migrations/versions/0024_condition_replay.py` | Additive schema and shard guards; applied to scratch databases only. |
| `scripts/initialize_wallet_shards.py` | Include new tables in copy, ownership verification and backup lock inventory. |
| `app/worker.py` | Use bounded executor and opt-in selective replay. |
| `app/storage/database.py` | Tag repeatable-read accounting sessions. |
| `app/valuation/engine.py` | Bypass shared valuation cache for tagged snapshot reads. |
| `app/config.py`, `.env.example` | Document opt-in controls and bounded database waits. |
| `tests/test_phase7_conditions.py` | Prepare pure computation, process parity and timeout cases. |
| `tests/test_phase7_replay.py` | Prepare correction/rebuild parity, supersession and untouched-condition cases. |
| `tests/test_phase7_snapshot_cache.py` | Prepare regression for cross-transaction cache mixing. |
| `scripts/phase7_compute_benchmark.py` | Prepare synthetic CPU-only comparison; no measurement claimed. |
| `docs/PHASE_7_PLAN.md` | Update implementation status while retaining open runtime gates. |
| `docs/phase6/phase7-implementation.md` | Record architecture, complexity, evidence boundaries and rollout rules. |
| `docs/phase6/phase7-testing-brief.md` | Independent test instructions for after testing authorization. |

## Static evidence (2026-11-16)

Commands executed after implementation:

```sh
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff check .
UV_CACHE_DIR=/tmp/polypulse-uv-cache uv run --no-sync ruff format --check .
git diff --check
```

Results: lint reported `All checks passed!`; formatting reported `170 files already
formatted`; diff whitespace check returned exit 0 with no output.
The initial default UV cache was read-only; using the temporary cache resolved
that environment issue without changing dependencies.

Scope: static lint, formatting and diff whitespace only.
VERDICT: PASS

The paragraph above records the earlier static-only stage. The subsequent
[focused runtime verification](phase7-verification.md) passed 79 tests and a small
database benchmark. Comprehensive release acceptance remains open, and default
parallelism remains disabled.
Split/merge economic support remains unsupported because local evidence cannot
establish token legs and cost rules. There are no newly assumed accounting rules.
