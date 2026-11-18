# Phase 7 implementation and release boundary

Implementation is opt-in. Focused Codex runtime verification has passed on an
isolated PostgreSQL cluster; comprehensive independent release tests remain open.
Migrations through 0024 were applied only to disposable databases. See
[verification evidence](phase7-verification.md). No live worker was started.

## Publication and isolation

The owning shard's wallet total row serializes ingestion, supersession, projection
and rebuild. Ingestion additionally takes its transaction advisory lock and cursor
row before the total row; projection never takes that ingestion lock in reverse.
The worker's session advisory lock stays on the same physical connection as its
writes. A replacement connection aborts that attempt before SQL execution.
Lock and statement timeouts bound database waits; errors roll back the transaction.

CPU children receive only events and position snapshots. They return an attempt
identity, wallet, condition, ordered effects and process timing intervals. They do
not receive sessions or write permission. A shared bounded pool allows at most its
configured process count of submitted tasks. Timeout, cancellation or failure
terminates that pool generation; affected coordinators roll back. Subsequent
attempts start fresh processes. A process failure can temporarily affect other
wallets sharing the pool; it never permits their partial publication.

The coordinator validates event coverage, ownership, token resolution, prior
position state, disposition cardinality and resulting position identity, then
folds effects in reference order. Missing conditions use serial computation.
One-condition workloads remain serial through the projector. The independent
CPU benchmark deliberately also measures process overhead for that control.

Positions, ledger, rejects, totals, checkpoints and applied progress share a
transaction. Notifications are enqueued there and delivered by PostgreSQL after
commit. Repeatable-read API/WS sessions bypass the global valuation micro-cache
so payload and accounting revision belong to the same snapshot. A separate market
snapshot still has independent price freshness. WebSocket publication revisions
prevent an older accounting snapshot from replacing a newer one.

WebSocket refreshes retain pending notifications across read failures and retry
three times with increasing delays. Reads and sends have a 10-second timeout.
After repeated read failure the server closes affected sockets with code 1013,
so stale clients are not left apparently healthy. Shutdown cancels retries without
recreating delivery tasks. Notifications arriving during a read remain pending.

## Corrections and complexity

Migration `0024_condition_replay` adds version-1 condition snapshots, earliest dirty
boundaries, shard guards and a wallet/condition/time source index. The shard-copy,
ownership verification and backup lock inventory includes both new tables.

Late arrivals and applied-event supersession record invalidation in the same
transaction as source changes. Supersession records both replaced and surviving
observations. Existing unbounded corrections remain unbounded and trigger rebuild.

Selective replay chooses a wallet checkpoint strictly before the earliest dirty
boundary. Condition snapshots must match that checkpoint's state and format.
Conditions with normally ordered but unprojected arrivals are also included;
otherwise advancing a wallet cursor could skip those arrivals. Only these
conditions' positions and suffix ledger/reject rows are rebuilt. Other condition
positions, ledger identities and timestamps stay unchanged.

Wallet aggregates start at the common checkpoint, then fold every active suffix
event's ledger delta in reference order using Decimal. Unsupported events and
rejects contribute their respective counts. Missing/conflicting dispositions
abort publication. Covered superseded observations advance progress without an
economic effect. Newer checkpoints are invalidated atomically and a corrected
final checkpoint is saved. Missing markers, unknown conditions or incompatible
condition snapshots use wallet replay.

Cost is **O(affected condition suffix calculation + global aggregate suffix scan)**,
not constant time. `events_processed`, `conditions_replayed` and
`aggregation_rows_read` distinguish these costs. Snapshot serialization still
scales with wallet position count. Snapshots currently duplicate condition state
inside wallet checkpoints for rollback compatibility. Retention keeps the latest
`ACCOUNTING_CHECKPOINT_KEEP` wallet boundaries (default 32), deleting older wallet
and condition snapshots in the same transaction. Measure their footprint before
prolonged operation. An old correction without a
usable snapshot can still require a full rebuild.

Projection loads only the selected conditions' positions. Full-wallet state is
loaded when saving a checkpoint. `ACCOUNTING_CHECKPOINT_INTERVAL` sets the minimum
applied-order advance between normal checkpoints; default 1 preserves the existing
cadence. Raising it amortizes full snapshot work across batches at the cost of a
longer correction suffix. The first batch always saves a checkpoint, as do replay
batches and completed selective corrections. This is not a hard memory bound:
each full checkpoint and a very large individual condition can still be large.

`ACCOUNTING_ATTEMPT_TIMEOUT_SECONDS` (default 120) bounds a projection attempt;
`WALLET_CYCLE_TIMEOUT_SECONDS` (default 180) lets the bucket scheduler move to
another wallet after cancellation and rollback. These are cooperative async
deadlines, not hard real-time CPU preemption. A correction exceeding its budget
remains pending and needs a measured maintenance budget; it never publishes a
partial replay. Process retirement is retained independently of caller cancellation.

## Split/merge evidence and decision

Inspected local evidence: `fixtures/sample_merge_event.json`, the matching trade
in `fixtures/appletiiime_page_1.json`, `app/chain/decoder.py`, and decision notes
002 and 006. The merge sample has amount and condition but no token legs or prior
cost state. Decision 006 identifies a split stakeholder that is neither tracked
wallet; a transaction containing a split does not establish a wallet-owned split.
The decoder preserves raw logs but its normalized split/merge record does not
provide resolved child tokens, collateral identity or parent collection semantics.

The retrieved [split receipt](phase7-split-receipt.json) confirms that stakeholder,
a zero parent collection, partition `[1, 2]`, collateral
`0x2791bca1f2de4661ed88a30c99a7a9449aa84174`, and raw amount 385500000.
This does not supply the tracked wallet's before/after cost state or golden PnL.
The documented merge receipt was unavailable from the two RPC providers tried
(PublicNode returned an HTTP error; 1RPC returned no receipt). Its absence is not
evidence that no merge happened.

**No split/merge economic support is enabled.** They remain raw, counted
unsupported events in both serial and parallel paths. This satisfies the plan's
evidence gate; it does not claim complete Polymarket accounting. Automatically
minting/burning both outcomes from these records could double-count a trade or
assign another participant's position to a wallet.

Before enabling a shape, capture its receipt and wallet transfers, establish
collateral and parent collection, resolve every token leg, establish before/after
balances and a documented cost-allocation rule, and supply independent golden
ledger/PnL values. A multi-leg ledger identity migration is needed before emitting
several rows for a source event. These missing observations are explicitly not
replaced with fabricated fixtures or assumed equal cost allocation.

## Rollout and rollback

Keep `ACCOUNTING_PARALLEL_PROCESSES=0` until all release gates pass. Stop existing
writers, back up both shards, apply migrations through 0024 to both, and deploy
the same code version to APIs, workers and maintenance scripts. The new writer
requires the new tables even when CPU parallelism is disabled. Do not mix old
writers that cannot maintain condition invalidations with enabled Phase 7 workers.

After tests, opt in a scratch wallet with `ACCOUNTING_PARALLEL_WALLETS` and a small
process count. Two shard Worker objects have separate process pools, so total CPU
children are the sum across shard workers and worker instances. Measure this on
the local machine rather than assuming its core or memory capacity.

`ACCOUNTING_CONDITION_REPLAY_WALLETS` independently opts wallets into selective
replay, including with process count zero. Existing parallel-wallet opt-in also
continues to enable selective replay. Clear the independent list as well as the
process count to return entirely to wallet-level replay.

Disable the process count and restart workers to return to serial computation.
With the independent selective replay list empty, the wallet replay path reads
compatible wallet checkpoints and clears condition metadata when
rebuilding. Reverting to older binaries requires quiescing writers and rebuilding
derived state with the intended version. Never downgrade a live schema as a crash
recovery method. PostgreSQL rollback plus durable cursors handle interrupted work;
backups address database loss and require a separate restore drill.
