# Phase 7: hot-wallet accounting assessment

This is a proposal, not a Phase 6 implementation or a claim of parallel-wallet
correctness. Phase 6 keeps one ordered writer for each wallet. The code references
below describe the current implementation; the design choices remain to be proven.

## Current dependencies

- `app/accounting/engine.py` calculates a buy's new average cost from the prior
  shares and cost. A sell uses that cost, rejects overselling, and updates realized
  PnL. A redeem closes the position and uses its remaining cost. Events affecting
  the same position must therefore be applied in deterministic order.
- `app/accounting/projector.py` resolves a redeem without a token by inspecting
  open positions for the wallet and condition, optionally using outcome index.
  Concurrent changes to candidate positions can change whether the redemption is
  unmatched, ambiguous, or uniquely resolvable. A token-only queue is insufficient
  for this case without a coordination rule.
- The same projector increments wallet-wide PnL, trade/redeem/unsupported/reject
  counts and a single applied-order cursor. It writes derived positions, ledger,
  rejects and checkpoint state together. Concurrent token workers cannot safely
  each advance that single cursor independently.
- Full replay orders active observations by `(event_time, api_order)`; normal
  catch-up follows `api_order`, with late arrivals triggering replay. Superseded
  observations are preserved but excluded. A parallel design must preserve the
  current deterministic economic result across late arrivals and enrichment, not
  merely the order in which messages happen to arrive.
- `SPLIT` and `MERGE` are currently counted as unsupported. Their decoded presence
  does not establish accounting semantics or prove safe cross-token independence.

## Necessary Phase 7 work

1. Define and document event dependency keys and ordering, including tie-breaking,
   tokenless redemptions, supersession, and the treatment of split/merge events.
   Establish expected ledger and PnL outcomes using real, independently checked
   examples before implementing additional event types.
2. Prototype partitioning work by position or condition while keeping each wallet
   on its existing database shard. Introduce explicit barriers or a wallet/condition
   coordinator for events whose affected positions are unknown or span partitions.
   Compare different designs; do not assume token-only routing suffices.
3. Design durable per-partition progress, versioned checkpoints, correction
   invalidation and a wallet-wide committed watermark. Specify how an atomic,
   consistent read combines position results and aggregate totals. Choose schema
   changes only after the coordination and recovery rules are established.
4. Define idempotent ownership, fencing against stale workers, retry and crash
   recovery. An old worker must not publish results after its ownership expires.
   Decide whether the existing PostgreSQL coordination is adequate; a queue is a
   separate design choice and does not remove the need for these guarantees.
5. Prove equivalence with the current deterministic full replay for ledger,
   rejects, positions, totals and PnL. Exercise interleavings, timestamp ties,
   late events, duplicate/enriched observations, missing or ambiguous redeem
   tokens, multi-position effects, worker loss and crashes during publication.
   Benchmark a single hot wallet and cross-wallet throughput on representative data.

## Beyond Phase 7 unless separately justified

Live bucket migration, a highly available proxy tier, managed deployment,
off-machine scheduled backups, and an alternative queue or analytical database
remain separate operational or architecture projects. None is required merely to
demonstrate a safe hot-wallet accounting prototype.
