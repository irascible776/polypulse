# Independent Phase 6 verification

Test the current PolyPulse implementation independently. Do not change application
code, migrations, configuration, tracked files, or the user's existing data. Do not
commit or push. Use only newly created, clearly named scratch PostgreSQL databases;
never use the production database or the `polypulse_scale_*` benchmark databases.
Avoid modifying existing checkpoint files. Preserve evidence and report the exact
commands, database targets, results, and failures. Do not claim unrun tests passed.

Review the repository's documented setup and test commands first, then independently
exercise the following where the environment permits:

1. Apply every migration on fresh databases, initialize the two wallet shards, and
   verify each database rejects writes for a wallet owned by the other. Verify both
   wallet reads and chain-event/checkpoint routing.
2. Run multiple workers for the same and different wallets, including terminating
   a worker or its database connection mid-processing. Verify no duplicate or lost
   events, no cross-wallet contamination, and safe continuation by another worker.
3. Compare late-event checkpoint replay with full replay, including buys, sells,
   redemptions, simultaneous timestamps, duplicate/enriched events, and events on
   different tokens. Verify positions, totals, ledger, and rejection records; report
   split/merge accounting as unsupported if that remains the implementation.
4. Verify COPY and chunked insertion have identical results and roll back cleanly;
   exercise backfill interruption around a timestamp shared across pages.
5. Verify price partition maintenance, archive and hourly roll-up results, exact
   historical reads, and rollback. Check both a late tick and retention boundary.
6. With two API instances behind the configured proxy, verify API and WebSocket
   behavior and failover when one API instance stops. Test shard failures separately
   and record which reads remain available. Do not claim proxy redundancy.
7. Take the coordinated backup of the scratch shards, restore both to fresh scratch
   databases, verify shard ownership, and compare wallet rebuilds and market data.
   Report any write blocking and the absence of off-host backup if applicable.
8. Run the documented test suite and lint/format checks. If resources permit, run
   the synthetic benchmark on separate scratch databases and report actual timing,
   machine conditions, worker counts, correctness checks, and API latency.

Report findings ordered by severity with file paths and line numbers. Clearly separate
confirmed failures, passing tests, environmental limitations, and untested items.
