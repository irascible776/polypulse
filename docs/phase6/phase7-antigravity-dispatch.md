# Antigravity assignment: independent Phase 7 execution

User authorized this testing handoff. Repository: `/home/utkarsh/Desktop/gravia`.
Codex retains all implementation and critical fault investigation. Test independently;
do not edit application code, existing tests, configuration, migrations or dependencies.
Temporary test harnesses and result artifacts outside the source tree are allowed.
Do not commit, push, deploy, alter live databases or enable live parallelism.

Read `docs/phase6/phase7-testing-brief.md`, `phase7-review-fixes.md` and
`phase7-implementation.md`. The brief is the full release checklist; this assignment
splits ownership rather than removing any gate.

## Your execution scope

1. Independently rerun the focused suites and applicable legacy accounting,
   ingestion, serving and shard tests. Record failures and skipped setup, not just
   a combined pass count. Add external reproductions where coverage is missing.
2. Run two API instances behind the existing self-hosted HAProxy configuration.
   Exercise REST/WS reconnect, stop one API, restore it and inspect actual routing.
   Test transient and persistent WS read failure and close/reconnect behavior.
3. Exercise two-shard routing and wrong-shard rejection, serial selective replay,
   checkpoint spacing, backup/restore into disposable targets, and switching back
   to serial operation. Check both new condition tables survive copy and restore.
4. Execute repeated CPU and database benchmarks with diverse conditions and the
   single-condition control. Proceed from small smoke to larger datasets. Run the
   Phase 6 million-event gate if resources permit; report resource limitations
   rather than substituting a smaller run and calling it a pass.
5. Return reproducible evidence and any newly discovered concerns. Do not fix code.

Codex owns the targeted lost-commit-acknowledgement, overlapping correction and
publication, physical-session fencing and actual process-loss investigations.
Independent confirmation is welcome; coordinate findings through the report.

## Isolation and reproducibility

- Test a frozen copy of the current dirty working tree, including untracked source
  files. A git checkout alone omits the Phase 6/7 implementation. Record SHA256
  hashes for copied app, scripts, tests and migrations before running. Do not copy
  `.env` or credentials. Use installed dependencies without changing the lockfile.
- Codex reserves `/tmp/polypulse-phase7-pg` and port 55437. Create your own disposable
  cluster, for example under `/tmp/polypulse-antigravity-phase7`, on unused ports.
  Explicitly override both database URLs for every process, including API/worker
  children. Confirm actual database identity before any migration or fault drill.
- The installed `docker` command is a native production-service wrapper, not real
  Docker. Inspect it before use; do not use it to create presumed isolated containers.
  Native PostgreSQL 16 binaries are installed under
  `/home/utkarsh/.local/share/postgresql-16/usr/lib/postgresql/16/bin` and need
  `LD_LIBRARY_PATH=/home/utkarsh/.local/share/postgresql-16/usr/lib/x86_64-linux-gnu`.
- The database benchmark requires a fresh database named `polypulse_phase7_bench`
  and no secondary URL; create it in your own cluster. Do not reset Codex databases.
- Use exact Decimal comparisons, stable economic keys, durable cursors/revisions
  and explicit failure assertions. Do not count HTTP 200 as accounting correctness.
- Do not run CPU/load measurements concurrently with Codex stress work. Record
  competing processes/load if unavoidable and mark those timings contaminated.

## Deliverable

Write results to `/tmp/polypulse-antigravity-phase7-report.md` and put logs/harnesses
in `/tmp/polypulse-antigravity-phase7-results/`. Include: source hashes, environment,
exact commands, test counts, expected versus actual assertions, minimal failures,
raw benchmark files, scope limitations and PASS/FAIL/NOT RUN per gate. Report any
missing tool/runtime as blocked for that case; continue unaffected tests.

The previous 86-test pass is context, not evidence from your run. Full snapshots
remain wallet-size dependent, and split/merge accounting remains unsupported.
