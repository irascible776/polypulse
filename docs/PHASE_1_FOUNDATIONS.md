# Phase 1: Foundations

Read PROJECT_BRIEF.md and STUDY_GUIDE_TEMPLATE.md first. Follow the AI rules in
the brief. Stay inside this phase's scope.

## Goal (plain words)
Get a clean project that starts with one command, and learn how the real
Polymarket interfaces actually behave BEFORE building on them.

## In scope
Repo skeleton, config, Postgres in Docker, one migration, health endpoints, two
spike scripts (plus one optional probe), three decision notes, one study guide.

## Out of scope
Any real ingestion, event tables, accounting, indexes, or dashboard logic.

## Ask me BEFORE coding
- Which wallet should the spike use? Propose one active proxy wallet from
  Polymarket's official docs or a public leaderboard and wait for my approval.
- Package tool (uv, or pip with a lockfile)? Is Docker installed?
- Any dependency not listed in the brief needs a one-line reason first.

## 1.1 Repo skeleton
- Python 3.12. Dependencies as listed in the brief. Pin exact versions in a lockfile.
- Layout: app/{ingestion,accounting,storage,serving,replay}, scripts/, tests/,
  docs/{study,decisions}, fixtures/.
- Makefile targets: run, test, lint, migrate, db-up.
- Also: .env.example, .gitignore, and a README with run steps.

## 1.2 Config, database, health
- Settings are loaded from environment variables. No secrets in code.
- docker-compose runs Postgres 16 only.
- GET /health (app is up) and GET /ready (database reachable).
- Tests: settings load; /health returns 200; /ready fails cleanly when the DB is down.

## 1.3 First migration
- Create ONLY tracked_wallets: normalized wallet address (lowercase, unique), label,
  tracking_start, status.
- Add a function that validates and normalizes an address (0x + 40 hex characters).
- Tests: accepts good addresses, rejects bad ones, and normalizing twice gives the
  same result.

## 1.4 Data API spike (scripts/spike_activity.py)
- Find the exact endpoint and parameter names in the OFFICIAL docs. Do not guess.
- Fetch one wallet's full history (start=1, ascending) and follow the cursor to the
  end. Be polite: one request at a time, a short pause, obey 429 and Retry-After.
- Save every raw page exactly as received, with a fetched_at time, into fixtures/.
- Print: pages, total events, count per event type, first and last event time, any
  repeated events across pages, request timings.
- Try one call WITHOUT a token and record the result.
- Find the data-freshness endpoint in the docs, call it, and record the answer.
- Settle the brief's open questions 1, 2, 3 and 6:
  1. Which field(s) uniquely identify an event?
  2. Is a transaction hash and log index exposed?
  3. Is a token needed?
  6. Is the wallet a proxy wallet?
- Tests: the cursor loop is tested with a mocked client (multi-page, empty page,
  error then retry, repeated cursor).

## 1.5 WebSocket spike (scripts/spike_market_ws.py)
- Find the subscribe message format in the official docs.
- Subscribe to ONE active market asset (find one through the documented
  market-metadata API).
- Log 60 seconds of raw messages to a file and print counts per event type.
- Note whether a keep-alive is required. Drop the connection once on purpose and
  reconnect.

## 1.6 Optional: RPC limits probe (scripts/spike_rpc_limits.py)
Only if I have supplied POLYGON_RPC_URL. Otherwise skip it and list open question 4
as TODO(verify). Record: the latest block number, the largest eth_getLogs block
range accepted on a small known range, error messages for oversized ranges, and
rate-limit behavior. Do not hardcode any provider URL.

## 1.7 Decision notes (docs/decisions/)
- 001-api-behavior.md: what the APIs REALLY did: auth, pagination, page size, rate
  limits, field names, one sample JSON of each event type, surprises, and anything
  that contradicts the brief.
- 002-accounting-boundary.md: DRAFT only: which event types are supported, and which
  are saved and counted but not processed.
- 003-architecture.md: one page copied from the brief, kept in sync.

## Definition of done
- One command starts the app and the database. `make test` and `make lint` pass.
- Raw fixtures exist, and 001 states observed facts, not documentation claims.
- Every TODO(verify) is either resolved or listed in 001.

## Study guide task
Write docs/study/01-foundations.md following STUDY_GUIDE_TEMPLATE.md: the folder
layout and why, config, health endpoints, the migration, the cursor-pagination
loop, and the findings of both spikes. Trace one API response from the HTTP call to
the saved fixture. Use the test cases as worked examples.

## Send back to me for review
001-api-behavior.md, one sample event of each type, and every TODO(verify).
