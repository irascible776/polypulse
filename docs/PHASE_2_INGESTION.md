# Phase 2: Data API ingestion and raw event storage

Read PROJECT_BRIEF.md, STUDY_GUIDE_TEMPLATE.md and docs/decisions/001-api-behavior.md
first. Follow the AI rules in the brief. Stay inside this phase's scope.

## Goal (plain words)
Reliably copy every event of a tracked wallet, its full history and then new
activity, into an append-only raw table. Running it twice, or crashing in the
middle, must never create duplicates or gaps. Measure how late we see events.

## In scope
Close-out fixes, event schema, ingestion client, backfill, live poller, wallet
seeding, a fixture replay source, status endpoint, tests, a crash test, and
measurements.

## Out of scope
Positions or PnL, the dashboard, push, the chain source, table partitioning (Phase 4),
extra indexes beyond those listed here.

## Approved Decisions & Spec Updates
- **DB driver:** psycopg 3 (approved).
- **Poll interval:** default 5s, with idle-backoff up to 30s and speed-up on activity; global request cap. 15-minute lag measurement run at 2s as an experiment and both reported.
- **Overlap window:** 60s. Count `is_late` events and lag p99 during live run. Reconcile sweep every 10-15 min re-walking the last hour and counting new events (expected 0; reported as metric).
- **Batch insert method:** `INSERT ... ON CONFLICT (source, source_event_id) DO NOTHING RETURNING id` to count inserted vs duplicate.
- **Cursor fallback:** `start = last_flushed_second - 1` (verified `start` is inclusive). Garbage cursor shape verified (`400 Bad Request`, `retryable: false`).
- **Spec Change 1:** Per wallet: backfill FIRST, live polling only after `backfill_state = complete`. Different wallets run in parallel. This keeps `api_order` chronological.
- **Spec Change 2:** Take a per-wallet write lock (advisory lock or row lock on its cursor row) when assigning `api_order`, so two writers can never race.

## 2.0 Close-out fixes (do these FIRST; they block the rest)

### 2.0a Make event ids stateless and deterministic
The Phase 1 EventIdGenerator keeps a running occurrence counter. That is risky:
when the same events are fetched again (a re-poll, an overlap, a restart), a counter
that remembers earlier calls would give them DIFFERENT ids, and they would be saved as
duplicates.
- base_key = SHA-256 of the exact fields: wallet, transaction_hash, type,
  condition_id, token_id, side, outcome_index, size, usdc_size, price, timestamp.
  Use Decimal, hash the normalized text, and exclude name, pseudonym, bio and
  profile images.
- occurrence = the position (0, 1, 2...) of this event among events with the SAME
  base_key in ONE complete group, in API order. source_event_id = base_key plus
  occurrence.
- A complete group means all events of one wallet in one timestamp second. Pages can
  split a group, so hold back the last second of a page until the next page arrives
  or the walk ends.
- Required tests:
  1. the same input gives the same ids whether the generator is new or has been used
     before;
  2. ingesting the same batch twice adds zero rows;
  3. two identical fills split across a page boundary get ids 0 and 1;
  4. an overlap re-fetch of already-saved events adds zero rows.

### 2.0b Parse numbers exactly
Parse JSON numbers as Decimal (json parse_float=Decimal). WebSocket prices and sizes
arrive as strings; keep them as text until converted to Decimal.

### 2.0c Price versus cash
In Phase 1, 75 of 2,261 trades (3.3%) had a gap greater than 0.01 between
size x price and usdc_size. The ratio usdc_size / (size x price) ranged from 0.9720
to 1.0486. The cause is NOT known: it could be fees, one order filling at several
price levels, or a rounded price.
- Decision for now: for money, use usdc_size (the actual cash). Store the reported
  price as price_reported. Derive effective_price = usdc_size / size when size > 0.
  Set a neutrally named flag cash_price_gap when the gap exceeds 0.01.
- Investigate WITHOUT guessing: (1) split the 75 by side and report the ratio range per
  side; (2) compare the gap rate across order-size buckets, including large orders
  that did NOT have a gap; (3) take 3 of the 75 transactions and look them up in the
  /v2/trades data (fixtures/sample_trades_v2.json, or a small live query) to see
  whether that transaction shows several fills at different prices. Record the
  result in 001. If it stays unknown, write UNVERIFIED. The exact answer will come
  from on-chain data in the chain phase.

### 2.0d Evidence and housekeeping
- Show that one command path works: make db-up, make migrate, make run, then
  curl /health and /ready. Paste the output.
- Confirm docs/study/01-foundations.md, docs/decisions/002 and 003 exist.
- Rate limits: the claim "about 800 requests per 10 seconds" must cite the exact docs
  page it came from, or be marked UNVERIFIED. Never present it as measured.
- From fixtures/docs_account_activity.html, extract the documented list of event
  types (the type enum) into 001. We have seen TRADE, REDEEM, MAKER_REBATE,
  TAKER_REBATE, REFERRAL_REWARD, YIELD and MERGE; SPLIT has not been seen yet.
- The MERGE sample came from a third wallet ("swisstony") that was not approved.
  Record how it was found. Ask me before querying any other wallet.
- Update PROJECT_BRIEF.md "Verified facts" with these OBSERVED facts:
  - No token needed for the Data API (tested without credentials).
  - 500 events per page accepted; cursor is opaque and signed. Never build or edit one.
  - Event objects have no unique id and no log index; transaction_hash is present.
  - Event types observed: TRADE, REDEEM, MAKER_REBATE, TAKER_REBATE, REFERRAL_REWARD,
    YIELD, MERGE. REDEEM usdc_size is the real cash payout; 80 of 158 REDEEMs had a
    payout of 0.
  - The Market WebSocket sends snapshots as a JSON list and live updates as single
    objects; the client must send the text PING every 10 seconds and gets PONG.
  - A last_trade_price event carries a transaction_hash (observed once; confirm).
  - /v2/status reports freshness (age and blocks behind).
- Fixtures: do NOT commit the bulk raw files (about 22 MB of third-party data). Add them
  to .gitignore, add a script that regenerates them, and commit only (a) a tiny
  synthetic fixture wallet and (b) a few small samples with name, pseudonym, bio and
  profile image fields removed.

## 2.1 Schema (one Alembic migration)

source_events (append-only; never UPDATE or DELETE)
- id bigserial primary key (arrival order; the projector's cursor will use it)
- source text, source_event_id text, UNIQUE (source, source_event_id)
- wallet text, event_type text (store ANY type, even unknown ones)
- event_time timestamptz, observed_at timestamptz (when we first saw it)
- transaction_hash text, condition_id text null, token_id text null,
  side text null, outcome_index int null
- size numeric, usdc_size numeric, price_reported numeric, effective_price numeric null
- cash_price_gap boolean
- api_order bigint: per wallet, increasing, assigned in the order the API returned
  events during an ASCENDING walk; events already saved keep their number
- is_late boolean: set when event_time is earlier than the newest stored event for the
  wallet (Phase 3 will recompute that wallet)
- raw jsonb: the event exactly as received

Index only: the unique key, and (wallet, event_time, api_order).

ingestion_cursors (one row per wallet and source)
- backfill_state (not_started, in_progress, complete), last_event_time, last_cursor
  (opaque, best effort), last_success_at, last_error, consecutive_failures

Unknown or unsupported event types are saved and COUNTED (a view or counter). They
must never be rejected or cause a crash.

## 2.2 Source interface and Data API client
- Define an EventSource interface with two implementations: DataApiSource and
  FixtureReplaySource (reads saved pages through the SAME pipeline).
- DataApiSource (app/ingestion/data_api.py): async httpx with timeouts; sends one
  request at a time per wallet and a global cap; pagination loop with protection
  (maximum pages, repeated-cursor detection, stop on empty page or has_more false);
  retries with exponential backoff and jitter on 429, 5xx and timeouts; obeys
  Retry-After; a circuit breaker that opens after N consecutive failures and probes
  again later.
- Counters for requests, retries, 429s, failures, and latency per request.

## 2.3 Backfill
- Per wallet: start=1, ascending, 500 per page. Save each page's events in one
  transaction with INSERT ... ON CONFLICT DO NOTHING; count inserted versus duplicate.
- Update ingestion_cursors in the SAME transaction as the page insert.
- Resume after a crash: first try the saved cursor; if it is rejected, restart from
  last_event_time with a small overlap and rely on dedupe.
- Mark backfill_state = complete only after the final page.

## 2.4 Live poller
- Per tracked wallet, every POLL_INTERVAL (approved default): ask for events from
  last_event_time minus the overlap, ascending, and walk the cursor.
- Set observed_at when an event is first inserted. Record detection lag
  = observed_at - event_time for each NEW event.
- Also poll /v2/status and store age_seconds, so we can compare the API's own
  freshness with our measured lag.
- Run backfill and live polling as separate tasks that never block each other.
  Failures in one wallet must not stop the others.

## 2.5 Wallet seeding
- wallets.yaml: wallet address, label, tracking_start. A loader validates with the
  Phase 1 normalizer and upserts tracked_wallets. Never put wallet lists in code.
- Seed: the two approved wallets plus a small synthetic fixture wallet for tests.

## 2.6 Status endpoint
GET /ingestion/status returns, per wallet: backfill_state, last_success_at,
events stored, counts per event type, unsupported type count, consecutive_failures,
and detection lag p50 and p95 over the last 1,000 new events.

## 2.7 Tests (all required)
- Idempotency: ingest the saved pages twice; the row count does not change.
- Counts must match the Phase 1 measurements:
  wallet AppleTiiime: 2,454 rows (TRADE 2,261, REDEEM 158, MAKER_REBATE 15,
  TAKER_REBATE 11, REFERRAL_REWARD 9); wallet JnStPrdctns: 835 rows, including
  44 YIELD.
- Resume: stop in the middle of a backfill, restart, and the final result is identical.
- api_order is increasing and survives re-ingestion.
- Unknown event type: saved and counted, no crash.
- 429 with Retry-After is obeyed; five failures open the circuit breaker; a
  successful probe closes it.
- Tests that touch the database use a real Postgres in Docker, not mocks.

## 2.8 Crash test (scripts/chaos_ingest.py)
Start a backfill of AppleTiiime, kill the process with SIGKILL at a random moment,
restart, and let it finish. Check: 2,454 rows, zero duplicates, and the same
per-type counts. Repeat 5 times. Write the results in
docs/decisions/004-ingestion-resilience.md.

## 2.9 Measurements (label every number MEASURED)
- Backfill: requests per second and events saved per second.
- Live run: 15 minutes with both wallets. Report detection lag p50, p95 and p99, polls
  per minute, and the API's own age_seconds from /v2/status beside it.
- If the numbers look odd (for example lag of zero), explain why before reporting.

## Definition of done
- Both wallets are fully backfilled and the live poller runs for 15 minutes.
- The tests above pass, along with make lint.
- The crash test passes 5 of 5 runs.
- 001 and the brief are updated; every TODO(verify) is resolved or listed.

## Study guide task
Write docs/study/02-ingestion.md following STUDY_GUIDE_TEMPLATE.md: the source
interface, the id scheme and why a stateful counter is dangerous, the pagination loop,
the backfill and live poll, retries and the circuit breaker, and the crash test. Trace one
event from the API response to its saved row. Use the tests as worked examples.

## Send back to me for review
004-ingestion-resilience.md, the per-type row counts from the database, the 2.0c
investigation results, the lag numbers, the make test output, and every open TODO(verify).
