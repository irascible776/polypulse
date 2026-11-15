# PolyPulse project brief (read first, every session)

## What and why
PolyPulse tracks chosen Polymarket wallets, rebuilds each wallet's positions and
profit/loss (PnL) from its trade history, serves a leaderboard and a live
dashboard, and publishes its own measured speed.

It is a portfolio project for a Gravia backend intern interview. It must be
correct, honest about what is MEASURED versus ESTIMATED, and explainable in
simple terms. This brief overrides ROADMAP.md where they differ.

## Locked decisions

### Stack
- Python 3.12, FastAPI, asyncio. Postgres 16 only.
- Libraries: psycopg 3 (async capable), SQLAlchemy, Alembic, httpx, websockets,
  pytest, ruff. Any other dependency (including a different DB driver) needs a
  one-line reason first.
- One app with clear modules: ingestion, accounting, storage, serving, replay.
- No Redis, Kafka or Rust until a measurement shows a need.

### Sources and the canonical flow
- Event sources are plug-ins behind one interface. All write the same schema with
  a source tag:
  (A) Data API: full-history backfill, then cursor polling;
  (B) Polygon OrderFilled logs, V1 and V2, via an RPC provider (time-boxed slice);
  (C) replay from fixtures.
  Prices come from the Market WebSocket.
- ONE canonical source feeds the projector (setting CANONICAL_SOURCE, default
  data_api). Other sources are stored with their source tag and used ONLY for
  comparison. They never change positions.
- Dedupe happens WITHIN a source: unique (source, source_event_id).
  source_event_id must be a real unique identifier found by inspecting the data.
  tx_hash + asset + side is NOT unique (one transaction can hold several fills),
  so never use it as an id.
- Matching ACROSS sources is a separate reconciliation job. Use tx hash + log
  index if both sides expose them. Otherwise fall back to wallet + asset + side +
  size + price + time window, and label those matches LOW-CONFIDENCE.
  Its result never affects PnL.
- Detection latency for each source = observed_at - block_time (when WE first saw
  the event minus when it happened on chain). Compare the two sources on matched
  trades. Store observed_at per source row.

### Raw data and retention
- Raw wallet events (source_events) are append-only and KEPT. They are the source
  of truth for rebuilds.
- Retention (drop old partitions + downsample) applies only to high-volume derived
  data: price_ticks and fine-grained snapshots.
- If raw retention is ever enabled: archive the partition first and keep per-wallet
  checkpoints (state as of event N). Not in the MVP.
- Hot state (positions, wallet totals) is never sharded. History tables are
  partitioned by time.

### Processing
- A projector reads events in order by sequence number, applies them
  deterministically, and saves state plus its cursor in ONE transaction.
- A late event is one that sorts before a wallet's last applied event. It triggers
  a recompute of that wallet from raw events, in a transaction, with that wallet's
  aggregate row locked (SELECT ... FOR UPDATE).
- Accounting: average cost basis. Money and share amounts are NUMERIC/Decimal,
  never floats. Store event time and receive time. Unsupported event types are
  saved and counted.
- Redeems and resolutions: use the payout amounts recorded in REDEEM events. Do
  NOT assume $1.00 per share or infer a result from a price of 0 or 1. How to treat
  a resolved but never-redeemed position is decided in the accounting phase.
- Do not track cash balance or deposits in the MVP; PnL does not need them.

### Serving
- Push to the dashboard after commit: a versioned snapshot first, then deltas.
  Clients that reconnect fetch a fresh snapshot.
- Category (from the Gamma API) is a first-class field. Leaderboards per category.

### Chain slice (source B)
- Covers a short recent window plus the live tail. History comes from the Data API.
- Keep a block cursor. Poll new blocks every few seconds, or subscribe to logs if
  the provider supports it. Do a small catch-up of recent blocks on start.
- Scan in small block windows, halve the window on range or size errors, and back
  off on rate limits. Filter by contract address and topics.
- Wait N confirmations, store block hashes, and on a reorg roll back that block's
  events and replay.
- Contract addresses come from Polymarket's own contracts page and live in
  config. Never hardcode them from third-party sources.
- Record the real provider limits in docs/decisions/001-api-behavior.md.

### Overrides to ROADMAP.md
- Full-history backfill IS possible (start=1).
- Prefer cursor walking over time-overlap polling.
- The chain slice is core, not a stretch goal.

## Headline experiment
For the same wallets, compare API polling against chain logs: detection latency
(p50/p95/p99), completeness, and every mismatch found.

## Non-goals
Trading, a full chain indexer, multi-database setups, running our own node.

## Open questions: settle by testing, never by guessing
1. Which field(s) of /v2/activity form a unique event id?
2. Does the API expose a transaction hash and log index?
3. Does the Data API need a token? (The docs disagree.)
4. What are the real eth_getLogs limits of the chosen RPC provider?
5. [SETTLED] How to treat resolved but unredeemed positions: Option A (leave position open, no phantom math; mark-to-market is serving-time only, see docs/PHASE_3_ACCOUNTING.md).
6. Are the wallet addresses from public leaderboards proxy wallets?

## Verified facts (12 Nov 2026)
- Polymarket docs: https://docs.polymarket.com
  Market channel: https://docs.polymarket.com/market-data/websocket/market-channel
  Account activity: https://docs.polymarket.com/api-reference/feeds/list-account-activity
  V2 migration: https://docs.polymarket.com/v2-migration
- Public Market WebSocket events (book, price change, last trade, tick size)
  identify the market/asset, NOT the trader's wallet. A last_trade_price event has
  asset ID, market, price, side, size, fee rate, timestamp, and carries transaction_hash (observed).
- The Market WebSocket sends snapshots as a JSON list and live updates as single
  objects; the client must send the text PING every 10 seconds and gets PONG.
- Data API (data-api.polymarket.com) /v2/activity: the user (wallet) parameter is
  required. Pagination is cursor-only (follow next_cursor until null; no offset).
  Re-send identical filters on every page. Order is (block_timestamp, sequence_id),
  descending by default, ascending with sort_direction=ASC. If start is omitted it
  covers only the last 3 years; start=1 asks for full history. start is a TIME
  filter, not an offset.
- No token needed for the Data API (tested live without credentials; public GET works).
- 500 events per page accepted; cursor is opaque and signed. Never build or edit one.
- Event objects have no unique id and no log index; transaction_hash is present.
- Event types observed: TRADE, REDEEM, MAKER_REBATE, TAKER_REBATE, REFERRAL_REWARD,
  YIELD, MERGE. REDEEM usdc_size is the real cash payout; 80 of 158 REDEEMs had a
  payout of 0.
- /v2/status reports freshness (age_seconds and blocks behind).
- CLOB V2 went live 28 April 2026: new exchange contracts, a new OrderFilled
  event, and collateral changed from USDC.e to pUSD. Reading history across the
  cutover needs both V1 and V2.
- On-chain OrderFilled events record a trade on Polygon. Count each fill by its
  maker side; the taker-side row has the exchange as taker.
- Third-party sources disagree on V2 exchange contract addresses. UNVERIFIED until
  taken from Polymarket's own contracts page.
- Gamma API (gamma-api.polymarket.com) gives market metadata such as category.

## Product context (Gravia careers deck; context only, not requirements)
They run their own Polygon node, build one terminal per market category, launched
copytrading in Aug 2026 and esports (map-by-map pricing) in Oct 2026.

## Rules for the AI writing code
1. Never assume an API shape, address or behavior. Check the official docs or call
   the API. If you cannot verify it, STOP, ask me, and mark TODO(verify).
2. Start every change with a list of your assumptions. Ask before big ones.
3. Work in small steps. A step is done only when tests pass and its study guide is
   written.
4. Every write must be safe to repeat. Every derived number must be rebuildable
   from raw events.
5. No new dependency without a one-line reason.
6. Label every number as MEASURED or ESTIMATED.
7. Write comments, docs and study guides in simple language.
8. Keep each step inside its stated scope. Do not build ahead.