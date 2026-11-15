# Decision Note 003: PolyPulse Architecture & Locked Decisions

**Status:** Locked (Copied from PROJECT_BRIEF.md, 12 Nov 2026)  
**Author:** PolyPulse Engineering Team  
**Scope:** Canonical system architecture, data flow, and design principles.

---

## 1. What and Why
PolyPulse tracks chosen Polymarket wallets, rebuilds each wallet's positions and profit/loss (PnL) from its trade history, serves a leaderboard and a live dashboard, and publishes its own measured speed.

It is a portfolio project for a Gravia backend intern interview. It must be correct, honest about what is MEASURED versus ESTIMATED, and explainable in simple terms.

---

## 2. Locked Decisions

### Stack
* **Python 3.12**, **FastAPI**, **asyncio**, **PostgreSQL 16** only.
* **Libraries:** `psycopg 3` (async capable), `SQLAlchemy`, `Alembic`, `httpx`, `websockets`, `pytest`, `ruff`. Any other dependency needs a one-line reason first.
* **One app with clear modules:** `ingestion`, `accounting`, `storage`, `serving`, `replay`.
* **No Redis, Kafka or Rust** until a measurement shows a need.

### Sources and Canonical Flow
* Event sources are plug-ins behind one interface, writing the same schema with a source tag:
  * (A) **Data API:** full-history backfill, then cursor polling;
  * (B) **Polygon OrderFilled logs (V1 and V2):** via an RPC provider (time-boxed slice);
  * (C) **Replay from fixtures.**
* Prices come from the Market WebSocket (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).
* **ONE canonical source feeds the projector:** Setting `CANONICAL_SOURCE` (default `data_api`). Other sources are stored with their source tag and used ONLY for comparison. They never change positions.
* **Dedupe happens WITHIN a source:** unique `(source, source_event_id)`.
* **Matching ACROSS sources is a separate reconciliation job:** Use `tx_hash + log_index` if both expose them, otherwise `wallet + asset + side + size + price + time_window`.
* **Detection latency:** Calculated as $\text{observed\_at} - \text{block\_time}$ per source.

### Raw Data & Retention
* Raw wallet events (`source_events`) are append-only and KEPT. They are the source of truth for rebuilds.
* Retention (drop old partitions + downsample) applies only to high-volume derived data: `price_ticks` and fine-grained snapshots.
* Hot state (positions, wallet totals) is never sharded. History tables are partitioned by time.

### Processing & Accounting
* A projector reads events in order by sequence number, applies them deterministically, and saves state plus its cursor in ONE transaction.
* A late event triggers a recompute of that wallet from raw events in a transaction with that wallet's row locked (`SELECT ... FOR UPDATE`).
* Accounting: average cost basis. Money and share amounts in `NUMERIC`/`Decimal`, never floats.
* Actual `REDEEM` payouts are used; never assume $1.00/share.
* Cash balance / deposits are excluded in the MVP.

### Serving
* Push to the dashboard after commit: a versioned snapshot first, then deltas.
* Category (from Gamma API) is a first-class field; leaderboards per category.

---

## 3. Headline Experiment
For the same wallets, compare API polling against chain logs: detection latency ($p50/p95/p99$), completeness, and every mismatch found.

