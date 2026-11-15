# Phase 5 Specification: Serving Layer, Live Fast-Path Valuation & Real-Time Dashboard

## 1. Executive Summary & Gravia Alignment
Phase 5 transitions PolyPulse from an offline, deterministic batch accounting pipeline into a production-grade quantitative serving layer and real-time trading dashboard. 

For high-frequency prediction market making at **Gravia.trade**, the primary invariant of a serving system is:
$$\text{Correctness of Valuation} \succ \text{Latency} \succ \text{UI Polish}$$

A dashboard displaying a visually pleasing but stale or incorrect Mark-to-Market (MtM) PnL poses direct financial risk. This specification details:
1. Append-only pricing ticks and deterministic point-in-time (`?as_of=`) valuation.
2. Tiered price source hierarchy with explicit staleness tracking.
3. High-throughput FastAPI REST serving layer with sub-millisecond p95 latency.
4. Low-overhead real-time WebSocket streaming powered by PostgreSQL `LISTEN/NOTIFY`.
5. High-density quantitative terminal dashboard.
6. Reorg safety and standing on-chain reconciliation invariants.

---

## 2. Architecture & System Flow

```
                      +-----------------------------------+
                      |   Polymarket CLOB & Market Data   |
                      +-----------------+-----------------+
                                        | (Midpoint / Last Trade / Resolved)
                                        v
+-----------------------+     +-------------------+
| Live Ingestion Worker | --> |   market_prices   | (Append-Only Ticks)
+-----------+-----------+     +---------+---------+
            |                           |
            v                           v
+-----------------------+     +-------------------+     +---------------------+
|    PostgreSQL 16      | --> |  Valuation Engine | --> |   FastAPI Serving   |
|   (LISTEN / NOTIFY)   |     | (Unrealized PnL)  |     |  REST + WebSockets  |
+-----------+-----------+     +-------------------+     +----------+----------+
            |                                                      |
            +======================= WS / HTTP ====================+
                                        |
                                        v
                        +-------------------------------+
                        | Gravia Terminal Live Dashboard|
                        | (High-Density Dark Monospace) |
                        +-------------------------------+
```

---

## 3. Pricing Hierarchy & Mark-to-Market Formulation

### A. Pricing Hierarchy
For any prediction market outcome token $T$, the valuation engine resolves mark price $P_{\text{mark}}(T, t)$ through the following strict precedence:
1. **Tier 1 (`live_quote`)**: Polymarket CLOB order book midpoint:
   $$P_{\text{mid}} = \frac{P_{\text{best\_bid}} + P_{\text{best\_ask}}}{2}$$
2. **Tier 2 (`last_trade`)**: Most recent matched fill price from the market trade log.
3. **Tier 3 (`resolved`)**: Binary settlement payout ($1.000000$ for winning outcome, $0.000000$ for losing outcome). This value is exact and immune to staleness.
4. **Tier 4 (`stale`)**: Fallback to last observed price if $\Delta t = t - t_{\text{observed}} > \tau_{\text{stale}}$ (where $\tau_{\text{stale}} = 60\text{s}$ for active markets), flagged prominently with `is_stale = True`.
5. **Tier 5 (`unavailable`)**: If no pricing data exists, valuation returns `None` and positions are flagged as unpriced rather than inventing a price.

### B. Unrealized PnL Formulation
Cost basis is consumed strictly from the Phase 3 `positions` table (`avg_cost_basis`). No valuation component may recalculate or alter cost basis:
$$\text{Unrealized PnL}_i = (\text{Mark Price}_i - \text{Cost Basis}_i) \times \text{Open Shares}_i$$
$$\text{Total Wallet PnL} = \text{Realized PnL} + \sum_{i \in \text{Open Positions}} \text{Unrealized PnL}_i$$

---

## 4. Serving Layer Specifications

### REST API Endpoints (`app/api/routes.py`)
- `GET /wallets/{address}/positions`: Current open positions, shares, cost basis, mark price, unrealized PnL, price tier, and `staleness_seconds`.
- `GET /wallets/{address}/pnl`: Realized PnL, unrealized PnL, total PnL, cash invested, and position count. Supports `?as_of=<timestamp>` for historical time-travel queries.
- `GET /wallets/{address}/trades`: Paginated ledger history with `cursor` and `limit`.
- `GET /markets/{condition_id}/price`: Latest known mark price, source, and staleness.
- `GET /health`: System health, PostgreSQL pool stats, latest block height, and RPC latency.

### WebSocket Protocol (`/ws/wallets/{address}`)
- Uses PostgreSQL `LISTEN / NOTIFY` on channel `polypulse_wallet_{address}`.
- Payload Schema:
  ```json
  {
    "type": "pnl_update" | "trade" | "price_update",
    "wallet": "0x...",
    "payload": { ... },
    "server_time": "2026-11-13T06:45:00Z"
  }
  ```

---

## 5. Migration 0005: Pricing & Reorg Safety
- `market_prices` table:
  - `id`: BigInteger PK
  - `condition_id`: String(66), nullable=True, indexed
  - `token_id`: String(78), not null, indexed
  - `price`: Numeric(18, 6), not null
  - `source`: String(32), not null (`live_quote`, `last_trade`, `resolved`, `stale`)
  - `observed_at`: DateTime(timezone=True), not null, indexed
  - Indices: `(token_id, observed_at DESC)` and `(condition_id, observed_at DESC)`.
- `chain_events` table addition:
  - Add `block_hash VARCHAR(66) NULL` to support reorg detection and re-scan validation.

