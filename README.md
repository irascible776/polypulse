# PolyPulse

PolyPulse ingests Polymarket wallet activity, builds average-cost positions and a
replayable ledger, compares known tokens with Polygon balances, and serves a
valuation dashboard. Values are estimates when they depend on marks rather than
cash received.

## Architecture

```text
Polymarket Data API / CLOB / Gamma ──► worker loops A–D ──► PostgreSQL
                                           │                    │
Polygon RPC ◄──────────────────────────────┘                    │
  └── balance and log checks                                    │
                                                                ▼
                         FastAPI: REST + WebSocket + dashboard.html
```

The worker fetches price observations and resolution evidence, polls and
projects wallets, and scans/reconciles chain events. The API reads the stored
state. Its WebSocket listener uses a PostgreSQL session; PgBouncer transaction
pooling does not preserve LISTEN subscriptions. See
[PostgreSQL LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) and
[PgBouncer features](https://www.pgbouncer.org/features.html).

## Run locally

Install Python and `uv`, then from a clean checkout:

```bash
uv sync
cp .env.example .env
# Set POSTGRES_PASSWORD, DATABASE_URL and POLYPULSE_API_KEY in .env.
make db-up
make migrate
make run
```

In another terminal with the same environment, run `make worker` and leave it
running for live data. Stop it with SIGTERM (for example, `kill -TERM <worker-pid>`).
Open
<http://localhost:8000/dashboard> (or run `xdg-open http://localhost:8000/dashboard`
on a Linux desktop). The continuous worker is deliberately not
started by `make run`. Migrations create schema, not historical events or wallet
registrations. `wallets.yaml` includes a synthetic fixture wallet; review it
before using the seeding helper. A fresh database needs a backfill before it
can show historical positions.
When the worker is off, stored values remain readable but ingestion and
resolution checks age. `/pnl` and `/positions` return `valuation_current=false`
when ingestion or unresolved-market resolution evidence is stale, or a position
is unpriced. The dashboard names those reasons in a banner. `/health` becomes
degraded when ingestion or reconciliation is stale; it reports in-flight chain
changes separately from unexplained mismatches.

Tests must use a scratch copy, never the database named `polypulse`. For example,
using local PostgreSQL client tools and the configured credentials:

```bash
createdb -h localhost -U postgres polypulse_scratch
pg_dump -h localhost -U postgres -Fc -f /tmp/polypulse-scratch.dump polypulse
pg_restore -h localhost -U postgres --no-owner --no-acl -d polypulse_scratch /tmp/polypulse-scratch.dump
set -a; . ./.env; set +a
DATABASE_URL="${DATABASE_URL%/*}/polypulse_scratch" make test
make lint
make bench
```

The test guard rejects the database named `polypulse`. The tests need seeded
scratch data; an empty schema alone does not reproduce the integration fixtures.
`make bench` reports cold and warm calls separately. `VALUATION_CACHE_DISABLED=1`
forces cold valuation; `BENCH_REQUESTS` controls the sample size. The benchmark
prints measurements rather than enforcing a performance threshold.

## Meaning of the numbers

- **Realized PnL:** gains or losses from applied closing events in the ledger.
- **Live unrealized:** open shares marked with a non-resolved price, less cost.
- **Estimated unrealized:** open, unredeemed shares marked at an agreed resolved
  payout. It is not realized cash.
- **Unpriced:** shares for which no usable mark exists. Their cost basis is
  reported separately.
- **Complete:** every open position has a mark. The dashboard hides economic PnL
  when this is false.
- **Valuation current:** complete, with both wallet ingestion and unresolved
  market checks within their configured freshness limits.
- **Economic PnL:** realized plus live unrealized plus estimated unrealized,
  when complete. The API field `total_pnl_excl_unpriced` excludes unpriced
  positions and includes estimates.

## Mark policy

| Order | Rule |
|---|---|
| First | Use a chain-confirmed payout when stored resolution evidence agrees with Gamma and identifies the outcome. |
| Next | Use a recent valid two-sided book midpoint within the configured spread. |
| Fallback | Use a valid stale two-sided quote before a last-trade tick. Both are labeled stale; the CLOB last-trade execution time is unknown. |
| Otherwise | Return an unpriced mark, not a fabricated zero. |

The CLOB last-price response has price and side but no execution time or trade
identifier. The public trade feed has timestamps, but matching a particular
CLOB observation to a feed trade is not established. See
[decision 008](docs/decisions/008-pricing-policy.md), the
[CLOB response](https://docs.polymarket.com/api-reference/market-data/get-last-trade-price),
and [trade feed](https://docs.polymarket.com/api-reference/feeds/list-trades).
The resolver relies on the latest stored resolution check; when the worker is
off, that check can lag a market that has since resolved.

## Measurements and limits

**MEASURED 2026-11-15:** the price pass and PnL output are in
[price-pass.txt](reports/phase5c/price-pass.txt). The subsequent chain comparison
used reference block 97361071: [reconciliation output](reports/phase5c/reconcile-before-scan.txt).
These measurements are observations at different times, not a single atomic
market snapshot.

| Wallet | Realized | Live unrealized | Estimated unrealized | Complete |
|---|---:|---:|---:|---|
| AppleTiiime | 42895.34498396215584770244434 | 3.134848120822122784851251323 | 0.04737544402202951270426782103 | true |
| JnStPrdctns | 26177.70511240808436588856012 | -71394.31652302921191232753028 | -348213.8721820238724535610298 | true |

At that comparison block, **MEASURED 2026-11-15**, AppleTiiime was
426 MATCH / 1 MISMATCH / 0 UNKNOWN and JnStPrdctns was 95 / 0 / 0. The
AppleTiiime difference matched later incoming transfers found by the independent
rescan; its source poll was stale at comparison time. The rescan and block
numbers are in [rescan.txt](reports/phase5c/rescan.txt).

**MEASURED 2026-11-15**, with the earlier reconciliation references 97360397
and 97360403, the [cold/warm benchmark](reports/phase5b-resume/benchmark.txt)
showed cold throughput around 25 requests/second per process at concurrency
20, while latency grew roughly with concurrency. Warm results are reported
separately in that file. CPU saturation is a plausible explanation, not a
profiled finding: TODO(verify). Snapshotting is a possible next step to measure.

Known limitations:

- JnStPrdctns's older chain history was not fully scanned. The reverse check only
  considers tokens known to the ledger or scanned events; unknown holdings can
  be missed.
- Event identity is a derived fingerprint, not an upstream event ID. The writer
  assumes one writer per wallet and uses uniqueness constraints as a safety net.
- Money arithmetic uses the Decimal context described in
  [decision 007](docs/decisions/007-wallet-event-order.md); the handover
  comparison tolerance is 1e-12 USDC. These are configuration and policy, not
  measured prices.
- Last-trade execution time is unknown for CLOB last-price observations; stored
  observation time is the polling time.
- Maker/taker rebates, referral rewards, rewards, and yield are stored in
  `source_events` but excluded from position and realized PnL accounting. In
  the **MEASURED 2026-11-15 Phase 5D pass**, `usdc_size` totals were:

  | Wallet | Type | Rows | `SUM(usdc_size)` |
  |---|---|---:|---:|
  | AppleTiiime | MAKER_REBATE | 19 | 1627.6904 |
  | AppleTiiime | TAKER_REBATE | 15 | 473.2791 |
  | AppleTiiime | REFERRAL_REWARD | 12 | 325.6916 |
  | AppleTiiime | REWARD | 1 | 5.1734 |
  | JnStPrdctns | MAKER_REBATE | 17 | 1464.9155 |
  | JnStPrdctns | TAKER_REBATE | 19 | 361.5241 |
  | JnStPrdctns | YIELD | 48 | 442.6534 |

  All stored amounts matched `raw.usdc_size` and were nonnegative. The raw
  `side` field was empty, so these values do not independently prove a cash
  transfer direction: TODO(verify).
- Valuation runs in the API process. The Docker worker image has not been built
  in this environment because no Docker daemon was available.
- Early history originated in fixtures; live ingestion uses a separate cursor.
