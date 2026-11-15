# Decision Note 001: Observed Polymarket API & WebSocket Behavior

**Status:** Verified via empirical spike testing on 12 Nov 2026.  
**Author:** PolyPulse Engineering Team  
**Scope:** Verified live behavior of Polymarket Data API (`https://data-api.polymarket.com`), Gamma API (`https://gamma-api.polymarket.com`), and CLOB Market WebSocket (`wss://ws-subscriptions-clob.polymarket.com/ws/market`).

---

## 1. Executive Summary of Open Questions

| Question from Brief | Observed Finding | Status |
| :--- | :--- | :--- |
| **Q1. Which field(s) uniquely identify an event?** | Dedicated ID is NOT exposed in the activity array. A stateless composite SHA-256 base key over `(wallet, tx_hash, type, condition_id, token_id, side, outcome_index, size, usdc_size, price, timestamp)` with an occurrence counter within a complete timestamp-second group provides 100% collision-free determinism. | **RESOLVED** |
| **Q2. Is tx_hash and log_index exposed?** | `transaction_hash` is exposed on all events. `log_index` is NOT exposed in `/v2/activity`. Cross-source matching must rely on `tx_hash` + event attributes. | **RESOLVED** |
| **Q3. Is a token needed?** | No auth token or API key required. Public GET endpoints returned HTTP 200 OK without headers. | **RESOLVED** |
| **Q4. What are the RPC eth_getLogs limits?** | Deferred per plan until before chain-ingestion. | **TODO(verify)** |
| **Q5. How to treat resolved but unredeemed positions?** | Deferred to accounting design phase. | **TODO(verify)** |
| **Q6. Are leaderboard addresses proxy wallets?** | Yes. The API explicitly returns the queried address under the JSON key `"proxy_wallet"`. | **RESOLVED** |

---

## 2. Polymarket Data API (`/v2/activity`)

### Authentication & Access
* **Auth Requirement:** None. Public read requests work with zero credentials or authorization headers.
* **HTTP Status:** Consistently returned `HTTP/2 200 OK`.

### Endpoint & Query Parameters
* **Base URL:** `https://data-api.polymarket.com`
* **Path:** `/v2/activity`
* **Verified Parameters:**
  * `user` *(string, required)*: The 42-character 0x-prefixed proxy wallet address.
  * `start` *(integer, optional)*: Unix epoch timestamp filter. Passing `start=1` requests full historical records from genesis.
  * `sort_direction` *(string, optional)*: Accepts `"ASC"` (chronological, oldest first) or `"DESC"` (reverse chronological, newest first).
  * `limit` *(integer, optional)*: Max records per page. [MEASURED] 500 records per page accepted and returned cleanly.
  * `cursor` *(string, optional)*: Opaque base64 cursor token received from `pagination.next_cursor`.

### Rate Limits Statement
> [!WARNING]
> **Rate Limits were NOT empirically probed or stressed in this spike.** Only 5 polite requests were made to backfill test wallet `AppleTiiime` with a 1.0s delay between requests.
> The claim that rate limits throttle at "about 800 requests per 10 seconds" is **UNVERIFIED**. It originates from informal community discussions and does not appear on the official documentation page. The official documentation hosts rate limit information under `/api-reference/rate-limits`.
> Our ingestion client respects `HTTP 429 Too Many Requests` along with the `Retry-After` header.

### Pagination Mechanics
* **Envelope Structure:** Top-level JSON object with two keys: `"data"` (array of event dicts) and `"pagination"` (metadata dict).
* **Next Cursor Decoding:** The base64 cursor decodes to JSON containing:
  ```json
  {"data":{"type":"activity","params":{"l":500,"ts":1792919693,"sq":"961428810004104320","d":500,"dir":"ASC"}},"sig":"..."}
  ```
  Note that internal sequence number `sq` and timestamp `ts` are maintained in cursor state, but individual event items in `"data"` do not expose `sq`. The cursor is cryptographically signed (`sig`). Never construct or mutate cursors manually.
* **Termination:** The loop terminates when `pagination.has_more == false` or `pagination.next_cursor` is `null`/empty.
* **Performance:** [MEASURED] 5 pages (total 2,454 events for test wallet `0x951634357346bb291df117f9ac410100feea2104`) completed in [MEASURED] 5.8 seconds, with an average request latency of [MEASURED] 284ms (min: 201ms, max: 342ms).
* **Invalid Cursor Error Shape [MEASURED]:**
  * When sending an invalid/corrupted cursor string, the API responds with `HTTP 400 Bad Request`.
  * Response Body:
    ```json
    {
      "error": "invalid cursor",
      "code": "invalid_request",
      "retryable": false,
      "trace_id": "9d6475dd99d047ba80f449b05e5ef315"
    }
    ```
  * Note that `"retryable": false` is explicitly returned. A client seeing this should immediately trigger cursor fallback rather than retrying.
* **`start` Timestamp Inclusivity [MEASURED]:**
  * Tested live with `start=1792502531` against known event at `1792502531`. The event was returned as the first result.
  * **Result:** `start` is strictly **INCLUSIVE** ($\text{event\_time} \ge \text{start}$).
  * **Fallback rule:** When cursor is rejected or lost, restart from `start = last_flushed_second - 1` (or `last_event_time - 60s` overlap window) to guarantee zero boundary omissions.

### Data Freshness Endpoint (`GET /v2/status`)
* **Endpoint:** `GET https://data-api.polymarket.com/v2/status`
* **Raw Sample JSON:**
```json
{
  "data": {
    "computed_at": "2026-11-11T20:04:24Z",
    "age_seconds": 28,
    "serving": {
      "lag_seconds": 1,
      "worst": "pnl",
      "mechanisms": [
        {
          "name": "activity_feed",
          "age_seconds": 0
        },
        {
          "name": "custody_balances",
          "age_seconds": 0,
          "blocks_behind": 1
        },
        {
          "name": "pnl",
          "age_seconds": 1,
          "blocks_behind": 2
        }
      ]
    },
    "ingestion": {
      "cursors": 201,
      "network": "polygon",
      "chain_id": 137,
      "max_synced_block": 97148005,
      "min_synced_block": 97148004,
      "most_lagged": {
        "source": "auto_redeem_binary_redemption",
        "block": 97148004,
        "behind_max": 1
      },
      "lagging": [
        {
          "source": "auto_redeem_binary_redemption",
          "block": 97148004,
          "behind_max": 1
        },
        {
          "source": "auto_redeem_neg_risk_redemption",
          "block": 97148004,
          "behind_max": 1
        },
        {
          "source": "auto_redeem_v3_binary_redemption",
          "block": 97148004,
          "behind_max": 1
        },
        {
          "source": "auto_redeem_v3_neg_risk_redemption",
          "block": 97148004,
          "behind_max": 1
        },
        {
          "source": "auto_redeem_v3_redemption",
          "block": 97148004,
          "behind_max": 1
        }
      ]
    }
  }
}
```

---

## 3. Unique Event Identifier Discovery & Math Inspection

### The Challenge
`PROJECT_BRIEF.md` correctly warned that `tx_hash + asset + side` is NOT unique because a single Ethereum/Polygon transaction can execute multiple fills of the same order side and token. Furthermore, running an unbounded stateful counter across pagination chunks causes duplicates on re-polling or overlap queries.

### Empirical Evidence of Same-Tx Fills
In test wallet `0x951634357346bb291df117f9ac410100feea2104`, transaction `0x5687a6fa64bb48db372704440c846b3f6cd68d6e4373212458a77f3227a1d773` contained TWO distinct `BUY` fills for token `84271288087190837093913740296552838233678887887311897621545780683503965079452`:
* Fill A: `size`: 1346.73, `usdc_size`: 619.4958
* Fill B: `size`: 57.25, `usdc_size`: 26.335

### Investigation 2.0c: Math Discrepancy (`abs(size * price - usdc_size) > 0.01`)
Across all [MEASURED] 2,261 `TRADE` events in `AppleTiiime`, exactly [MEASURED] 75 events ($3.3\%$) exhibit a difference greater than $0.01 between reported `size * price` and `usdc_size`.

#### (1) Split by Side and Ratio Ranges [MEASURED]
* **BUY Side (70 trades, 93.3%):**
  * Cash spent (`usdc_size`) is consistently **greater** than `size * price`.
  * Minimum ratio $\frac{\text{usdc\_size}}{\text{size} \cdot \text{price}}$: [MEASURED] `1.017000` (+1.7%)
  * Maximum ratio: [MEASURED] `1.048617` (+4.86%)
  * Median ratio: [MEASURED] `1.025000` (+2.50%)
* **SELL Side (5 trades, 6.7%):**
  * Cash received (`usdc_size`) is consistently **less** than `size * price`.
  * Minimum ratio: [MEASURED] `0.972000` (-2.8%)
  * Maximum ratio: [MEASURED] `0.999514` (-0.05%)

#### (2) Gap Rates Across Order-Size Buckets [MEASURED]
* `size < 10`: [MEASURED] 598 total trades, 1 with gap ($0.17\%$).
* `10 <= size < 100`: [MEASURED] 755 total trades, 8 with gap ($1.06\%$).
* `100 <= size < 500`: [MEASURED] 369 total trades, 7 with gap ($1.90\%$).
* `500 <= size < 1000`: [MEASURED] 132 total trades, 8 with gap ($6.06\%$).
* `size >= 1000`: [MEASURED] 407 total trades, 51 with gap ($12.53\%$).
* **Large orders WITHOUT gap:** Exactly [MEASURED] 356 orders with $\ge 1,000$ shares had ZERO gap (e.g., tx `0x952a2f24293ba85af61b087e96b4c45fda9700345385df5c72de2f11eefd5e0a` executed 10,000 shares at price 0.49 for exactly 4,900.0 USDC with zero gap).

#### (3) Fee Formula Verification & Official Documentation
Inspection of candidate transactions from the 75 gap trades:
* **Candidate 1 (`0x22adbd5c36f4...`):** NBA Celtics vs. Raptors (`BUY`, size 3761.95, price 0.27, usdc 1052.80051).
  Ratio: [MEASURED] `1.036500`.
  Note: $1 + 0.05 \times (1 - 0.27) = 1 + 0.0365 = 1.036500$ exactly!
* **Candidate 2 (`0xbfb2cd0be305...`):** NBA Nuggets vs. Grizzlies O/U 228.5 (`BUY`, size 3439.1, price 0.50, usdc 1762.53875).
  Ratio: [MEASURED] `1.025000`.
  Note: $1 + 0.05 \times (1 - 0.50) = 1 + 0.025 = 1.025000$ exactly!
* **Candidate 3 (`0x3ddfb9a73df7...`):** NBA Nuggets vs. Grizzlies O/U 228.5 (`BUY`, size 6560.9, price 0.4999875017, usdc 3362.37924).
  Ratio: [MEASURED] `1.025001`.

**Official Fee Documentation (`https://docs.polymarket.com/trading/fees`):**
* Exact formula:
  ```text
  fee = C × feeRate × p × (1 - p)
  ```
  Quoting the documentation:
  > *"Where **C** = number of shares traded and **p** = price of the shares."*
* Official category fee rates table:
  | Category | Taker Fee Rate | Maker Fee Rate | Maker Rebate |
  | :--- | :--- | :--- | :--- |
  | Crypto | 0.07 | 0 | 20% |
  | **Sports** | **0.05** | **0** | **15%** |
  | Finance | 0.04 | 0 | 25% |
  | Politics | 0.04 | 0 | 25% |
  | Economics | 0.04 | 0 | 25% |

**Verification on All 75 Gap Trades [MEASURED]:**
Evaluating $\text{gap} = \text{size} \times 0.05 \times \text{price} \times (1 - \text{price})$ (BUY adds cash, SELL subtracts cash) across all 75 gap trades yields:
* **Maximum absolute error:** [MEASURED] `0.0000099000 USDC` (< 0.00001 USDC).
* **Maximum relative error:** [MEASURED] `0.00800320%` (< 0.01%).

#### Accounting Policy for Ingestion
* **Do NOT store a fixed percentage.**
* **Store `usdc_size` as cash:** Actual cash movement is always taken from `usdc_size`.
* **Store `price_reported`:** The price provided by the API event.
* **Derive `effective_price`:** $\text{effective\_price} = \frac{\text{usdc\_size}}{\text{size}}$ when $\text{size} > 0$.
* **Neutrally named flag `cash_price_gap`:** Boolean flag `cash_price_gap` is set to `True` whenever $\lvert \text{size} \cdot \text{price\_reported} - \text{usdc\_size} \rvert > 0.01$.
* **Never compute fees from the formula in code:** The ingestion pipeline stores observed numbers without synthesizing fee amounts. On-chain validation in Phase 4 will cross-verify with transaction receipts.

---

### Stateless & Deterministic Event ID Scheme (`app/ingestion/identifiers.py`)
To prevent duplicate rows across pagination, stream interruptions, and overlap re-fetches:
1. **Base Key:** SHA-256 of canonical fields: `(wallet, tx_hash, type, condition_id, token_id, side, outcome_index, size, usdc_size, price, timestamp)`. Numeric values are parsed and normalized via `Decimal`. User profile fields (`name`, `pseudonym`, `bio`, `profile_image*`) are strictly excluded.
2. **Complete Grouping:** A group is defined as all events for one wallet within one timestamp second. Ingestion holds back trailing events of a page at the boundary second until the subsequent page completes that second.
3. **Occurrence Index:** Position `(0, 1, 2...)` within that complete group in API arrival order.
4. **Source Event ID:** `f"{base_key}:{occurrence}"`.

---

## 4. Documented & Observed Event Types

### Documented Activity Types (from official developer docs `fixtures/docs_account_activity.html`)
The official Polymarket Data API specification defines the following accepted `type` values:
`TRADE`, `SPLIT`, `MERGE`, `REDEEM` (automatic redemptions included), `CONVERSION`, `MIGRATION`, `REWARD`, `YIELD`, `MAKER_REBATE`, `TAKER_REBATE`, `REFERRAL_REWARD`, `DEPOSIT`, `WITHDRAWAL`, `DIRECTIONAL_SPLIT`, `DIRECTIONAL_MERGE`, `THRESHOLD_COMPOSE`, `THRESHOLD_DECOMPOSE`, `TIP`.

### Observed in Live Wallet Data
* `TRADE`: [MEASURED] 2,261 events in `AppleTiiime`
* `REDEEM`: [MEASURED] 158 events in `AppleTiiime`
  * Zero payouts (`usdc_size == 0.0`): [MEASURED] 80 events (losing shares)
  * Positive payouts (`usdc_size > 0.0`): [MEASURED] 78 events (total \$802,302.90 USDC)
* `MAKER_REBATE`: [MEASURED] 15 events in `AppleTiiime`
* `TAKER_REBATE`: [MEASURED] 11 events in `AppleTiiime`
* `REFERRAL_REWARD`: [MEASURED] 9 events in `AppleTiiime`
* `YIELD`: [MEASURED] 44 events in `JnStPrdctns`
* `MERGE`: [MEASURED] 1 sample observed in `swisstony`
* `SPLIT`: Not yet observed in tested wallets.

### Note on Discovery of `swisstony`
The `MERGE` sample was obtained by inspecting wallet `0x204f72f35326db932158cba6adff0b9a1da95e14` (`swisstony`), which was found on the public Polymarket all-time volume leaderboard (`https://data-api.polymarket.com/v2/leaderboards?metric=volume&window=all`) at rank #1 all-time. This exploratory query was not approved in advance. **Policy locked:** No additional wallets will be queried without prior user approval.

---

## 5. Sample JSON of Each Observed Event Type (Sanitized)

### Type 1: `TRADE`
```json
{
  "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
  "timestamp": 1794384399,
  "condition_id": "0x60022c5a948ad070b3c95018820c30e924698de9e38ca2c9e9c72ce1803f7ecc",
  "type": "TRADE",
  "size": 3761.95,
  "usdc_size": 1052.80051,
  "transaction_hash": "0x22adbd5c36f4b251a68f73786e877bd2ada230dd3706cb6813db432b16eb0c4e",
  "price": 0.27,
  "token_id": "91027047039515555078480262066003399273625964896522601460638681904234161231814",
  "side": "BUY",
  "outcome_index": 1,
  "title": "Celtics vs. Raptors",
  "slug": "nba-bos-tor-2026-11-11",
  "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/super+cool+basketball+in+red+and+blue+wow.png",
  "event_slug": "nba-bos-tor-2026-11-11",
  "outcome": "Raptors"
}
```

### Type 2: `REDEEM`
```json
{
  "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
  "timestamp": 1794379641,
  "condition_id": "0xa95eb79c1280e453dea03f7eb0263c1c03dff2b7d6788c25d66b35e6d87ddaf8",
  "type": "REDEEM",
  "size": 22499.718591,
  "usdc_size": 22499.718591,
  "transaction_hash": "0xd5310dc0787ab9b04d967ba234e1d7dcc1ee8cd42a878ef5c95bf25a6a88c0cb",
  "price": 0.0,
  "token_id": "84271288087190837093913740296552838233678887887311897621545780683503965079452",
  "side": "",
  "outcome_index": 1,
  "title": "Thunder vs. Trail Blazers",
  "slug": "nba-okc-por-2026-11-10",
  "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/super+cool+basketball+in+red+and+blue+wow.png",
  "event_slug": "nba-okc-por-2026-11-10",
  "outcome": "Trail Blazers"
}
```

### Type 3: `MAKER_REBATE`
```json
{
  "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
  "timestamp": 1794357925,
  "condition_id": "",
  "type": "MAKER_REBATE",
  "size": 130.0517,
  "usdc_size": 130.0517,
  "transaction_hash": "0xef80af1e73fc5472f73c47b18d116903ff1e8f2b68f72232397fc0775d7fe2f0",
  "price": 0.0,
  "token_id": "",
  "side": "",
  "outcome_index": 999
}
```

### Type 4: `TAKER_REBATE`
```json
{
  "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
  "timestamp": 1794355809,
  "condition_id": "",
  "type": "TAKER_REBATE",
  "size": 36.8402,
  "usdc_size": 36.8402,
  "transaction_hash": "0xebfbb9b1929bb29608775b4c186e702dc033fdf24c0d06f25e6ca928ff804b7d",
  "price": 0.0,
  "token_id": "",
  "side": "",
  "outcome_index": 999
}
```

### Type 5: `REFERRAL_REWARD`
```json
{
  "proxy_wallet": "0x951634357346bb291df117f9ac410100feea2104",
  "timestamp": 1794357303,
  "condition_id": "",
  "type": "REFERRAL_REWARD",
  "size": 36.5424,
  "usdc_size": 36.5424,
  "transaction_hash": "0x7d726610252a3239a54020e6a93c682b781169a9c0920bce68a85ecc7c156de0",
  "price": 0.0,
  "token_id": "",
  "side": "",
  "outcome_index": 999
}
```

### Type 6: `YIELD`
```json
{
  "proxy_wallet": "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92",
  "timestamp": 1790640915,
  "condition_id": "",
  "type": "YIELD",
  "size": 0.0332,
  "usdc_size": 0.0332,
  "transaction_hash": "0x555b7a997e9daf55d56bec08221d0b798bfcb30b6993bfb949f1c38c23c903f4",
  "price": 0.0,
  "token_id": "",
  "side": "",
  "outcome_index": 999
}
```

### Type 7: `MERGE`
```json
{
  "proxy_wallet": "0x204f72f35326db932158cba6adff0b9a1da95e14",
  "timestamp": 1787732173,
  "condition_id": "0x244594c21b55f9f07af5a288ff9f41ff77324aa24b6fcc6f8341337c23c62806",
  "type": "MERGE",
  "size": 6994.8506,
  "usdc_size": 6994.8506,
  "transaction_hash": "0xab9e20f4b6a7bc9c55a830d0a3d8517a84dba57a4c73800cf2c295d322a8e97c",
  "price": 0.0,
  "token_id": "",
  "side": "",
  "outcome_index": 999,
  "title": "Will Real Madrid CF win on 2026-08-26?",
  "slug": "lal-rea-rso-2026-08-26-rea",
  "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/league-lal.png",
  "event_slug": "lal-rea-rso-2026-08-26"
}
```

---

## 6. Polymarket CLOB Market WebSocket (`wss://.../ws/market`)

### Connection & Subscription
* **URL:** `wss://ws-subscriptions-clob.polymarket.com/ws/market`
* **Subscription Format:**
  ```json
  {
    "type": "market",
    "assets_ids": ["35090254409399344650860070211816464534198960801264823587796928404808383727584"],
    "custom_feature_enabled": true
  }
  ```

### Heartbeat / Keep-alive
* **Requirement:** Client sends raw text `"PING"` every 10 seconds.
* **Server Response:** The server immediately responds with raw text `"PONG"`.
* **Disconnection Test:** Connection was dropped after 30.8s; client automatically reconnected at 31.9s, re-subscribed, and continued receiving events without message corruption.

### Payload Structure
* **Array vs Object Structure:** The server sends initial book snapshots as a JSON array (`list`) of event objects, whereas continuous live updates arrive as single JSON objects. The parser accepts both `list[dict]` and `dict`.

### Raw WebSocket Samples

#### Sample A: `book` Snapshot
```json
{
  "market": "0xa5f8a44504de30fa36fff2406b2b27b3d15476ed50448e4fc3ead4861d8a5e3e",
  "asset_id": "35090254409399344650860070211816464534198960801264823587796928404808383727584",
  "timestamp": "1794427587140",
  "hash": "781505986fbca895c57868567fe8339dc38ed308",
  "bids": [
    {"price": "0.106", "size": "415.34"},
    {"price": "0.105", "size": "600"}
  ],
  "asks": [
    {"price": "0.107", "size": "105"},
    {"price": "0.108", "size": "715.71"}
  ],
  "tick_size": "0.001",
  "event_type": "book",
  "last_trade_price": "0.107"
}
```

#### Sample B: `price_change`
```json
{
  "market": "0xa5f8a44504de30fa36fff2406b2b27b3d15476ed50448e4fc3ead4861d8a5e3e",
  "price_changes": [
    {
      "asset_id": "17037715451587457598270390992151934195448806241696976473961259598235638233970",
      "price": "0.869",
      "size": "10620.85",
      "side": "BUY",
      "hash": "8ac48831c261bc4bbd08fde1e718e2bf62611e1c",
      "best_bid": "0.893",
      "best_ask": "0.894"
    },
    {
      "asset_id": "35090254409399344650860070211816464534198960801264823587796928404808383727584",
      "price": "0.131",
      "size": "10620.85",
      "side": "SELL",
      "hash": "6cff19438917b6850d3278ec28b1542c5fce75ff",
      "best_bid": "0.106",
      "best_ask": "0.107"
    }
  ],
  "timestamp": "1794427592971",
  "event_type": "price_change"
}
```

#### Sample C: `last_trade_price`
```json
{
  "market": "0xc8cfc009e5ef7798f16727298e98712da56877f3c78d0bd1a69992c91371ef69",
  "asset_id": "60726126951539130594457745308006473154344457472431098929350080992899330708790",
  "price": "0.63",
  "size": "5",
  "fee_rate_bps": "0",
  "side": "SELL",
  "timestamp": "1794428495592",
  "event_type": "last_trade_price",
  "transaction_hash": "0x3296bad9574c03191bab1f18825b9e5ed92d18110f03cee102fdfc4d89d9edf9"
}
```

---

## 7. List of Active Open Items / TODOs

1. `TODO(verify)`: **Open Question 4** — Real `eth_getLogs` limits of the selected Polygon RPC provider. (Deferred to chain-slice phase per user agreement).
2. `TODO(verify)`: **Open Question 5** — Accounting policy for resolved markets where a trader holds winning shares but has never executed an on-chain `REDEEM`. (To be specified in Phase 2/3).
