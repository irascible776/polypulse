# Decision Note 003: Accounting Key Fields & REDEEM Token ID Resolution

**Status:** Verified & Documented  
**Author:** PolyPulse Engineering Team  
**Scope:** Resolution of Step 3.0a and 3.0b: Verification of `token_id` and `condition_id` across historical `TRADE` and `REDEEM` records, and deterministic resolution of `REDEEM` events with empty token IDs.

---

## 1. Empirical Verification (Step 3.0a)

A database-wide query across all 3,289 historical events for tracked wallets `AppleTiiime` (`0x95163435...`) and `JnStPrdctns` (`0xa98a9d23...`) produced the following counts:

| Event Type | Total Rows | Non-Null `condition_id` | Non-Null `token_id` | Non-Null `outcome_index` |
| :--- | :---: | :---: | :---: | :---: |
| **`TRADE`** | [MEASURED] 2,976 | [MEASURED] 2,976 (100%) | [MEASURED] 2,976 (100%) | [MEASURED] 2,976 (100%) |
| **`REDEEM`** | [MEASURED] 201 | [MEASURED] 201 (100%) | [MEASURED] 197 (98.0%) | [MEASURED] 201 (100%) |

### Findings:
1. Every single `TRADE` event has a non-null `condition_id`, non-null `token_id`, and non-null `outcome_index`.
2. Every single `REDEEM` event has a non-null `condition_id` and non-null `outcome_index`.
3. Exactly **4 `REDEEM` rows** (3 in `JnStPrdctns`, 1 in `AppleTiiime`) have `token_id = NULL` (empty string `""` in the raw API response).

---

## 2. Forensic Analysis of the 4 REDEEM Rows with Missing `token_id` (Step 3.0b)

We inspected each of the 4 rows against the historical `TRADE` events for the same `(wallet, condition_id)`:

### Case 1: `JnStPrdctns` — Condition `0xa27567516c012f...` (BTC $88k)
* Raw REDEEM: `size = 297.74`, `usdc_size = 297.74`, `outcome_index = 1`, `token_id = ""`
* Prior Trades for this `condition_id` and `outcome_index = 1`:
  * BUY: `1228.74` shares (`token_id = 84416778953362665398397651082201882220957652780405483512714850725156647801481`)
  * SELL: `931.00` shares
  * **Net Shares Held:** $1228.74 - 931.00 = \mathbf{297.74}$ shares.
* **Result:** Net held shares matches the REDEEM `size` of `297.74` **to the exact decimal**. The token is unambiguously `84416778953362...`.

### Case 2: `JnStPrdctns` — Condition `0x81e3be684d83cb...` (ETH $2,200)
* Raw REDEEM: `size = 180.907393`, `usdc_size = 180.907393`, `outcome_index = 1`, `token_id = ""`
* Prior Trades:
  * BUY cumulative: `4670.907393` shares (`token_id = 57347415831125089468572798516193393192127430792416793204829653853571822237999`)
  * SELL: `4490.00` shares
  * **Net Shares Held:** $4670.907393 - 4490.00 = \mathbf{180.907393}$ shares.
* **Result:** Exactly matches REDEEM size `180.907393`. Unambiguous token match.

### Case 3: `JnStPrdctns` — Condition `0x1c5999038a75cc...` (ETH $2,700)
* Raw REDEEM: `size = 398.0`, `usdc_size = 398.0`, `outcome_index = 1`, `token_id = ""`
* Prior Trades:
  * BUY cumulative: `4559.0` shares (`token_id = 82657229606472284301148972242553245032321096225420688267401874624776458100965`)
  * SELL: `4161.0` shares
  * **Net Shares Held:** $4559.0 - 4161.0 = \mathbf{398.0}$ shares.
* **Result:** Exactly matches REDEEM size `398.0`. Unambiguous token match.

### Case 4: `AppleTiiime` — Condition `0x028dc9f74bd4f1...` (Celtics vs. Pistons)
* Raw REDEEM: `size = 9999.977693`, `usdc_size = 9999.977693`, `outcome_index = 1`, `token_id = ""`
* Prior Trades:
  * 11 BUY trades totaling: $\mathbf{9999.977693}$ shares (`token_id = 6194634649894872222114139800075806437965693537103509072979175541757029916386`)
  * SELL: `0` shares
  * **Net Shares Held:** $\mathbf{9999.977693}$ shares.
* **Result:** Exactly matches REDEEM size `9999.977693`. Unambiguous token match.

---

## 3. Policy & Resolution Architecture

In Gnosis Conditional Tokens Framework (ERC-1155) used by Polymarket:
1. Each outcome token is uniquely identified by `(condition_id, outcome_index)`.
2. The `token_id` is a deterministic hash of the collateral token address, condition ID, and index set.
3. When the Polymarket Data API returns a `REDEEM` event without a `token_id`, the position being closed is resolved deterministically in the accounting engine using:
   ```python
   # Resolution order:
   # 1. If event.token_id is present: use event.token_id directly.
   # 2. If event.token_id is empty/NULL: lookup the open position for (wallet, condition_id, outcome_index).
   ```
4. If multiple open positions match, write the event to `position_ledger_rejects` with `reject_reason = "AMBIGUOUS_REDEEM_TOKEN"`. If none match, use `reject_reason = "UNMATCHED_REDEEM_TOKEN"`.

---

## 4. `api_order` Contiguity Verification (Step 3.0c)

We executed window function queries over all 3,289 stored events across both wallets:
* `AppleTiiime`: [MEASURED] Min: `1`, Max: `2,454`, Count: `2,454`, Gaps: `0`.
* `JnStPrdctns`: [MEASURED] Min: `1`, Max: `835`, Count: `835`, Gaps: `0`.

`api_order` is confirmed strictly monotonic and contiguous with zero gaps per wallet.

