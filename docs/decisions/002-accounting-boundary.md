# Decision Note 002: Accounting Boundary (DRAFT)

**Status:** DRAFT (Phase 1 Baseline)  
**Author:** PolyPulse Engineering Team  
**Scope:** Defines which Polymarket event types mutate wallet positions and PnL, versus which event types are preserved in the raw append-only store and counted without mutating positions.

---

## 1. Principles
1. **Raw Completeness:** Every ingested event, regardless of type, is permanently stored in `source_events` with its raw payload, observed timestamp, and deduplication key.
2. **Deterministic Projection:** Only well-defined state-transition events modify the wallet ledger.
3. **No Phantom Math:** We never guess payouts or assume $1.00; payouts must come from explicit `REDEEM` records.

---

## 2. Event Classification Matrix

| Event Type | Action in Accounting Engine | Impact on Position | Impact on PnL | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **`TRADE` (BUY)** | **Supported** | Increases token share balance; increases total cost basis. | None realized immediately (unrealized PnL updates via mark-to-market). | Average cost basis recalculation: $C_{new} = \frac{C_{old} \cdot S_{old} + \text{usdc\_size}}{S_{old} + \text{size}}$. |
| **`TRADE` (SELL)** | **Supported** | Decreases token share balance. | **Realized PnL** = $\text{usdc\_size} - (\text{size} \cdot \bar{C})$. | If entire position is closed, remaining cost basis drops to 0. |
| **`REDEEM`** | **Supported** | Closes outcome shares down to 0. | **Realized PnL** = $\text{usdc\_size} - (\text{size} \cdot \bar{C})$. | Uses exact `usdc_size` from event payload; never assumes $1.00/share. |
| **`MAKER_REBATE`** | **Saved & Counted** | None (0 shares affected). | None in MVP. | Saved to raw store; counter incremented. Cash balance is excluded in MVP. |
| **`TAKER_REBATE`** | **Saved & Counted** | None (0 shares affected). | None in MVP. | Saved to raw store; counter incremented. |
| **`REFERRAL_REWARD`** | **Saved & Counted** | None (0 shares affected). | None in MVP. | Saved to raw store; counter incremented. |
| **`YIELD`** | **Saved & Counted** | None (0 shares affected). | None in MVP. | Saved to raw store; counter incremented. |
| **`SPLIT`** | **Saved & Counted (Deferred)** | None in MVP. | None in MVP. | Minting complete sets (Yes + No) from collateral. To be addressed in later phase if present. |
| **`MERGE`** | **Saved & Counted (Deferred)** | None in MVP. | None in MVP. | Burning complete sets (Yes + No) back to collateral. To be addressed in later phase if present. |

---

## 3. Unresolved Open Question (TODO)
* **`TODO(verify)` (Open Question 5):** How should the system evaluate a position when a market resolves, but the user has **not yet redeemed**?
  * *Option A:* Leave position open at mark-to-market price of the winning/losing outcome ($1.00 or $0.00).
  * *Option B:* Automatically compute synthetic resolution when the market resolution event is observed on the WebSocket / Gamma API.
  * *Status:* To be settled prior to Phase 2 (Accounting Engine implementation).

