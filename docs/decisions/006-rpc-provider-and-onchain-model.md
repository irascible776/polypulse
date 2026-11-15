# Decision Note 006: Polygon RPC Provider & On-Chain CTF Data Model

**Date:** 13 Nov 2026  
**Status:** In progress  
**Scope:** Phase 4 Chain Slice & Polygon RPC Headline Experiment  

## Contamination recorded before rebuild

The pre-rebuild `chain_events` table contained 5,694 contaminated rows out of
5,739 for AppleTiiime and 90 out of 109 for JnStPrdctns. The puller decoded
contract-wide recent logs against `WALLETS[0]` and decoded receipt logs without
filtering for the wallet involved. Prior Phase 4 measurements and coverage
numbers are SUPERSEDED; the 2,000-block provider limit below is also superseded
by the current 10,000-block probe. Confirmation depth for the new scan is 32
blocks.

## Rebuild and handover results

- Before the rebuild, **MEASURED** AppleTiiime had 5,694 contaminated rows of
  5,739 (about 99%); JnStPrdctns had 90 of 109. No reference block was recorded
  for these database counts. Contract-wide logs were decoded as the first wallet,
  and receipt logs were not filtered for involvement. Migration 0007 changed the
  row identity and the table was repulled with wallet filters.
- Fixture and live cursors originally allocated orders independently. The 0008
  constraints and writer use the wallet-wide maximum order and identity to avoid
  duplicate events and detect order collisions. The single-writer limitation is
  described in decision 007.
- A scratch late-event drill **MEASURED** a realized PnL error of approximately
  5,209.06 USDC, with no chain reference block applicable. The late flag now
  triggers recompute in event-time order; see decision 007.
- At reference block **97308708 MEASURED**, an event-time cutoff preceded sales
  of 24,890.584518 shares and the chain comparison showed a mismatch. This was
  a stale source-event snapshot; event time alone cannot establish poll health.
  A later production poll ingested the sales.
- At reference block **97309047 MEASURED**, AppleTiiime matched 406/406 known
  tokens and JnStPrdctns matched 95/95, with zero mismatches and zero unknowns.
  These counts cover known tokens, not all possible token IDs. Money arithmetic
  uses Decimal precision 28 and the handover PnL invariant tolerance is 1e-12
  USDC; the rebuild verifier uses 1e-18.

### SPLIT stakeholder observation

Receipt `0x81f71a911cd09f37e6d3236992eb816391df714abe1ac6035a5090dfc8cd239b`,
log index `0x19b`, contains a CTF `PositionSplit` whose indexed stakeholder is
`0xada100874d00e3331d00f2007a9c336a65009718`. This was read directly from
the transaction receipt and decoded from topic 1. It is neither tracked
wallet. **TODO(verify):** establish whether that address is an exchange,
adapter, or another participant; the receipt alone does not establish its role.
The [official contract list](https://docs.polymarket.com/resources/contracts)
currently lists a different `CtfCollateralAdapter` address, so it does not
verify this stakeholder's role.

---

## 1. Context & Objective
In Phase 2 and 3, PolyPulse ingested historical activity and calculated average-cost-basis PnL strictly from the Polymarket Data API v2.
Phase 4 executes an independent, on-chain ground truth verification against the Polygon blockchain to confirm:
1. Does the off-chain accounting engine's derived token balance match the actual ERC-1155 balance held on the Conditional Tokens Framework (CTF) contract on Polygon?
2. How do on-chain events (PositionSplit, PositionsMerge, PayoutRedemption, and ERC-1155 transfers) correspond to off-chain trade records?
3. What are the measured RPC latency and throughput characteristics on Polygon?

---

## 2. Polygon RPC Provider Selection

| Provider | Endpoint | Auth Required | Block Range Limit | Status / Measurement |
| :--- | :--- | :---: | :---: | :--- |
| **PublicNode Bor RPC** | `https://polygon-bor-rpc.publicnode.com` | **No (Open)** | 10,000 blocks [MEASURED]; prior 2,000 SUPERSEDED | **Selected (Primary)** — previous latency measurement SUPERSEDED |
| **1RPC** | `https://1rpc.io/matic` | **No (Open)** | 1,000 blocks | **Selected (Fallback)** |
| **Ankr Public** | `https://rpc.ankr.com/polygon` | Yes (API Key) | N/A | Rejected (Requires registration) |
| **Polygon-RPC** | `https://polygon-rpc.com` | Yes (Tenant key) | N/A | Rejected (HTTP 403 Disabled) |

### Adaptive Chunking & Rate-Limiting Policy:
- Default `eth_getLogs` batch block range: **100 blocks**.
- On HTTP 429 or response payload size limits: split block range in half and retry with exponential backoff (initial 0.5s, jitter $\pm 20\%$, max 5 retries).
- User-Agent header enforced on all requests: `PolyPulse/1.0`.

---

## 3. Polymarket Smart Contract Architecture on Polygon

### 3.1 Conditional Tokens Framework (CTF)
- **Contract Address:** `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` (Gnosis CTF on Polygon)
- Implements ERC-1155 multi-token standard for prediction market outcome tokens.
- Token decimals: **6 decimal places** ($1 \text{ share} = 10^6 \text{ raw on-chain units}$).

### 3.2 Polymarket Proxy Wallet Pattern
We inspected contract bytecode via `eth_getCode` for both tracked wallets:
- `AppleTiiime` (`0x951634357346bb291df117f9ac410100feea2104`): Bytecode present (EIP-1167 proxy, runtime 85 bytes, logic implementation `0x8d5f44d8779b619f50e93928ba34986175df2a0d`).
- `JnStPrdctns` (`0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92`): Bytecode present (EIP-1167 proxy, logic implementation `0x984ec0b4cae92d1e7377c1bea6fe269c254314c2`).

**Crucial Finding:** The proxy wallet address is the direct entity holding the ERC-1155 outcome tokens on the CTF contract. Off-chain Data API addresses and on-chain token holders are **identical 1:1**.

### 3.3 Empirical On-Chain Balance Verification [MEASURED]
We queried live on-chain balances via `eth_call balanceOf(address,uint256)` against the CTF contract:
1. **AppleTiiime:**
   - Token `60150478766427655464939988524322736170735963119451703309436561797949171579582`
   - Off-Chain Derived Balance: `4981.00` shares
   - On-Chain CTF Balance: `4981000000` raw units = `4981.000000` shares
   - **Discrepancy:** **0.000000 shares (Exact Match)**.
2. **JnStPrdctns:**
   - Token `43873895024879972348799332304583852450961083930689783649843650004302588743583`
   - Off-Chain Derived Balance: `259422.998885` shares
   - On-Chain CTF Balance: `259422998885` raw units = `259422.998885` shares
   - **Discrepancy:** **0.000000 shares (Exact Match)**.

---

## 4. On-Chain Event Signatures & Topics

| Event Name | Topic 0 (Keccak-256 Hash) | Decoded Fields |
| :--- | :--- | :--- |
| **`TransferSingle`** | `0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62` | `operator, from, to, id, value` |
| **`TransferBatch`** | `0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb` | `operator, from, to, ids[], values[]` |
| **`PositionSplit`** | `0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298` | `stakeholder, collateralToken, parentCollectionId, conditionId, partition[], amount` |
| **`PositionsMerge`** | `0x09da510257321e03a985cc7ec0d53c6e94a86ce880ee111718335b2e535a0928` | `stakeholder, collateralToken, parentCollectionId, conditionId, partition[], amount` |
| **`PayoutRedemption`** | `0x2e6840742f56b6c2c8f8b89e3a35a60e0a5c48b111666ff0597143922da50937` | `redeemer, collateralToken, parentCollectionId, conditionId, indexSets[], payout` |

---

## 5. Architectural Rule
All chain-ingested events are stored in a new append-only table `chain_events`. Off-chain accounting logic and database tables (`positions`, `position_ledger`, `wallet_totals`) remain 100% read-only and decoupled.

