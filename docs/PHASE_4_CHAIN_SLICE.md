# Phase 4 Specification: Chain Slice & Polygon RPC Headline Experiment

**Status:** Approved by Claude (`claude_2`) on 13 Nov 2026.  
**Scope:** Deliberately a time-boxed experiment, not a production on-chain indexer (Phase 2/3 already own the Data-API-based ingestion path).

---

## 1. Objective
Independently verify the off-chain accounting against on-chain ground truth by pulling a targeted slice of Polygon chain data for the two tracked wallets, and specifically test where on-chain token movements originate (CTF splits, merges, redemptions, and secondary transfers).

---

## 2. Scope Boundaries
1. **No full archive-node sync**, no generalized chain indexer, no permanent live polling service.
2. Operates on the existing two tracked wallets:
   - `AppleTiiime`: `0x951634357346bb291df117f9ac410100feea2104`
   - `JnStPrdctns`: `0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92`
   and filters on the active condition/token set already present in `positions`.
3. One-shot / batch script (`scripts/pull_chain_slice.py`), not a background daemon.
4. Additive and read-only against existing Phase 2 & 3 accounting invariants (Rule 4, idempotent replay, chaos recovery).

---

## 3. Deliverables

### 3.1 Decision Note 006: RPC Provider & On-Chain Data Model (`docs/decisions/006-rpc-provider-and-onchain-model.md`)
- Chosen RPC provider (public Polygon RPCs vs free-tier Alchemy/Infura/Ankr endpoints) with rate limits and `eth_getLogs` block-range caps.
- Gnosis Conditional Tokens Framework (CTF) contract address on Polygon (`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`), and exact event signatures to decode:
  - `PositionSplit(address indexed stakeholder, address collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)`
  - `PositionsMerge(address indexed stakeholder, address collateralToken, bytes32 indexed parentCollectionId, bytes32 indexed conditionId, uint256[] partition, uint256 amount)`
  - `PayoutRedemption(address indexed redeemer, address indexed collateralToken, bytes32 indexed parentCollectionId, bytes32 conditionId, uint256[] indexSets, uint256 payout)`
  - ERC-1155 `TransferSingle(address indexed operator, address indexed from, address indexed to, uint256 id, uint256 value)`
  - ERC-1155 `TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)`
- Polymarket Proxy-Wallet verification: Confirm whether tracked addresses hold CTF ERC-1155 tokens directly or through a proxy contract.

### 3.2 Database Migration 0004: `chain_events` Table
- Create append-only `chain_events` table for decoded Polygon logs:
  - `id` bigserial primary key
  - `wallet` text not null
  - `block_number` bigint not null
  - `block_time` timestamptz not null
  - `transaction_hash` text not null
  - `log_index` int not null
  - `contract_address` text not null
  - `event_name` text not null (e.g. `TransferSingle`, `TransferBatch`, `PositionSplit`, `PositionsMerge`, `PayoutRedemption`)
  - `condition_id` text null
  - `token_id` text null
  - `from_address` text null
  - `to_address` text null
  - `amount` numeric not null
  - `raw_log` jsonb not null
  - `created_at` timestamptz not null
  - Unique constraint on `(transaction_hash, log_index)`.

### 3.3 Chain-Slice Puller Script (`scripts/pull_chain_slice.py`)
- Query Polygon RPC via `eth_getLogs` with adaptive block-range chunking and exponential backoff retry.
- Decode event logs using standard ABI signatures.
- Store raw decoded events into `chain_events`.

### 3.4 Reconciliation Report (`scripts/reconcile_chain_slice.py`)
- Per wallet, per `token_id`: compare net on-chain transfer-in / mint quantity against net off-chain BUY - SELL quantity from `source_events`.
- Quantify on-chain coverage percentage and report any delta.

### 3.5 Benchmarks [MEASURED]
- Measure RPC round-trip latency (min / avg / p95 / max), total logs fetched, throughput (logs/sec), and wall-clock execution time.

### 3.6 Study Guide 04 (`docs/study/04-chain-slice.md`)
- Plain-English explanation of Conditional Tokens Framework (CTF) binary splits, index sets, and redemptions.
- Worked walkthrough of raw log hex decoding (`topics`, `data`, 256-bit word alignment).
- 10 Gravia interview Q&As tailored to on-chain execution and RPC architecture.

