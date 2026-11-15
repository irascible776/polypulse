# Decision 008: Auditable market marks

Status: in progress. Implementation and verification are not yet complete.

For each token, select an agreed chain-confirmed resolution, a fresh valid live
quote, the newest valid stale two-sided live quote, a last-trade tick, or unpriced,
in that order. A last-trade
tick has an unknown execution time and is not classified as fresh. Ignore
historical `resolved` ticks in `market_prices`. A live quote is
valid only when both bid and ask exist and their spread is at most the configured
maximum; last trades require a price from zero through one. Pre-0009 quotes
without bid and ask cannot be used. A closed market with chain denominator zero
uses last trade only and is flagged closed but unresolved.

Resolved payouts are the chain payout numerator for the outcome divided by the
denominator; they need not be zero or one. Open shares at that price are
estimated, not realized cash. Resolved marks do not become stale. The live
quote observation-age threshold and spread limit are configurable. A last
trade's `observed_at` is when this service polled CLOB; it is not execution time.
The API reports `price_time_known=false`, `staleness_seconds=null`, and
`is_stale=true` for those marks. The dashboard labels them time unknown.
When no fresh quote exists, a valid stale quote wins over a newer last-trade
observation because its observation age is known; the last trade's execution
time is not. The quote remains explicitly stale.

The CLOB last-trade response documents price and side, but no execution time or
trade identifier: https://docs.polymarket.com/api-reference/market-data/get-last-trade-price.
The public trade feed does have token IDs and timestamps:
https://docs.polymarket.com/api-reference/feeds/list-trades. It does not document
an identifier in the CLOB response with which to match a specific feed trade.
TODO(verify): establish a reliable correspondence before assigning a feed
timestamp to a CLOB last-price observation. The individual endpoint documents
an empty side as the no-trade default; the worker conservatively rejects it.
TODO(verify): the batch reference does not state whether it uses the same default.

Polymarket documents using the last traded price when the bid-ask spread is
greater than 0.10: https://docs.polymarket.com/concepts/prices-orderbook.
Its book API documents bid and ask price/size fields:
https://docs.polymarket.com/api-reference/market-data/get-order-book.
Gamma's market documentation describes outcome labels and token identifiers:
https://docs.polymarket.com/market-data/market-details.

The Gamma schema lists `umaResolutionStatus` as a string; the official SDK
enumerates its lifecycle values, including `resolved`:
https://github.com/Polymarket/py-sdk/blob/main/src/polymarket/models/gamma/market.py.
TODO(verify): The Gamma API reference does not specify an authoritative winner field. The resolution
endpoint documents a payouts array but does not specify its encoding:
https://docs.polymarket.com/api-reference/markets/get-resolution-state.
Do not turn an ambiguous or unsupported observation into a 0/1 resolved mark.
Polymarket also documents possible 50/50 resolutions, which do not have a single
winner: https://docs.polymarket.com/concepts/resolution.

Existing resolved ticks have no stored winner evidence and existing live ticks
have no stored bid/ask. They cannot independently establish that a mark meets
this policy. Historical observations must not be treated as current evidence.

The nine AppleTiiime 0.500000 ticks were recorded within 0.12 seconds; their
writer is unknown. A later CLOB check found no book for seven tokens. The other
two had last trades of approximately 0.026 and 0.035. Current books do not prove
what was in the books when the historical ticks were written.

The CTF payout mappings and denominator calculation are documented in the
contract source: https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol.
Polymarket's deployed CTF address is listed at
https://docs.polymarket.com/resources/contracts. The Gamma market endpoint and
condition filter are documented at https://docs.polymarket.com/api-reference/markets/list-markets.
Gamma `closed` can mean closed without resolution, so it is not alone proof of
resolution: https://docs.polymarket.com/market-data/market-details.
The source activity `outcome` and `token_id` fields used for labels are shown at
https://docs.polymarket.com/api-reference/feeds/list-account-activity.

Before the Phase 5A-2 worker handover, `tracked_wallets` was empty. The user
authorized registering AppleTiiime and JnStPrdctns from `wallets.yaml` with
idempotent inserts; the synthetic fixture wallet is excluded. Registration
does not itself start a backfill. The ingestion-status listing and price-update
broadcasts also read this table.
