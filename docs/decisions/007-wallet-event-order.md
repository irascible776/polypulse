# Decision 007: Wallet-wide event ordering

The ingestion writer assumes one writer per wallet. Each writer locks its
source-specific cursor, starts after the greater of that cursor's order and
the wallet's largest stored order, and deduplicates event identities across
sources. The unique `(wallet, api_order)` constraint makes allocation
collisions fail loudly; the source-specific cursor lock does not serialize
writers using different sources.

Money arithmetic uses Python `Decimal` with precision 28. Summing ledger
realized PnL at arbitrary precision can differ from the precision-28 running
total by roughly 1e-23 USDC (the measured difference can vary with the wallet).
The handover's PnL-change invariant uses a tolerance of 1e-12 USDC;
`scripts/verify_rebuild.py` currently uses a stricter 1e-18 tolerance.

The fixture cursor and fresh live cursor previously competed for the same
wallet's order sequence, and identities could be repeated across sources.
Migration 0008 adds wallet-wide unique keys for order and event identity. The
writer continues after the wallet maximum and skips an already stored identity.

In a scratch late-event drill, **MEASURED** baseline realized PnL was
40586.90924355597797048729560 USDC and the incorrect result was
45795.97149355597797048729550 USDC: an error of approximately 5,209.06 USDC.
No chain reference block applies to this database-only drill. Marking a late
event now sets `recompute_needed`; catch-up recomputes in `(event_time,
api_order)` order. The corrected drill matched the baseline economic state.

The chain table had previously been contaminated by decoding contract-wide
logs against the first wallet and unfiltered receipt logs. **MEASURED** counts
were 5,694/5,739 for AppleTiiime and 90/109 for JnStPrdctns; no reference block
was recorded for those database counts. At block **97308708 MEASURED**, a
24,890.584518-share mismatch followed the source-event cutoff; after a live
poll and projection, block **97309047 MEASURED** showed 406/406 and 95/95
known tokens matching for AppleTiiime and JnStPrdctns respectively. The cutoff
is the most recent event time, not evidence of the most recent successful poll.
