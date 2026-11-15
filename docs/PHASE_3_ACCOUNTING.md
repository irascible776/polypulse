# PHASE 3: ACCOUNTING & PNL ENGINE
Settling the replay-testing and acceptance plan

Settling the replay-testing and acceptance plan
PHASE 3: ACCOUNTING & PNL ENGINE

Read PROJECT_BRIEF.md, docs/decisions/002-accounting-boundary.md, and PHASE_2_INGESTION.md
first. Follow the AI rules in the brief. Stay inside this phase’s scope.

Goal (plain words)

Turn the append-only source_events log into a correct, rebuildable picture of
what each wallet owns and how much money it has made or lost — using only
exact decimal math, never guessing a payout, and never touching the raw log.
If we delete every derived table and replay, we must get back exactly what we
had.

In scope

Schema for positions, wallet totals, and an audit ledger. A deterministic
projector that replays source_events in api_order. Average-cost-basis
accounting for TRADE and REDEEM. Late-event recompute under a wallet lock.
Idempotency and concurrency guarantees. Unit, property, and integration tests.
Settling Open Question 5 (resolved-but-unredeemed positions).

Out of scope

The dashboard, push/websocket serving, the chain source (B), SPLIT/MERGE
accounting (events are saved and counted per 002, but still do not mutate
positions — deferred to a later phase if they appear in volume), table
partitioning (Phase 4), leaderboard ranking logic.

Approved decisions & spec updates
Position granularity: one row per (wallet, condition_id, token_id) —
the same token can appear in TRADE and REDEEM rows; outcome_index is stored
for display but is not part of the key, since token_id already identifies
the specific outcome share.
Cost basis: average cost, Decimal throughout, using the formulas already
fixed in 002 section 2. Cash always comes from usdc_size, never from
price_reported * size. This was already true in Phase 2 storage; Phase 3
just never deviates from it.
Open Question 5 is now settled — Option A, “leave it open”:
A resolved-but-unredeemed position is not auto-closed and no synthetic
REDEEM is invented. Shares and average cost basis stay exactly as the last
TRADE left them. Realized PnL is untouched. A position’s redemption status
(resolved? redeemed?) is a serving-time concern (Gamma API lookup),
outside this phase. If Phase 5 wants a mark-to-market “paper value” for a
resolved-unredeemed position, it must be computed at serving time from
Gamma’s resolution outcome, kept out of the wallet_totals.realized_pnl
column entirely, and labeled ESTIMATED wherever it is shown. This keeps
“No Phantom Math” (002 §1.3) intact: the accounting engine itself never
assumes $1.00 or $0.00 a share.
Late-event recompute strategy: full wallet replay, not a patch. Given
our volumes (hundreds to low thousands of events per wallet), a full replay
from source_events is cheap, trivially correct, and avoids ever having to
reason about “undoing” a partial average-cost mutation. We do not attempt
incremental late-event surgery in the MVP.
Audit ledger: every projector application of an event writes one
immutable ledger row recording exactly what changed. This is what makes
rule 4 (“every derived number must be rebuildable”) checkable in practice —
we can prove positions by re-summing position_ledger.
Locking primitive: SELECT ... FOR UPDATE on a per-wallet row in a new
wallet_totals table (not a Postgres advisory lock — advisory locks don’t
show up in pg_locks joined to app state the same way, and we want a real
row we can also read as “wallet summary”). One wallet’s lock never blocks
another wallet’s projector run.
Trigger model: a projector_cursors row per wallet tracks
last_applied_api_order. A projector worker loop, per wallet, compares this
against MAX(source_events.api_order) for that wallet and catches up. This
decouples ingestion (Phase 2) from accounting (Phase 3) completely — the
projector never talks to the Data API.
3.0 Close-out fixes (do these FIRST; they block the rest)
3.0a Confirm the key fields exist

Before writing the migration, query the actual ingested data for AppleTiiime
and JnStPrdctns and confirm: every TRADE and REDEEM row has a non-null
token_id and condition_id. If any are null, STOP and report — the position
key depends on them. Do not substitute a derived key without asking.

3.0b Confirm REDEEM carries outcome_index / token_id correctly

Spot-check 10 REDEEM rows against their matching TRADE rows for the same
condition_id to confirm the token being redeemed is unambiguous (REDEEM
should close out shares of one specific outcome token, not the whole market).
Record findings in docs/decisions/003-accounting-key-fields.md. If REDEEM
does not carry token_id directly, document how it’s inferred and mark it
TODO(verify) until confirmed against docs or a second source.

3.0c Confirm api_order is strictly increasing per wallet with no gaps

across the full stored history for both wallets (it should be, per Phase 2,
but Phase 3 depends on it absolutely — the projector has no other ordering
signal). Write a one-off check query and paste the output.

3.1 Database schema (one Alembic migration: 0003)

positions (current state; mutated only by the projector, in a transaction)

wallet text, condition_id text, token_id text, outcome_index int null
shares numeric (≥ 0, enforced by CHECK; selling more than held is an error,
see 3.3)
avg_cost_basis numeric (cost per share, Decimal; 0 when shares = 0)
realized_pnl numeric (cumulative, this token only)
last_applied_api_order bigint (per-token high-water mark, for debugging —
the authoritative cursor lives on projector_cursors)
updated_at timestamptz
PRIMARY KEY (wallet, condition_id, token_id)

wallet_totals (one row per wallet; this is the row we SELECT ... FOR UPDATE to serialize a wallet’s projector run)

wallet text PRIMARY KEY
total_realized_pnl numeric
total_trade_count int, total_redeem_count int, unsupported_event_count int
reject_count int (number of current reject rows for this wallet)
last_applied_api_order bigint
last_recompute_at timestamptz, recompute_count int (how many times a late
event forced a full replay — useful signal for Phase 2 overlap tuning)
updated_at timestamptz

position_ledger (append-only except during full-wallet recompute in one transaction)

id bigserial primary key
wallet text, condition_id text, token_id text
source_event_id text, api_order bigint, event_type text, event_time timestamptz
shares_before numeric, shares_delta numeric, shares_after numeric
avg_cost_before numeric, avg_cost_after numeric
cash_delta numeric (usdc_size as applied: positive for a sell/redeem
proceeds, negative for a buy cost)
realized_pnl_delta numeric
is_recompute boolean (true if this row was written by a full wallet
replay rather than incremental catch-up — lets us distinguish the two
code paths in the data itself)
created_at timestamptz
UNIQUE (wallet, source_event_id). A full recompute deletes and rebuilds the
wallet's ledger within one transaction, leaving one row per applied event.

projector_cursors

wallet text PRIMARY KEY, last_applied_api_order bigint, updated_at timestamptz

Index: position_ledger (wallet, api_order), position_ledger (wallet, token_id, api_order).

3.2 Deterministic replay / rebuild

apply_event(wallet, event) → position delta is a pure function: same
event + same prior position state → same result, every time. It is unit
tested in total isolation from the database (see 3.6).

Incremental catch-up (the common case):

BEGIN
SELECT * FROM wallet_totals WHERE wallet = :w FOR UPDATE (creates the row
if absent, inside the same transaction, so a brand-new wallet is never
raced).
Fetch source_events for this wallet where api_order > projector_cursors.last_applied_api_order, ordered by api_order ASC.
For each event, call apply_event, upsert positions, insert one
position_ledger row, accumulate wallet totals.
Update wallet_totals and projector_cursors with the new high-water
mark.
COMMIT — one transaction for the whole catch-up batch, not one per
event. (A crash mid-batch loses nothing: the cursor only advances on
commit, so a restart just re-reads from the old cursor — this mirrors the
Phase 2 idempotency story and is covered by a dedicated crash test, 3.8.)

Full recompute (triggered by a late event, or run manually):

BEGIN
SELECT ... FOR UPDATE the wallet row (same lock as above — a recompute
and an incremental catch-up for the same wallet can never run concurrently).
DELETE FROM position_ledger WHERE wallet = :w and
DELETE FROM positions WHERE wallet = :w and
DELETE FROM position_ledger_rejects WHERE wallet = :w. Reset reject_count;
rebuild the reject rows and counter during replay.
Replay every source_events row for the wallet, in api_order order,
from scratch, exactly like incremental catch-up but starting from zero
state.
Set wallet_totals.last_recompute_at = now(), increment
recompute_count, write every ledger row with is_recompute = true.
COMMIT.

This is the one and only rebuild path in the system: “replay everything for
this wallet” and “catch up from the cursor” are the same function with a
different starting cursor (0 vs. the saved one). That sameness is itself a
test (3.6).

3.3 Average cost basis math (strict Decimal, zero floats)

Implements 002 §2 exactly, plus the edge cases 002 leaves implicit:

BUY: shares_after = shares_before + size;
avg_cost_after = (avg_cost_before * shares_before + usdc_size) / shares_after.
If shares_before = 0, avg_cost_after = usdc_size / size.
SELL: realized_pnl_delta = usdc_size - (size * avg_cost_before);
shares_after = shares_before - size; avg_cost_after is unchanged if shares
remain, and resets to 0 when shares_after = 0. The ledger keeps avg_cost_before.
Selling more than is held (size > shares_before): this must never
silently go negative. Treat it as a hard error: write the event to a
position_ledger_rejects table (same columns, plus a reject_reason text
field) instead of mutating positions, increment a counter on
wallet_totals, and continue processing later events for the wallet (one
bad event must not halt the whole replay). This should be rare — it signals
either a missing earlier TRADE (gap in source_events, which Phase 2’s
dedupe/backfill is supposed to prevent) or a true accounting bug — and is
the signal to investigate, not paper over.
cash_price_gap trades: no special handling needed here — accounting
always uses usdc_size, so a trade already flagged cash_price_gap = true
by Phase 2 is accounted for correctly automatically. We only carry the flag
through to position_ledger for visibility.
Zero-size or zero-usdc_size events: apply normally (a BUY of size 0 at
cost 0 is a no-op delta but still gets a ledger row, so nothing is silently
dropped).
Everything is Decimal; the Python layer never casts to float at any
point, including intermediate division — use decimal.Decimal division
with an explicit context precision (document the chosen precision, e.g. 28
significant digits, in docs/decisions/003).
3.4 REDEEM handling

REDEEM is accounted exactly like a SELL of the wallet’s full remaining share
balance for that token, using usdc_size as proceeds:
realized_pnl_delta = usdc_size - (shares_before * avg_cost_before);
shares_after = 0 and avg_cost_after = 0; the ledger keeps avg_cost_before.
No branching is needed for “payout of 0” — a losing
redemption is simply a SELL at proceeds 0, and the formula produces a full
loss of the cost basis automatically. This is why 002’s “80 of 158 REDEEMs
paid 0” number needs no special code path, only a test asserting it (3.6).

If a REDEEM arrives for a token with shares_before = 0 (nothing held — e.g.
a gap, or a wallet that acquired shares via a mechanism we don’t track in the
MVP such as SPLIT): write to position_ledger_rejects, do not fabricate
negative shares, flag for investigation.

Resolved-but-unredeemed: see “Open Question 5 is now settled” above — no code
path exists for this in the accounting engine at all; it is simply the
absence of a REDEEM event, which requires no special handling by definition.

3.5 Late events and recompute

An event is late (Phase 2 sets is_late = true) when its event_time sorts
before the wallet’s newest already-applied event at ingestion time. Phase 3’s
job is only to detect that a late event needs a recompute and run one —
not to re-derive lateness itself (that stays Phase 2’s job and signal).

A background job polls source_events for (wallet, is_late = true) rows
not yet covered by a recompute (compare source_events.id against
wallet_totals.last_recompute_at’s corresponding event, or simpler: a
recompute_needed boolean flag added to wallet_totals, set true by a
trigger or by the ingestion poller directly when it marks an event late,
and cleared by the recompute transaction itself, inside the same
transaction as step 3.2’s full recompute — so “needed” and “cleared” can
never race).
Multiple late events for the same wallet before the recompute runs collapse
into a single recompute (the flag is idempotent — it doesn’t queue one
recompute per late event).
3.6 Concurrency & idempotency guarantees
Single writer per wallet: the wallet_totals row lock (FOR UPDATE)
guarantees only one projector run — incremental or recompute — is ever
mutating a given wallet’s positions at a time. Two wallets’ projectors run
fully in parallel (hot state is never sharded per the brief, but it is also
never globally serialized).
Re-running the same catch-up twice is a no-op: because the cursor only
advances inside the same committed transaction as the writes, there is no
window where positions are updated but the cursor isn’t (or vice versa).
Re-running catch-up after a crash just re-reads last_applied_api_order
and finds nothing new.
Re-running a recompute twice produces byte-identical results: since it
always starts from a full delete-and-replay of the same immutable
source_events rows in the same api_order, this is a direct corollary of
apply_event being pure.
Rebuildability (rule 4): a top-level script, scripts/verify_rebuild.py,
re-sums position_ledger per wallet/token and asserts it exactly equals
positions. This is run as part of CI, not just ad hoc.
3.7 Tests (all required)

Unit (pure apply_event, no database):

Single BUY, then query avg cost and shares.
Two BUYs at different prices → correct weighted average (hand-computed
worked example, exact Decimal match, not “close enough”).
BUY then partial SELL → avg cost of the remainder is unchanged;
realized PnL matches usdc_size - size*avg_cost exactly.
BUY then full SELL → shares = 0, avg cost = 0, and the ledger retains
avg_cost_before for audit.
REDEEM with usdc_size = 0 → realized PnL = -shares*avg_cost exactly
(a full loss), shares → 0.
REDEEM with a nonzero payout → matches hand computation.
SELL/REDEEM exceeding shares held → rejected, not negative, ledger-rejects
row written, wallet processing continues past it.
Unsupported/unknown event type → saved to position_ledger’s sibling
counter in wallet_totals, zero effect on shares/PnL, no crash.
Decimal precision: a case with a repeating-decimal division (e.g.
1 / 3 shares) stays exact through the configured Decimal context and
round-trips without float contamination — assert the value is Decimal
end to end, never float, at every intermediate step (can assert via
isinstance checks in a test harness or static typing + mypy).

Property-based (Hypothesis), generating random event sequences for one
synthetic token:
10. Invariant: realized_pnl + shares*avg_cost_basis reconciles with
total cash in minus cash out across all applied TRADE/REDEEM events, to
the Decimal.
11. Order-independence of batching: splitting the same ordered event
sequence into different page/batch boundaries (simulating different
catch-up timings) and applying it via multiple incremental catch-up runs
produces the identical final positions row as applying it in one shot.
This directly tests the Phase 2 “group may split across a page” lesson
carried into Phase 3.
12. Recompute equivalence: for any sequence, incremental catch-up and a
full recompute of the same final event set yield byte-identical
positions and identical position_ledger content (ignoring
is_recompute and created_at).
13. Late-event determinism (the big one): take a sequence, apply it
incrementally; then take the same sequence with one event moved to
arrive after a later one (simulating lateness), triggering a recompute;
assert the final state equals applying the original, correctly-ordered
sequence from scratch. This is the test that actually proves the late-
event story works, not just that recompute runs.

Integration (real Postgres in Docker, using the real ingested data):
14. Full projector run over AppleTiiime’s and JnStPrdctns’ real
source_events completes without crashing, every event type from Phase 2
is accounted for (TRADE/REDEEM mutate; MAKER_REBATE/TAKER_REBATE/
REFERRAL_REWARD/YIELD/MERGE are counted, not mutating).
15. Re-run the same projector catch-up twice: zero additional ledger rows,
identical positions.
16. Manually inject one artificial late event (via a test fixture, not real
data) into the middle of AppleTiiime’s history, confirm is_late style
recompute path fires and the result matches test 13’s guarantee.
17. Concurrency test: kick off two projector runs for the same wallet at
once (simulating a scheduling bug) and confirm the row lock serializes
them — no duplicate ledger rows, no lost update.
18. scripts/verify_rebuild.py passes for both real wallets (rule 4 check).

3.8 Crash test (scripts/chaos_accounting.py)

Start a full recompute of AppleTiiime, SIGKILL mid-replay at a random point,
restart, confirm the recompute re-runs cleanly from zero (not from a
half-written state — the delete-then-replay is itself inside the one
transaction, so a kill before commit leaves the old positions/ledger
completely untouched). Repeat 5 times. Final positions,
wallet_totals.total_realized_pnl, and ledger row counts must be identical
across all 5 runs. Write results to docs/decisions/005-accounting-resilience.md.

3.9 Measurements (label every number MEASURED)
Full recompute throughput (events/sec) for each of the two real wallets.
Incremental catch-up latency: time from a new source_events row existing
to the projector’s cursor advancing past it, measured over a short live
run alongside Phase 2’s live poller.
Count of position_ledger_rejects rows produced over real data (expected:
0; if nonzero, investigate before reporting, per the brief’s “explain
before reporting” rule).
Final total_realized_pnl per wallet, reported as a concrete Decimal
number, explicitly labeled MEASURED (computed, not estimated) — this is the
number a Gravia reviewer would sanity-check first.
Definition of done
Migration 0003 applied; schema matches 3.1 exactly.
apply_event unit tests (1–9) and property tests (10–13) all pass, zero
skips.
Integration tests (14–18) pass against real Postgres with the real
AppleTiiime and JnStPrdctns data from Phase 2.
scripts/verify_rebuild.py passes for both wallets.
Crash test (3.8) passes 5 of 5 runs with identical final state.
position_ledger_rejects count on real data is 0, or is explained, not just
reported.
Every TODO(verify) opened in 3.0 is resolved or explicitly still open and
listed.
make lint and make test both pass.
docs/decisions/003 and 005 exist; PROJECT_BRIEF.md’s Open Question 5 is
marked settled with a one-line pointer to this document’s decision.
Study guide task

Write docs/study/03-accounting.md following STUDY_GUIDE_TEMPLATE.md, covering:

A plain-English analogy for average cost basis (e.g., buying the same stock
at different prices and tracking your “blended” purchase price) and why
realized PnL only crystallizes on a sell/redeem, never on a price move.
A fully worked numeric example: 3 BUYs at different prices, a partial SELL,
a REDEEM at a reduced payout — hand-traced through the exact formulas in
3.3/3.4, matching what the code produces.
Why a stateless, pure apply_event function is what makes replay and
recompute “the same code path with a different starting point,” and why
that’s safer than writing separate “catch-up” and “recompute” logic.
Why the system deliberately does not invent a $1.00/$0.00 value for a
resolved-but-unredeemed position, tying back to 002’s “No Phantom Math”
principle, and what Gravia’s own copytrading product would get wrong if it
guessed instead of waited for the REDEEM.
Out-loud interview questions for Gravia, e.g.: “Walk me through what
happens, end to end, if an event arrives three days late.” / “Why is a row
lock on wallet_totals enough concurrency control, and what would break if
positions were sharded by wallet across database shards?” / “Why didn’t you
special-case the zero-payout REDEEM?” / “What would you change if a wallet
had 50 million events instead of a few thousand?”
Send back to me for review
docs/decisions/003-accounting-key-fields.md and
docs/decisions/005-accounting-resilience.md.
scripts/verify_rebuild.py output for both real wallets.
Final positions row counts and wallet_totals.total_realized_pnl for
AppleTiiime and JnStPrdctns.
position_ledger_rejects count (and explanation if nonzero).
Full make test output, including the Hypothesis property test run (with
its example count/seed for reproducibility).
Crash test (3.8) results, 5/5.
Every open TODO(verify) remaining after this phase.
