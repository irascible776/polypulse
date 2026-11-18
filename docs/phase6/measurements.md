# Phase 6 local measurements

These are synthetic tests on one local machine, not Polymarket production traffic.
The live ingestion source remains the Polymarket public API. Measurements are
single observations, not capacity guarantees or statistically controlled comparisons.

The benchmark script has since been extended with an enrichment supersession
scenario and a synthetic backlog-age upper bound. Neither addition has been run
or measured yet. All synthetic events are treated as available at the start; the
bound uses the last wallet-cycle completion and includes post-projection bookkeeping.
It is not an observed live Polymarket backlog or an event-by-event age distribution.

## Mixed workload, 30,000 events

The benchmark used two local PostgreSQL databases, 30 wallets and 30,000 generated
events, with 20% assigned to one hot wallet. Each worker process started a worker
for each shard. One run was made for each worker count. Concurrent API measurements
used in-process ASGI requests, not a real network or the proxy.

| Worker processes | Elapsed seconds | Events/second | API p95 milliseconds |
| --- | ---: | ---: | ---: |
| 1 | 37.057 | 809.55 | 26.60 |
| 2 | 31.633 | 948.38 | 33.85 |
| 4 | 33.289 | 901.19 | 40.44 |

Each run reported zero added rows on duplicate delivery. A late correction replayed
1,001 events from a checkpoint versus 6,001 in a full replay; the benchmark
compared economic state, ledger entries and rejection records. Split/merge events
are recorded as unsupported, rather than accounted for. The results are in
`mixed-30000-results.json`. These runs used an earlier development version; later
safety and recovery fixes are not measured by these numbers.

The benchmark command was `python -m scripts.phase6_scale_benchmark --events 30000
--wallets 30 --workers N`, with freshly initialized scratch shards for each N.
The tested machine had two physical CPU cores and four logical CPUs. Concurrent
local work and caching can affect the numbers; adding processes did not produce
monotonic improvements in this sample.

## Mixed workload, 1,000,000 events

The first run used one worker process with a worker per shard, 100 wallets, and a 200,000-event
hot wallet. Ingestion and projection took 1,367.124 seconds (731.46 events/second).
The in-process API p95 was 34.12 ms across 12,101 successful responses. Duplicate
delivery added zero events. A late correction replayed 5,001 events in 3.082 seconds;
a full replay processed 200,001 events in 70.851 seconds, with economic state,
ledger and rejection records compared. The two databases occupied approximately
1.40 GB and 1.04 GB as measured by PostgreSQL. Other verification ran concurrently.

The command was `python -m scripts.phase6_scale_benchmark --events 1000000 --wallets
100 --workers 1` against freshly initialized local scratch shards. This run used a
frozen development snapshot with migrations through `0020`, before subsequent
wallet-validation, backfill-recovery and shard-guard changes. It does not measure the
exact final build. Raw measurements are in `mixed-million-results.json`.
Hashes of selected snapshot files are in `million-snapshot-sha256.txt`.

The same workload and frozen snapshot with `--workers 2` took 1,114.724 seconds
(897.08 events/second), with in-process API p95 of 40.33 ms over 9,590 successful
responses. Duplicate delivery added zero events. Checkpoint replay processed 5,001
events in 2.172 seconds, versus 200,001 events in 72.758 seconds for full replay;
the state, ledger and rejection comparisons passed.

With `--workers 4`, the workload took 1,037.047 seconds (964.28 events/second),
with in-process API p95 of 41.40 ms over 8,885 successful responses. Duplicate
delivery added zero events. Checkpoint replay processed 5,001 events in 2.902
seconds, versus 200,001 events in 75.356 seconds for full replay; the state,
ledger and rejection comparisons passed. These single runs show diminishing
returns on this host and should not be extrapolated to other deployments.

## COPY and chunked insertion, final build

Four sequential runs ingested and projected 10,000 synthetic buy events each,
using 2,000-event input batches and fresh wallets in the same scratch database.
The order was chunked, COPY, COPY, chunked. All runs stored and projected 10,000
events. Ingestion took 11.446 and 10.796 seconds for chunked inserts, and 2.757 and
2.728 seconds for COPY. Projection took 4.370, 4.778, 4.831 and 4.608 seconds,
respectively. This is a small local comparison; database contents and caches changed
between runs, and it does not measure network or API fetching.

The command was `python -m scripts.phase6_baseline --events 10000 --batch-size
2000`, with `INGESTION_COPY_THRESHOLD=0` for chunked and `=1` for COPY, against
a scratch `polypulse_benchmark` database migrated through `0022`.
