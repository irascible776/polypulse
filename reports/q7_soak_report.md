# Extended RSS soak — 2026-11-15 UTC

All numbers below are MEASURED. The worker ran 1800.041 seconds, exited 0,
completed 3 loop D cycles and 60 successful wallet polls, with 0 failed polls,
0 inserted source rows and 0 logged error events. The monitor's total time was
1810.22 seconds. RSS had start/min 376 KB, maximum 130228 KB, final sample
129800 KB. The first sample was taken immediately after process creation.

The complete ten-second series is `soak_rss_1800.csv`. Timestamps below are UTC.

| D cycle | Start | Completion | Previous RSS sample (time, KB) | Sample closest to 20s after completion (time, KB) | Change KB |
|---|---|---|---|---|---:|
| 1 | 19:51:29.912198 | 19:51:35.335402 | 19:51:28.829287, 376 | 19:51:58.832973, 128972 | +128596 |
| 2 | 20:01:29.968464 | 20:01:34.387476 | 20:01:28.891889, 125028 | 20:01:58.897938, 130184 | +5156 |
| 3 | 20:11:30.002718 | 20:11:33.347755 | 20:11:28.975680, 130228 | 20:11:48.978439, 129764 | -464 |

The first comparison includes process startup. RSS rose around cycle 2, but did
not rise around cycle 3; these samples do not establish the memory cause.

| Wallet | Superseded before → after | All dedup groups | Gaps | Pairable trade groups | Duplicate live-key groups |
|---|---:|---:|---:|---:|---:|
| Apple | 2 → 2 | 2 | 0 | 0 | 0 |
| Jn | 0 → 0 | 0 | 0 | 0 |

The 2 Apple dedup groups contain the already superseded pairs. `verify_rebuild.py`
passed. One fresh read-only reconciliation at block 97379082 returned Apple
MATCH 428 / MISMATCH 0 / IN_FLIGHT 0 / UNKNOWN 0 and Jn MATCH 95 / MISMATCH 0 /
IN_FLIGHT 0 / UNKNOWN 0. The bounded worker exited and no worker process remained.

Backup: `backups/polypulse_q7_20261116_012038.dump`, 2428336 bytes, SHA-256
`a26accd40561d91c137f59cf4d702f5004005c6f8e216f922fe51eb2c0cf1616`.
