"""Run one bounded worker and sample its resident memory on Linux."""

import argparse
import csv
import json
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=900)
    parser.add_argument("--interval", type=float, default=10)
    args = parser.parse_args()
    reports = Path("reports")
    reports.mkdir(exist_ok=True)
    rss_values = []
    stats = Counter()
    inserted = Counter()
    violations = []
    with (
        (reports / "soak_worker.log").open("w+") as log,
        (reports / "soak_rss.csv").open("w", newline="") as samples,
    ):
        csv_writer = csv.writer(samples)
        csv_writer.writerow(("timestamp_utc", "elapsed_seconds", "pid", "rss_kb"))
        proc = subprocess.Popen(
            [sys.executable, "-m", "app.worker", "--duration", str(args.duration)],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        start = time.monotonic()
        offset = 0
        try:
            while proc.poll() is None:
                try:
                    status = Path(f"/proc/{proc.pid}/status").read_text()
                    rss = int(
                        next(s.split()[1] for s in status.splitlines() if s.startswith("VmRSS:"))
                    )
                    rss_values.append(rss)
                    csv_writer.writerow(
                        (
                            datetime.now(timezone.utc).isoformat(),
                            round(time.monotonic() - start, 2),
                            proc.pid,
                            rss,
                        )
                    )
                    samples.flush()
                except (FileNotFoundError, StopIteration):
                    pass
                log.flush()
                with (reports / "soak_worker.log").open() as reader:
                    reader.seek(offset)
                    lines = reader.readlines()
                    offset = reader.tell()
                for line in lines:
                    try:
                        entry = json.loads(line)
                    except ValueError:
                        continue
                    event = entry.get("event", "")
                    if event == "loop_c":
                        stats["poll_ok"] += 1
                        inserted[entry["wallet"]] += entry.get("inserted", 0)
                    elif event == "loop_c_error":
                        stats["poll_failed"] += 1
                    if event.endswith("_error"):
                        stats["errors"] += 1
                    if event == "loop_d" and entry.get("mismatches", 0):
                        violations.append(
                            {"wallet": entry.get("wallet"), "mismatches": entry["mismatches"]}
                        )
                if violations or time.monotonic() - start > args.duration + 60:
                    proc.send_signal(signal.SIGTERM)
                    break
                time.sleep(args.interval)
            proc.wait(timeout=90)
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                proc.wait(timeout=90)
        # Include final records emitted while the worker shuts down.
        log.flush()
        text = (reports / "soak_worker.log").read_text()
        stats.clear()
        inserted.clear()
        for line in text.splitlines():
            try:
                entry = json.loads(line)
            except ValueError:
                continue
            event = entry.get("event", "")
            if event == "loop_c":
                stats["poll_ok"] += 1
                inserted[entry["wallet"]] += entry.get("inserted", 0)
            elif event == "loop_c_error":
                stats["poll_failed"] += 1
            if event.endswith("_error"):
                stats["errors"] += 1
        summary = next(
            (
                json.loads(line)
                for line in reversed(text.splitlines())
                if line.startswith('{"event": "summary"')
            ),
            None,
        )
        result = {
            "event": "soak_summary",
            "exit": proc.returncode,
            "elapsed_seconds": round(time.monotonic() - start, 2),
            "rss_start_kb": rss_values[0] if rss_values else None,
            "rss_max_kb": max(rss_values) if rss_values else None,
            "rss_end_kb": rss_values[-1] if rss_values else None,
            "poll_ok": stats["poll_ok"],
            "poll_failed": stats["poll_failed"],
            "inserted_per_wallet": dict(inserted),
            "errors": stats["errors"],
            "violations": violations,
            "worker_summary": summary,
        }
        print(json.dumps(result), flush=True)
        return 1 if proc.returncode != 0 or violations or summary is None else 0


if __name__ == "__main__":
    raise SystemExit(main())
