import argparse
import asyncio
import logging
import time

from app.ingestion.data_api import DataApiSource
from app.ingestion.poller import LivePoller
from app.storage.database import async_session_factory

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("live_measurement")

WALLETS = [
    ("AppleTiiime", "0x951634357346bb291df117f9ac410100feea2104"),
    ("JnStPrdctns", "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"),
]


async def run_measurement(duration_seconds: float, poll_interval: float, run_label: str):
    logger.info("=== STARTING LIVE MEASUREMENT: %s ===", run_label)
    logger.info(
        "Duration: %.1fs (%.1f mins), Base Poll Interval: %.1fs",
        duration_seconds,
        duration_seconds / 60,
        poll_interval,
    )

    source = DataApiSource()

    # Create pollers for each wallet
    pollers = []
    for label, wallet in WALLETS:
        p = LivePoller(
            wallet=wallet,
            source=source,
            session_factory=async_session_factory,
            base_poll_interval=poll_interval,
            min_poll_interval=min(2.0, poll_interval),
            max_poll_interval=30.0,
            overlap_seconds=60,
            reconcile_interval_seconds=min(720.0, duration_seconds * 0.8),
        )
        pollers.append((label, p))

    # Also track /v2/status age_seconds throughout the run
    status_ages = []

    async def poll_status_loop(stop_event: asyncio.Event):
        while not stop_event.is_set():
            try:
                st = await source.fetch_status()
                if st and "data" in st:
                    age = float(st["data"].get("age_seconds", 0))
                    status_ages.append((time.time(), age))
            except Exception as e:
                logger.warning("Status fetch failed: %s", e)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=15.0)
            except asyncio.TimeoutError:
                pass

    stop_event = asyncio.Event()
    status_task = asyncio.create_task(poll_status_loop(stop_event))

    # Run poller tasks concurrently
    poller_tasks = [asyncio.create_task(p.run()) for _, p in pollers]

    t_start = time.time()
    logger.info("Live pollers running. Monitoring for %.1f seconds...", duration_seconds)

    # Monitor loop
    try:
        while time.time() - t_start < duration_seconds:
            elapsed = time.time() - t_start
            await asyncio.sleep(min(30.0, duration_seconds - elapsed))
            logger.info(
                "Measurement progress: %.1fs / %.1fs elapsed",
                time.time() - t_start,
                duration_seconds,
            )
    finally:
        logger.info("Stopping measurement run...")
        stop_event.set()
        for _, p in pollers:
            p.stop()
        await status_task
        for t in poller_tasks:
            t.cancel()
        await asyncio.gather(*poller_tasks, return_exceptions=True)

    actual_duration = time.time() - t_start
    actual_minutes = actual_duration / 60.0

    print("\n=======================================================")
    print(f"RESULTS FOR: {run_label}")
    print(f"Configured Poll Interval: {poll_interval}s")
    print(f"Actual Duration: {actual_duration:.2f}s ({actual_minutes:.2f} minutes) [MEASURED]")
    print("=======================================================\n")

    # API Freshness stats
    if status_ages:
        ages = [a for _, a in status_ages]
        avg_age = sum(ages) / len(ages)
        min_age = min(ages)
        max_age = max(ages)
        print("Polymarket API /v2/status Freshness:")
        print(f"  Samples recorded: {len(status_ages)}")
        print(
            f"  age_seconds (min/avg/max): "
            f"{min_age:.1f}s / {avg_age:.1f}s / {max_age:.1f}s [MEASURED]"
        )
    else:
        print("No /v2/status samples recorded.")

    # Per wallet stats
    for label, p in pollers:
        m = p.metrics
        polls_per_min = p.poll_count / actual_minutes if actual_minutes > 0 else 0
        print(f"\nWallet: {label} ({p.wallet})")
        print(f"  Total Polls: {p.poll_count} [MEASURED]")
        print(f"  Polls Per Minute: {polls_per_min:.2f} [MEASURED]")
        print(f"  New Events Detected: {m.total_new_events} [MEASURED]")
        print(f"  Late Events (is_late=True): {m.total_late_events} [MEASURED]")
        print(f"  Reconcile Sweeps Run: {m.reconcile_sweeps_count} [MEASURED]")
        print(f"  Reconcile Discovered New Events: {m.reconcile_new_events_count} [MEASURED]")
        if m.lags_seconds:
            print(f"  Detection Lag p50: {m.lag_p50:.3f}s [MEASURED]")
            print(f"  Detection Lag p95: {m.lag_p95:.3f}s [MEASURED]")
            print(f"  Detection Lag p99: {m.lag_p99:.3f}s [MEASURED]")
        else:
            print("  Detection Lag: 0 new events occurred during this observation window.")
            print("  Explanation: Neither wallet executed trades during this window.")

    return {
        "label": run_label,
        "interval_s": poll_interval,
        "duration_s": actual_duration,
        "status_ages": status_ages,
        "pollers_metrics": {label: p.metrics for label, p in pollers},
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration", type=float, default=900.0, help="Run duration in seconds (default 900 = 15m)"
    )
    parser.add_argument("--interval", type=float, default=5.0, help="Base poll interval in seconds")
    parser.add_argument("--label", type=str, default="Live Measurement", help="Label for this run")
    args = parser.parse_args()

    asyncio.run(run_measurement(args.duration, args.interval, args.label))


if __name__ == "__main__":
    main()
