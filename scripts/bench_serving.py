"""Measure serving endpoints; VALUATION_CACHE_DISABLED=1 forces cold valuation."""

import asyncio
import math
import os
import platform
import statistics
import time
from datetime import datetime, timezone

import httpx

from app.main import app
from app.serving.routes import valuation_engine

WALLETS = {
    "AppleTiiime": "0x951634357346bb291df117f9ac410100feea2104",
    "JnStPrdctns": "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92",
}


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


async def main():
    n = int(os.environ.get("BENCH_REQUESTS", "20"))
    print(
        f"MEASURED date={datetime.now(timezone.utc).isoformat()} "
        f"machine={platform.platform()} requests={n}"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://bench"
    ) as client:
        for mode in ("cold", "warm"):
            os.environ["VALUATION_CACHE_DISABLED"] = "1" if mode == "cold" else "0"
            for concurrency in (1, 20):
                for label, wallet in WALLETS.items():
                    for endpoint in ("positions", "pnl"):
                        path = f"/wallets/{wallet}/{endpoint}"
                        valuation_engine.invalidate(wallet)
                        if mode == "warm":
                            await client.get(path)
                        semaphore = asyncio.Semaphore(concurrency)
                        durations = []
                        errors = 0

                        async def request():
                            nonlocal errors
                            async with semaphore:
                                start = time.perf_counter()
                                try:
                                    response = await client.get(path)
                                    if response.status_code != 200:
                                        errors += 1
                                except Exception:
                                    errors += 1
                                durations.append((time.perf_counter() - start) * 1000)

                        start = time.perf_counter()
                        await asyncio.gather(*(request() for _ in range(n)))
                        elapsed = time.perf_counter() - start
                        print(
                            f"MEASURED {mode} concurrency={concurrency} wallet={label} "
                            f"endpoint={endpoint} p50_ms={statistics.median(durations):.3f} "
                            f"p95_ms={percentile(durations, 0.95):.3f} "
                            f"p99_ms={percentile(durations, 0.99):.3f} "
                            f"throughput_rps={n / elapsed:.2f} errors={errors}",
                            flush=True,
                        )
    os.environ.pop("VALUATION_CACHE_DISABLED", None)


if __name__ == "__main__":
    asyncio.run(main())
