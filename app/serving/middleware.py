"""
app/serving/middleware.py

Latency tracking middleware for FastAPI serving layer.
Tracks p50, p95, and p99 request latencies across endpoints.
"""

import logging
import time
from collections import deque

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


class LatencyTracker:
    def __init__(self, max_samples: int = 2000) -> None:
        self.latencies: deque[float] = deque(maxlen=max_samples)

    def record(self, duration_ms: float) -> None:
        self.latencies.append(duration_ms)

    def get_stats(self) -> dict[str, float]:
        if not self.latencies:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}

        sorted_vals = sorted(self.latencies)
        n = len(sorted_vals)

        def percentile(p: float) -> float:
            idx = int(round(p * (n - 1)))
            return sorted_vals[min(max(idx, 0), n - 1)]

        return {
            "p50": round(percentile(0.50), 3),
            "p95": round(percentile(0.95), 3),
            "p99": round(percentile(0.99), 3),
            "min": round(sorted_vals[0], 3),
            "max": round(sorted_vals[-1], 3),
            "count": n,
        }


global_latency_tracker = LatencyTracker()


class LatencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        t0 = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - t0) * 1000.0

        global_latency_tracker.record(duration_ms)
        response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"
        return response
