from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class PageResult:
    """Represents a single page of events fetched from an event source."""

    events: list[dict[str, Any]]
    next_cursor: str | None
    has_more: bool
    status_code: int = 200
    raw_response: dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionMetrics:
    """Tracks network, retry, and operational metrics for an ingestion source."""

    requests_total: int = 0
    requests_failed: int = 0
    retries_total: int = 0
    rate_limits_429: int = 0
    circuit_breaker_trips: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    def record_request(self, latency_ms: float, success: bool = True) -> None:
        self.requests_total += 1
        if not success:
            self.requests_failed += 1
        self.latencies_ms.append(latency_ms)
        # Keep last 1,000 latencies in memory for percentiles
        if len(self.latencies_ms) > 1000:
            self.latencies_ms.pop(0)

    def record_retry(self) -> None:
        self.retries_total += 1

    def record_429(self) -> None:
        self.rate_limits_429 += 1

    def record_circuit_trip(self) -> None:
        self.circuit_breaker_trips += 1

    @property
    def latency_p50(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        return s[len(s) // 2]

    @property
    def latency_p95(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]

    @property
    def latency_p99(self) -> float:
        if not self.latencies_ms:
            return 0.0
        s = sorted(self.latencies_ms)
        idx = int(len(s) * 0.99)
        return s[min(idx, len(s) - 1)]


class EventSource(ABC):
    """Abstract base class for Polymarket event sources."""

    @property
    @abstractmethod
    def source_tag(self) -> str:
        """Identifier for this source (e.g. 'data_api' or 'fixture_replay')."""
        ...

    @abstractmethod
    async def fetch_page(
        self,
        wallet: str,
        cursor: str | None = None,
        start: int | None = None,
        limit: int = 500,
        sort_direction: str = "ASC",
    ) -> PageResult:
        """Fetch a single page of events for a wallet."""
        ...

    @abstractmethod
    async def fetch_status(self) -> dict[str, Any] | None:
        """Fetch status/freshness metadata if supported."""
        ...
