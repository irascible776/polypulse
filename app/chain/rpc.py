"""
Resilient asynchronous Polygon RPC client with adaptive block chunking,
automatic endpoint failover, rate-limit backoff, and latency instrumentation.
"""

import asyncio
import logging
import random
import time
from decimal import Decimal
from typing import Any

import httpx

logger = logging.getLogger("polygon_rpc")

DEFAULT_ENDPOINTS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]

CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_SCALE = Decimal("1000000")


class PolygonRpcClient:
    def __init__(
        self,
        endpoints: list[str] | None = None,
        timeout: float = 15.0,
        max_retries: int = 5,
    ) -> None:
        self.endpoints = endpoints or list(DEFAULT_ENDPOINTS)
        self.timeout = timeout
        self.max_retries = max_retries
        self.latencies: list[float] = []
        self._current_endpoint_idx = 0

    @property
    def current_endpoint(self) -> str:
        return self.endpoints[self._current_endpoint_idx]

    def _rotate_endpoint(self) -> str:
        self._current_endpoint_idx = (self._current_endpoint_idx + 1) % len(self.endpoints)
        return self.current_endpoint

    async def call(self, method: str, params: list[Any]) -> Any:
        """Execute a single JSON-RPC call with retry, backoff, and endpoint rotation."""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        }
        headers = {
            "User-Agent": "PolyPulse/1.0",
            "Content-Type": "application/json",
        }

        last_error = None
        for attempt in range(1, self.max_retries + 1):
            endpoint = self.current_endpoint
            t0 = time.perf_counter()
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    resp = await client.post(endpoint, json=payload, headers=headers)

                dur = time.perf_counter() - t0
                self.latencies.append(dur)

                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", "1.0"))
                    backoff = retry_after + random.uniform(0.1, 0.5)
                    logger.warning(
                        "HTTP 429 rate limit on %s. Sleeping %.2fs...",
                        endpoint,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                    self._rotate_endpoint()
                    continue

                if resp.status_code >= 400:
                    logger.warning(
                        "HTTP %d from %s. Rotating endpoint.",
                        resp.status_code,
                        endpoint,
                    )
                    self._rotate_endpoint()
                    continue

                data = resp.json()
                if "error" in data:
                    err_msg = data["error"].get("message", str(data["error"]))
                    # Check for rate limit or block range cap
                    if "rate" in err_msg.lower() or "limit" in err_msg.lower():
                        backoff = 0.5 * (2 ** (attempt - 1)) + random.uniform(0.1, 0.4)
                        logger.warning(
                            "RPC error from %s: %s. Backoff %.2fs",
                            endpoint,
                            err_msg,
                            backoff,
                        )
                        await asyncio.sleep(backoff)
                        self._rotate_endpoint()
                        last_error = RuntimeError(err_msg)
                        continue
                    raise RuntimeError(f"RPC error: {err_msg}")

                return data.get("result")

            except (httpx.RequestError, asyncio.TimeoutError) as exc:
                dur = time.perf_counter() - t0
                logger.warning(
                    "Network error calling %s: %s (after %.2fs)",
                    endpoint,
                    exc,
                    dur,
                )
                self._rotate_endpoint()
                last_error = exc
                await asyncio.sleep(0.5 * (2 ** (attempt - 1)))

        raise RuntimeError(f"Exceeded max retries calling {method}: {last_error}")

    async def get_latest_block_number(self) -> int:
        res = await self.call("eth_blockNumber", [])
        return int(res, 16)

    async def get_code(self, address: str) -> str:
        res = await self.call("eth_getCode", [address.lower(), "latest"])
        return str(res)

    async def get_balance_of(
        self, contract: str, account: str, token_id: str, block: int | None = None
    ) -> Decimal:
        """Call ERC-1155 balanceOf(address,uint256) on contract."""
        selector = "00fdd58e"
        addr_padded = account[2:].lower().rjust(64, "0")
        id_padded = hex(int(token_id))[2:].rjust(64, "0")
        calldata = "0x" + selector + addr_padded + id_padded

        block_tag = hex(block) if block is not None else "latest"
        res = await self.call("eth_call", [{"to": contract.lower(), "data": calldata}, block_tag])
        if res is None:
            raise RuntimeError("balanceOf returned no result")
        hex_val = str(res)
        raw_int = int(hex_val, 16)
        return Decimal(raw_int) / CTF_SCALE

    async def get_balances_of_batch(
        self, contract: str, account: str, token_ids: list[str], block: int
    ) -> list[Decimal]:
        """Call ERC-1155 balanceOfBatch for one account at a fixed block."""
        if not token_ids or len(token_ids) > 100:
            raise ValueError("balanceOfBatch requires 1 to 100 token IDs")
        word = lambda n: f"{n:064x}"  # noqa: E731
        n = len(token_ids)
        account_word = account[2:].lower().rjust(64, "0")
        calldata = (
            "0x4e1273f4"
            + word(64)
            + word(96 + 32 * n)
            + word(n)
            + account_word * n
            + word(n)
            + "".join(word(int(token)) for token in token_ids)
        )
        result = await self.call(
            "eth_call", [{"to": contract.lower(), "data": calldata}, hex(block)]
        )
        if not isinstance(result, str) or not result.startswith("0x"):
            raise ValueError("balanceOfBatch returned no data")
        raw = bytes.fromhex(result[2:])
        if len(raw) < 64:
            raise ValueError("balanceOfBatch returned truncated data")
        offset = int.from_bytes(raw[:32], "big")
        count = int.from_bytes(raw[offset : offset + 32], "big")
        if count != n or len(raw) < offset + 32 * (n + 1):
            raise ValueError("balanceOfBatch returned an invalid result count")
        return [
            Decimal(int.from_bytes(raw[offset + 32 * (i + 1) : offset + 32 * (i + 2)], "big"))
            / CTF_SCALE
            for i in range(n)
        ]

    async def get_logs_adaptive(
        self,
        from_block: int,
        to_block: int,
        address: str | None = None,
        topics: list[Any] | None = None,
        max_chunk: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Fetch logs across [from_block, to_block] with adaptive chunking and splitting
        on size limits.
        """
        if from_block > to_block:
            return []

        # If range exceeds max_chunk, break into sub-ranges
        if to_block - from_block + 1 > max_chunk:
            all_logs = []
            curr = from_block
            while curr <= to_block:
                chunk_to = min(curr + max_chunk - 1, to_block)
                logs = await self.get_logs_adaptive(
                    from_block=curr,
                    to_block=chunk_to,
                    address=address,
                    topics=topics,
                    max_chunk=max_chunk,
                )
                all_logs.extend(logs)
                curr = chunk_to + 1
            return all_logs

        req_params: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
        }
        if address:
            req_params["address"] = address.lower()
        if topics:
            req_params["topics"] = topics

        try:
            result = await self.call("eth_getLogs", [req_params])
            return result or []
        except RuntimeError as err:
            err_str = str(err).lower()
            # If error suggests range is too wide or response too large, divide and conquer
            if ("range" in err_str or "size" in err_str or "limit" in err_str) and (
                from_block < to_block
            ):
                mid = (from_block + to_block) // 2
                logger.info(
                    "Splitting block range [%d, %d] -> [%d, %d] and [%d, %d]",
                    from_block,
                    to_block,
                    from_block,
                    mid,
                    mid + 1,
                    to_block,
                )
                left = await self.get_logs_adaptive(
                    from_block, mid, address, topics, max_chunk=max_chunk
                )
                right = await self.get_logs_adaptive(
                    mid + 1, to_block, address, topics, max_chunk=max_chunk
                )
                return left + right
            raise

    def get_latency_stats(self) -> dict[str, float]:
        if not self.latencies:
            return {"min": 0.0, "avg": 0.0, "max": 0.0, "count": 0}
        return {
            "min": min(self.latencies),
            "avg": sum(self.latencies) / len(self.latencies),
            "max": max(self.latencies),
            "count": float(len(self.latencies)),
        }
