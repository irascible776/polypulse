"""Bounded, disposable CPU workers with no database publication capability."""

import asyncio
import multiprocessing
import os
import time
from uuid import uuid4

from app.accounting.engine import compute_condition_events


def _compute(attempt, wallet, condition, events, positions, now):
    started = time.monotonic_ns()
    effects = compute_condition_events(wallet, condition, events, positions, now)
    return attempt, wallet, condition, effects, os.getpid(), started, time.monotonic_ns()


class ConditionExecutor:
    """Share at most `workers` submitted jobs across all wallets in this worker.

    A timeout or cancelled owner invalidates the entire pool generation. Running
    children are terminated before capacity is reused. Other owners of that pool
    fail their transactions too; a subsequent call starts fresh processes.
    """

    def __init__(self, workers):
        if workers < 1:
            raise ValueError("Condition executor requires at least one worker")
        self.workers = workers
        self._slots = asyncio.Semaphore(workers)
        self._pool = None
        self._pending = set()
        self._reset_lock = asyncio.Lock()
        self._closed = False
        self._cleanup_task = None
        self._cleanup_pool = None

    async def _get_pool(self):
        if self._cleanup_task is not None:
            await asyncio.shield(self._cleanup_task)
        async with self._reset_lock:
            if self._closed:
                raise RuntimeError("Condition executor is closed")
            if self._pool is None:
                self._pool = multiprocessing.get_context("spawn").Pool(self.workers)
            return self._pool

    async def _discard(self, pool):
        # Retain cleanup independently of the cancelling request. New owners and
        # close() must await it before creating another generation or exiting.
        if self._cleanup_pool is not pool:
            self._cleanup_pool = pool
            self._cleanup_task = asyncio.create_task(self._retire(pool))
        await asyncio.shield(self._cleanup_task)

    async def _retire(self, pool):
        async with self._reset_lock:
            if self._pool is not pool:
                return
            self._pool = None
            # No await between invalidating generation and failing its waiters.
            for future in list(self._pending):
                if not future.done():
                    future.set_exception(RuntimeError("Condition worker generation aborted"))
            await asyncio.to_thread(pool.terminate)
            await asyncio.to_thread(pool.join)

    async def close(self):
        self._closed = True
        if self._pool is not None:
            await self._discard(self._pool)
        elif self._cleanup_task is not None:
            await asyncio.shield(self._cleanup_task)

    async def compute(self, wallet, work, now, timeout):
        attempt = uuid4().hex
        pool = await self._get_pool()
        loop = asyncio.get_running_loop()
        results = [None] * len(work)
        next_index = 0

        async def consume():
            nonlocal next_index
            while next_index < len(work):
                index = next_index
                next_index += 1
                condition, events, positions = work[index]
                async with self._slots:
                    if pool is not self._pool:
                        raise RuntimeError("Condition worker generation changed")
                    future = loop.create_future()
                    self._pending.add(future)

                    def complete(value, target=future):
                        if not target.done():
                            target.set_result(value)

                    def fail(error, target=future):
                        if not target.done():
                            target.set_exception(error)

                    try:
                        pool.apply_async(
                            _compute,
                            (attempt, wallet, condition, events, positions, now),
                            callback=lambda value, done=complete: loop.call_soon_threadsafe(
                                done, value
                            ),
                            error_callback=lambda error, done=fail: loop.call_soon_threadsafe(
                                done, error
                            ),
                        )
                        result = await future
                    finally:
                        self._pending.discard(future)
                    if result[:3] != (attempt, wallet, condition):
                        raise RuntimeError(
                            "Condition result belongs to another computation attempt"
                        )
                    results[index] = result

        tasks = [asyncio.create_task(consume()) for _ in range(min(self.workers, len(work)))]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._discard(pool)
            raise
        return results
