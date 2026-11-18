"""Bound one projection attempt; callers must roll back when it expires."""

import asyncio
from functools import wraps

from app.config import get_settings


def bounded_projection(method):
    @wraps(method)
    async def run(*args, **kwargs):
        async with asyncio.timeout(get_settings().accounting_attempt_timeout_seconds):
            return await method(*args, **kwargs)

    return run
