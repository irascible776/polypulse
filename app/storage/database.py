import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from decimal import Decimal
from typing import Any, AsyncGenerator

import psycopg.types.json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings
from app.storage.sharding import wallet_bucket

logger = logging.getLogger(__name__)

settings = get_settings()


def _decimal_default_serializer(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _custom_json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=_decimal_default_serializer)


# Register serializer with psycopg 3 to support Decimal objects inside JSONB columns
psycopg.types.json.set_json_dumps(_custom_json_dumps)


def _build_engine(raw_url: str):
    sa_url, connect_args = settings.sqlalchemy_url_and_args(raw_url)
    connect_args["prepare_threshold"] = None
    return create_async_engine(
        sa_url,
        echo=(settings.log_level.upper() == "DEBUG"),
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        connect_args=connect_args,
        json_serializer=_custom_json_dumps,
    )


# Async SQLAlchemy engine with psycopg 3 (postgresql+psycopg)
engine = _build_engine(settings.database_url)

async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

secondary_engine = (
    _build_engine(settings.secondary_database_url) if settings.secondary_database_url else None
)
secondary_session_factory = (
    async_sessionmaker(bind=secondary_engine, class_=AsyncSession, expire_on_commit=False)
    if secondary_engine is not None
    else None
)


def wallet_session_factory(wallet: str) -> async_sessionmaker[AsyncSession]:
    if secondary_session_factory is not None and wallet_bucket(wallet) >= 8:
        return secondary_session_factory
    return async_session_factory


def wallet_engine(wallet: str):
    if secondary_engine is not None and wallet_bucket(wallet) >= 8:
        return secondary_engine
    return engine


async def validate_wallet_shard(session: AsyncSession, wallet: str) -> None:
    if secondary_engine is None:
        return
    expected = 0 if wallet_bucket(wallet) < 8 else 1
    await validate_shard_identity(session, expected)


async def validate_shard_identity(session: AsyncSession, expected: int) -> None:
    actual, state = (
        await session.execute(
            text("SELECT shard_id, migration_state FROM wallet_shard_settings WHERE id = 1")
        )
    ).one()
    if actual != expected or state != "active":
        raise RuntimeError("Wallet database shard is not configured for this wallet")


@asynccontextmanager
async def market_session(session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    shared = session.info.get("market_session")
    if shared is not None:
        yield shared
        return
    bound = session.bind
    if isinstance(bound, AsyncConnection):
        bound = bound.engine
    if secondary_engine is not None and bound is secondary_engine:
        stack = session.info.get("market_stack")
        if stack is not None:
            shared_session = await stack.enter_async_context(async_session_factory())
            await shared_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            session.info["market_session"] = shared_session
            yield shared_session
        else:
            async with async_session_factory() as shared_session:
                await shared_session.execute(
                    text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                )
                yield shared_session
    else:
        yield session


@asynccontextmanager
async def wallet_read_session(wallet: str) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncExitStack() as stack:
        session = await stack.enter_async_context(wallet_session_factory(wallet)())
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await validate_wallet_shard(session, wallet)
        session.info["market_stack"] = stack
        session.info["accounting_snapshot"] = True
        yield session


# Convenience alias
async_session = async_session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide an asynchronous database session."""
    async with async_session_factory() as session:
        yield session


async def check_database_connection() -> bool:
    """Check if the database is reachable by executing a simple SELECT 1 query."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            return True
    except Exception as exc:
        logger.warning(f"Database readiness check failed: {exc}")
        return False


async def check_shard_ready(db_engine, expected: int) -> bool:
    try:
        async with db_engine.connect() as connection:
            row = (
                await connection.execute(
                    text("SELECT shard_id, migration_state FROM wallet_shard_settings WHERE id = 1")
                )
            ).one_or_none()
            return row == (expected, "active")
    except Exception as exc:
        logger.warning("Shard %s readiness check failed: %s", expected, exc)
        return False
