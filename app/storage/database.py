import json
import logging
from decimal import Decimal
from typing import Any, AsyncGenerator

import psycopg.types.json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

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

# Async SQLAlchemy engine with psycopg 3 (postgresql+psycopg)
engine = create_async_engine(
    settings.database_url,
    echo=(settings.log_level.upper() == "DEBUG"),
    pool_pre_ping=True,
    pool_size=25,
    max_overflow=50,
    json_serializer=_custom_json_dumps,
)

async_session_factory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

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
