"""
app/serving/routes.py

FastAPI router implementing all REST and WebSocket endpoints for Phase 5 serving layer.
"""

import hmac
import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.chain.rpc import PolygonRpcClient
from app.config import get_settings
from app.ingestion.status import ingestion_freshness
from app.serving.middleware import global_latency_tracker
from app.serving.time_format import utc_timestamp
from app.serving.websocket import ws_manager
from app.storage.database import async_session_factory, check_database_connection
from app.storage.models import (
    ChainEvent,
    IngestionCursor,
    MarketPrice,
    MarketResolution,
    Position,
    PositionLedger,
    SourceEvent,
    TrackedWallet,
)
from app.valuation.engine import ValuationEngine
from app.valuation.pricing import PriceResolver, PriceTier

logger = logging.getLogger(__name__)

router = APIRouter()
valuation_engine = ValuationEngine()
price_resolver = PriceResolver()


async def get_db_session() -> AsyncSession:
    async with async_session_factory() as session:
        yield session


async def wallet_freshness(session: AsyncSession, wallet: str) -> dict[str, Any]:
    cursor = await session.get(IngestionCursor, (wallet.lower(), "data_api"))
    freshness = ingestion_freshness(cursor.last_success_at if cursor else None)
    valid_tick = or_(
        and_(MarketPrice.source == "last_trade", MarketPrice.price.between(0, 1)),
        and_(
            MarketPrice.source == "live_quote",
            MarketPrice.bid >= 0,
            MarketPrice.ask <= 1,
            MarketPrice.bid <= MarketPrice.ask,
            MarketPrice.ask - MarketPrice.bid <= get_settings().max_spread,
            or_(
                MarketResolution.condition_id.is_(None), MarketResolution.gamma_closed.is_not(True)
            ),
        ),
    )
    per_token = (
        select(MarketPrice.token_id, func.max(MarketPrice.observed_at).label("tick_time"))
        .join(Position, Position.token_id == MarketPrice.token_id)
        .outerjoin(MarketResolution, MarketResolution.condition_id == Position.condition_id)
        .where(
            Position.wallet == wallet.lower(),
            Position.shares > 0,
            MarketPrice.observed_at <= datetime.now(timezone.utc),
            valid_tick,
        )
        .group_by(MarketPrice.token_id)
        .subquery()
    )
    newest, oldest = (
        await session.execute(
            select(func.max(per_token.c.tick_time), func.min(per_token.c.tick_time))
        )
    ).one()
    freshness.update(
        prices_as_of=utc_timestamp(newest),
        oldest_tick_age_seconds=max(0.0, (datetime.now(timezone.utc) - oldest).total_seconds())
        if oldest
        else None,
    )
    condition_rows = (
        await session.execute(
            select(MarketResolution.checked_at, MarketResolution.resolved)
            .select_from(Position)
            .outerjoin(MarketResolution, MarketResolution.condition_id == Position.condition_id)
            .where(Position.wallet == wallet.lower(), Position.shares > 0)
            .distinct()
        )
    ).all()
    unchecked = [checked for checked, resolved in condition_rows if resolved is not True]
    oldest_check = min((checked for checked in unchecked if checked is not None), default=None)
    missing_resolution = any(checked is None for checked in unchecked)
    resolution_stale = missing_resolution or (
        oldest_check is not None
        and (datetime.now(timezone.utc) - oldest_check).total_seconds()
        > get_settings().resolution_stale_seconds
    )
    freshness.update(
        resolutions_as_of=utc_timestamp(oldest_check),
        resolution_stale=resolution_stale,
    )
    return freshness


@router.get("/serving/health", status_code=status.HTTP_200_OK)
@router.get("/system/health", status_code=status.HTTP_200_OK)
async def serving_health(session: AsyncSession = Depends(get_db_session)) -> dict[str, Any]:
    """
    Health and status probe:
    Verifies DB connectivity, reports RPC provider latency,
    latest block height, and serving latency stats (p50/p95/p99).
    """
    db_connected = await check_database_connection()

    # Query latest chain_event block
    stmt = (
        select(ChainEvent.block_number, ChainEvent.created_at)
        .order_by(desc(ChainEvent.block_number))
        .limit(1)
    )
    latest_event = (await session.execute(stmt)).first()

    rpc_client = PolygonRpcClient()
    rpc_stats = rpc_client.get_latency_stats()
    serving_stats = global_latency_tracker.get_stats()
    ws_stats = ws_manager.get_stats()

    return {
        "status": "healthy" if db_connected else "unhealthy",
        "database_connected": db_connected,
        "last_chain_pull": {
            "block_number": latest_event[0] if latest_event else None,
            "pulled_at": utc_timestamp(latest_event[1]) if latest_event else None,
        },
        "rpc_provider": {
            "primary": rpc_client.current_endpoint,
            "latency_stats": rpc_stats,
        },
        "serving_latency_ms": serving_stats,
        "websocket_stats": ws_stats,
    }


@router.get("/wallets/{address}/positions")
async def get_wallet_positions(
    address: str,
    as_of: str | None = Query(
        None, description="ISO timestamp for historical point-in-time valuation"
    ),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Returns open positions with shares, cost basis, current mark price,
    unrealized PnL, price source tier, and staleness badge.
    """
    clean_wallet = address.lower()
    as_of_dt = None
    if as_of:
        try:
            as_of_dt = datetime.fromisoformat(as_of.replace(" ", "+"))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid ISO timestamp format for as_of parameter",
            )

    summary = await valuation_engine.value_wallet(session, clean_wallet, as_of=as_of_dt)
    freshness = await wallet_freshness(session, clean_wallet)
    return {
        "wallet": clean_wallet,
        "as_of": utc_timestamp(as_of_dt),
        "open_positions_count": summary.open_positions_count,
        "unpriced_positions_count": summary.unpriced_positions_count,
        "stale_positions_count": summary.stale_positions_count,
        "positions": [p.to_dict() for p in summary.positions],
        "valuation_current": not (
            summary.unpriced_positions_count
            or freshness["ingestion_stale"]
            or freshness["resolution_stale"]
        ),
        **freshness,
    }


@router.get("/wallets/{address}/pnl")
async def get_wallet_pnl(
    address: str,
    as_of: str | None = Query(
        None, description="ISO timestamp for historical point-in-time valuation"
    ),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Returns realized + unrealized mark-to-market PnL summary.
    Supports point-in-time historical queries (?as_of=).
    """
    clean_wallet = address.lower()
    as_of_dt = None
    if as_of:
        try:
            as_of_dt = datetime.fromisoformat(as_of.replace(" ", "+"))
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="Invalid ISO timestamp format for as_of parameter",
            )

    summary = await valuation_engine.value_wallet(session, clean_wallet, as_of=as_of_dt)
    result = summary.to_dict()
    result.pop("positions")
    result.update(await wallet_freshness(session, clean_wallet))
    result["valuation_current"] = (
        result["complete"] and not result["ingestion_stale"] and not result["resolution_stale"]
    )
    return result


@router.get("/wallets/{address}/trades")
async def get_wallet_trades(
    address: str,
    cursor: int | None = Query(None, description="Pagination cursor (api_order)"),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Returns paginated trade ledger events for a wallet.
    """
    clean_wallet = address.lower()
    stmt = (
        select(PositionLedger)
        .where(PositionLedger.wallet == clean_wallet)
        .order_by(desc(PositionLedger.api_order))
        .limit(limit + 1)
    )
    if cursor is not None:
        stmt = stmt.where(PositionLedger.api_order < cursor)

    entries = (await session.execute(stmt)).scalars().all()
    has_more = len(entries) > limit
    results = entries[:limit]
    next_cursor = results[-1].api_order if has_more and results else None

    return {
        "wallet": clean_wallet,
        "count": len(results),
        "next_cursor": next_cursor,
        "trades": [
            {
                "id": t.id,
                "event_type": t.event_type,
                "event_time": utc_timestamp(t.event_time),
                "condition_id": t.condition_id,
                "token_id": t.token_id,
                "shares_delta": str(t.shares_delta),
                "shares_after": str(t.shares_after),
                "avg_cost_after": str(t.avg_cost_after),
                "cash_delta": str(t.cash_delta),
                "realized_pnl_delta": str(t.realized_pnl_delta),
                "is_recompute": t.is_recompute,
            }
            for t in results
        ],
    }


@router.get("/markets/{condition_id}/price")
async def get_market_price(
    condition_id: str,
    token_id: str | None = Query(None),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Returns latest known mark price, source, and staleness for a condition or token.
    """
    condition_id = condition_id.lower()
    ids = set(
        (
            await session.execute(
                select(Position.token_id).where(Position.condition_id == condition_id)
            )
        )
        .scalars()
        .all()
    )
    ids.update(
        (
            await session.execute(
                select(MarketPrice.token_id).where(MarketPrice.condition_id == condition_id)
            )
        )
        .scalars()
        .all()
    )
    ids.update(
        token
        for token in (
            await session.execute(
                select(SourceEvent.token_id).where(SourceEvent.condition_id == condition_id)
            )
        )
        .scalars()
        .all()
        if token
    )
    state = await session.get(MarketResolution, condition_id)
    context = {}
    if state:
        raw_ids = state.gamma_raw.get("clobTokenIds") or "[]"
        try:
            outcome_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
            for index, item in enumerate(outcome_ids):
                ids.add(item)
                context[item] = (condition_id, index)
        except (TypeError, ValueError):
            pass
    if token_id and token_id not in ids:
        raise HTTPException(status_code=404, detail="Token not found for specified market")
    if not ids:
        raise HTTPException(status_code=404, detail="No price found for specified market")
    selected = [token_id] if token_id else sorted(ids)
    quotes = await price_resolver.get_mark_prices_batch(session, selected, token_context=context)
    entries = [
        {"outcome": quotes[t].outcome, "token_id": t, "mark": quotes[t].to_dict()} for t in selected
    ]
    return entries[0] if token_id else {"condition_id": condition_id, "prices": entries}


@router.post("/markets/price")
async def post_market_price(
    token_id: str,
    price: Decimal,
    condition_id: str | None = None,
    source: str = PriceTier.LIVE_QUOTE.value,
    bid: Decimal | None = None,
    ask: Decimal | None = None,
    x_api_key: str | None = Header(None, alias="X-API-Key"),
    session: AsyncSession = Depends(get_db_session),
) -> dict[str, Any]:
    """
    Records an append-only price tick and broadcasts price_update to WS subscribers.
    Protected by X-API-Key authentication to ensure market data integrity.
    """
    api_key = os.environ.get("POLYPULSE_API_KEY")
    if not api_key or not x_api_key or not hmac.compare_digest(x_api_key, api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: Valid X-API-Key required for market price injection",
        )

    tick = await price_resolver.record_price(
        session=session,
        token_id=token_id,
        price=price,
        source=source,
        condition_id=condition_id,
        bid=bid,
        ask=ask,
    )

    # Invalidate live valuation micro-cache on price tick injection
    valuation_engine.invalidate()

    # Broadcast price update across affected wallets
    # Query wallets holding this token
    stmt = select(TrackedWallet.address).distinct()
    wallets = (await session.execute(stmt)).scalars().all()
    for w in wallets:
        await ws_manager.broadcast_to_wallet(
            wallet=w,
            event_type="price_update",
            payload={
                "token_id": token_id,
                "price": str(price),
                "source": source,
                "observed_at": utc_timestamp(tick.observed_at),
            },
        )

    return {
        "success": True,
        "id": tick.id,
        "token_id": tick.token_id,
        "price": str(tick.price),
        "source": tick.source,
    }


@router.websocket("/ws/wallets/{address}")
async def websocket_wallet_endpoint(websocket: WebSocket, address: str) -> None:
    """
    WebSocket endpoint streaming real-time PnL updates and new trades for a wallet.
    """
    clean_wallet = address.lower()
    if not re.fullmatch(r"0x[0-9a-f]{40}", clean_wallet):
        await websocket.close(code=4403)
        return
    async with async_session_factory() as session:
        tracked = await session.get(TrackedWallet, clean_wallet)
        if tracked is None or tracked.status != "active":
            await websocket.close(code=4403)
            return
    await ws_manager.connect(clean_wallet, websocket)

    try:
        # Send initial state snapshot upon connection
        await ws_manager.snapshot(clean_wallet, websocket)

        while True:
            # Client heartbeat / command listener
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        await ws_manager.disconnect(clean_wallet, websocket)
    except Exception as e:
        logger.warning("WS connection error for %s: %s", clean_wallet, e)
        await ws_manager.disconnect(clean_wallet, websocket)
