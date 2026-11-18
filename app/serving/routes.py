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
from sqlalchemy import and_, desc, func, or_, select, text, union_all
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.chain.rpc import PolygonRpcClient
from app.config import get_settings
from app.ingestion.status import ingestion_freshness
from app.serving.analytics import (
    get_system_metrics,
    get_wallet_analytics,
    get_wallets_leaderboard,
)
from app.serving.fleet import get_fleet_status
from app.serving.middleware import global_latency_tracker
from app.serving.time_format import utc_timestamp
from app.serving.websocket import ws_manager
from app.storage.database import (
    async_session_factory,
    check_database_connection,
    market_session,
    secondary_session_factory,
    validate_shard_identity,
    wallet_read_session,
)
from app.storage.models import (
    ArchivedMarketPrice,
    ChainEvent,
    IngestionCursor,
    MarketPrice,
    MarketResolution,
    Position,
    PositionLedger,
    SourceEvent,
    TrackedWallet,
    WalletTotal,
)
from app.valuation.engine import ValuationEngine
from app.valuation.pricing import PriceResolver, PriceTier

logger = logging.getLogger(__name__)

router = APIRouter()
valuation_engine = ValuationEngine()
price_resolver = PriceResolver()


async def get_db_session() -> AsyncSession:
    try:
        async with async_session_factory() as session:
            if secondary_session_factory is not None:
                await validate_shard_identity(session, 0)
            yield session
    except (SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="Market database unavailable") from exc


async def get_wallet_db_session(address: str) -> AsyncSession:
    try:
        async with wallet_read_session(address) as session:
            yield session
    except (SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="Wallet database unavailable") from exc


async def wallet_freshness(session: AsyncSession, wallet: str) -> dict[str, Any]:
    cursor = await session.get(IngestionCursor, (wallet.lower(), "data_api"))
    freshness = ingestion_freshness(cursor.last_success_at if cursor else None)
    total = await session.get(WalletTotal, wallet.lower())
    latest_order = await session.scalar(
        select(func.max(SourceEvent.api_order)).where(
            SourceEvent.wallet == wallet.lower(), SourceEvent.superseded_by.is_(None)
        )
    )
    applied_order = total.last_applied_api_order if total else 0
    freshness.update(
        accounting_applied_order=applied_order,
        accounting_revision=total.publication_revision if total else 0,
        accounting_current=not (total and total.recompute_needed)
        and (latest_order is None or latest_order <= applied_order),
    )
    positions = (
        await session.execute(
            select(Position.token_id, Position.condition_id).where(
                Position.wallet == wallet.lower(), Position.shares > 0
            )
        )
    ).all()
    async with market_session(session) as market_db:
        resolutions = (
            (
                await market_db.execute(
                    select(MarketResolution).where(
                        MarketResolution.condition_id.in_({condition for _, condition in positions})
                    )
                )
            )
            .scalars()
            .all()
        )
        states = {record.condition_id: record for record in resolutions}
        open_tokens = {
            token
            for token, condition in positions
            if condition not in states or states[condition].gamma_closed is not True
        }
        closed_tokens = {token for token, _ in positions} - open_tokens
        tick_queries = []
        for model in (MarketPrice, ArchivedMarketPrice):
            valid_tick = or_(
                and_(model.source == "last_trade", model.price.between(0, 1)),
                and_(
                    model.source == "live_quote",
                    model.bid >= 0,
                    model.ask <= 1,
                    model.bid <= model.ask,
                    model.ask - model.bid <= get_settings().max_spread,
                    model.token_id.not_in(closed_tokens),
                ),
            )
            tick_queries.append(
                select(model.token_id, model.observed_at).where(
                    model.token_id.in_({token for token, _ in positions}),
                    model.observed_at <= datetime.now(timezone.utc),
                    valid_tick,
                )
            )
        ticks = union_all(*tick_queries).subquery()
        per_token = (
            select(ticks.c.token_id, func.max(ticks.c.observed_at).label("tick_time"))
            .group_by(ticks.c.token_id)
            .subquery()
        )
        newest, oldest = (
            await market_db.execute(
                select(func.max(per_token.c.tick_time), func.min(per_token.c.tick_time))
            )
        ).one()
    freshness.update(
        prices_as_of=utc_timestamp(newest),
        oldest_tick_age_seconds=max(0.0, (datetime.now(timezone.utc) - oldest).total_seconds())
        if oldest
        else None,
    )
    unchecked = [
        states[condition].checked_at if condition in states else None
        for _, condition in positions
        if condition not in states or states[condition].resolved is not True
    ]
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
        "instance_id": get_settings().resolve_instance_id(),
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


@router.get("/fleet", status_code=status.HTTP_200_OK)
async def fleet_status() -> dict[str, Any]:
    """Return active/stale API & worker instances and live 16-bucket ownership map."""
    try:
        return await get_fleet_status()
    except (SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="Fleet status unavailable") from exc


@router.get("/system/metrics", status_code=status.HTTP_200_OK)
async def system_metrics() -> dict[str, Any]:
    """Return rolling throughput, detection lag, API latency, partition stats, and benchmarks."""
    try:
        return await get_system_metrics()
    except (SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="System metrics unavailable") from exc


@router.get("/wallets", status_code=status.HTTP_200_OK)
async def list_wallets(
    window: str = Query("all", description="Time window: 7d, 30d, or all"),
    min_volume: Decimal = Query(Decimal("0"), ge=Decimal("0")),
) -> dict[str, Any]:
    """Cross-shard tracked wallet leaderboard with honest PnL, exposure, win rate, and reconcile."""
    if window not in {"7d", "30d", "all"}:
        raise HTTPException(status_code=400, detail="window must be one of: 7d, 30d, all")
    try:
        return await get_wallets_leaderboard(
            valuation_engine=valuation_engine, window=window, min_volume=min_volume
        )
    except (SQLAlchemyError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="Wallet leaderboard unavailable") from exc


@router.get("/wallets/{address}/analytics", status_code=status.HTTP_200_OK)
async def wallet_analytics(
    address: str,
    window: str = Query("all", description="Time window: 7d, 30d, or all"),
    session: AsyncSession = Depends(get_wallet_db_session),
) -> dict[str, Any]:
    """Return equity/drawdown curve, entry-price buckets, excluded flows, audit, and reconcile."""
    if window not in {"7d", "30d", "all"}:
        raise HTTPException(status_code=400, detail="window must be one of: 7d, 30d, all")
    return await get_wallet_analytics(session=session, wallet=address, window=window)


@router.get("/wallets/{address}/positions")
async def get_wallet_positions(
    address: str,
    as_of: str | None = Query(
        None, description="ISO timestamp for historical point-in-time valuation"
    ),
    session: AsyncSession = Depends(get_wallet_db_session),
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

    valuation_engine.invalidate(clean_wallet)
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
            or not freshness["accounting_current"]
        ),
        **freshness,
    }


@router.get("/wallets/{address}/pnl")
async def get_wallet_pnl(
    address: str,
    as_of: str | None = Query(
        None, description="ISO timestamp for historical point-in-time valuation"
    ),
    session: AsyncSession = Depends(get_wallet_db_session),
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

    valuation_engine.invalidate(clean_wallet)
    summary = await valuation_engine.value_wallet(session, clean_wallet, as_of=as_of_dt)
    result = summary.to_dict()
    result.pop("positions")
    result.update(await wallet_freshness(session, clean_wallet))
    result["valuation_current"] = (
        result["complete"]
        and not result["ingestion_stale"]
        and not result["resolution_stale"]
        and result["accounting_current"]
    )
    return result


@router.get("/wallets/{address}/trades")
async def get_wallet_trades(
    address: str,
    cursor: int | None = Query(None, description="Pagination cursor (api_order)"),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_wallet_db_session),
) -> dict[str, Any]:
    """
    Returns paginated trade ledger events for a wallet.
    """
    clean_wallet = address.lower()
    stmt = (
        select(PositionLedger, SourceEvent.transaction_hash)
        .outerjoin(
            SourceEvent,
            and_(
                SourceEvent.wallet == PositionLedger.wallet,
                SourceEvent.source_event_id == PositionLedger.source_event_id,
            ),
        )
        .where(PositionLedger.wallet == clean_wallet)
        .order_by(desc(PositionLedger.api_order))
        .limit(limit + 1)
    )
    if cursor is not None:
        stmt = stmt.where(PositionLedger.api_order < cursor)

    entries = (await session.execute(stmt)).all()
    has_more = len(entries) > limit
    results = entries[:limit]
    next_cursor = results[-1][0].api_order if has_more and results else None

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
                "transaction_hash": tx_hash,
                "shares_delta": str(t.shares_delta),
                "shares_after": str(t.shares_after),
                "avg_cost_after": str(t.avg_cost_after),
                "cash_delta": str(t.cash_delta),
                "realized_pnl_delta": str(t.realized_pnl_delta),
                "is_recompute": t.is_recompute,
            }
            for t, tx_hash in results
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
        (
            await session.execute(
                select(ArchivedMarketPrice.token_id).where(
                    ArchivedMarketPrice.condition_id == condition_id
                )
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
    if secondary_session_factory is not None:
        try:
            async with secondary_session_factory() as other:
                await validate_shard_identity(other, 1)
                ids.update(
                    (
                        await other.execute(
                            select(Position.token_id).where(Position.condition_id == condition_id)
                        )
                    ).scalars()
                )
                ids.update(
                    token
                    for token in (
                        await other.execute(
                            select(SourceEvent.token_id).where(
                                SourceEvent.condition_id == condition_id
                            )
                        )
                    ).scalars()
                    if token
                )
        except SQLAlchemyError as exc:
            raise HTTPException(status_code=503, detail="Market coverage incomplete") from exc
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

    # Each API replica listens to the shard's notification channel.
    factories = [async_session_factory]
    if secondary_session_factory is not None:
        factories.append(secondary_session_factory)
    try:
        for factory in factories:
            async with factory() as notification_session:
                async with notification_session.begin():
                    wallets = (
                        (
                            await notification_session.execute(
                                select(TrackedWallet.address).distinct()
                            )
                        )
                        .scalars()
                        .all()
                    )
                    for wallet in wallets:
                        payload = json.dumps(
                            {"type": "price_update", "wallet": wallet, "ref": str(tick.id)}
                        )
                        await notification_session.execute(
                            text("SELECT pg_notify('polypulse_events', :payload)"),
                            {"payload": payload},
                        )
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=503, detail="Price stored; notification incomplete"
        ) from exc

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
    if not get_settings().is_origin_allowed(websocket.headers.get("origin")):
        await websocket.close(code=4403)
        return
    clean_wallet = address.lower()
    if not re.fullmatch(r"0x[0-9a-f]{40}", clean_wallet):
        await websocket.close(code=4403)
        return
    try:
        async with wallet_read_session(clean_wallet) as session:
            tracked = await session.get(TrackedWallet, clean_wallet)
            if tracked is None or tracked.status != "active":
                await websocket.close(code=4403)
                return
    except (SQLAlchemyError, RuntimeError):
        await websocket.close(code=1013)
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
