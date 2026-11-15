"""Compare Gamma resolution status with CTF payout vectors at a fixed block.

CTF mappings: https://github.com/gnosis/conditional-tokens-contracts/blob/master/contracts/ConditionalTokens.sol
Gamma fields: https://docs.polymarket.com/market-data/market-details
Gamma status enum: https://github.com/Polymarket/py-sdk/blob/main/src/polymarket/models/gamma/market.py
"""

import asyncio
import re
from datetime import datetime, timezone
from decimal import Decimal

import httpx
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.chain.rpc import CTF_CONTRACT, PolygonRpcClient
from app.storage.models import MarketResolution


async def check_resolution(condition_id: str) -> dict:
    """Fetch Gamma and CTF evidence; unknown Gamma status never agrees."""
    condition_id = condition_id.lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", condition_id):
        raise ValueError("condition_id must be 32 bytes")
    async with httpx.AsyncClient(timeout=20) as http:
        for attempt in range(3):
            try:
                response = await http.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"condition_ids": condition_id, "closed": "any"},
                )
                response.raise_for_status()
                markets = response.json()
                break
            except (httpx.NetworkError, httpx.TimeoutException):
                if attempt == 2:
                    raise
                await asyncio.sleep(0.5 * 2**attempt)
    matching = [m for m in markets if m.get("conditionId", "").lower() == condition_id]
    if len(matching) != 1:
        raise ValueError(f"Gamma returned {len(matching)} matching markets")
    gamma = matching[0]
    client = PolygonRpcClient(endpoints=["https://polygon.drpc.org/"], max_retries=3)
    block = await client.get_latest_block_number() - 32

    # Keccak selectors derived using web3_sha3 on Polygon PublicNode.
    selectors = {
        "payoutDenominator(bytes32)": "dd34de67",
        "getOutcomeSlotCount(bytes32)": "d42dc0c2",
        "payoutNumerators(bytes32,uint256)": "0504c814",
    }

    async def call(signature: str, extra: str = "") -> int:
        data = "0x" + selectors[signature] + condition_id[2:] + extra
        raw = await client.call("eth_call", [{"to": CTF_CONTRACT, "data": data}, hex(block)])
        if not isinstance(raw, str) or not raw.startswith("0x") or len(raw) != 66:
            raise ValueError(f"invalid CTF response for {signature}")
        return int(raw, 16)

    denominator = await call("payoutDenominator(bytes32)")
    slots = await call("getOutcomeSlotCount(bytes32)")
    if slots > 256:
        raise ValueError("invalid CTF outcome slot count")
    numerators = [
        await call("payoutNumerators(bytes32,uint256)", f"{i:064x}") for i in range(slots)
    ]
    status = gamma.get("umaResolutionStatus") or None
    closed = gamma.get("closed")
    # The official SDK enumerates UMA resolution lifecycle states, including resolved.
    gamma_resolved = True if closed is True and status == "resolved" else None
    if status in ("requested", "proposed", "disputed") or (closed is False and status is None):
        gamma_resolved = False
    resolved = denominator > 0
    return {
        "condition_id": condition_id,
        "resolved": resolved,
        "payout_numerators": numerators,
        "payout_denominator": Decimal(denominator),
        "gamma_closed": closed,
        "gamma_uma_status": status,
        "gamma_raw": gamma,
        "chain_block": block,
        "checked_at": datetime.now(timezone.utc),
        "agreement": gamma_resolved is not None and gamma_resolved == resolved,
    }


async def save_resolution(session: AsyncSession, record: dict) -> None:
    """Upsert an observed resolution within the caller's transaction."""
    stmt = insert(MarketResolution).values(**record)
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=[MarketResolution.condition_id],
            set_={key: value for key, value in record.items() if key != "condition_id"},
        )
    )
