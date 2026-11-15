"""
On-chain event log decoder for Gnosis Conditional Tokens Framework (CTF) and ERC-1155 on Polygon.
Pure functions, zero external web3 dependencies, strict Decimal scaling.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

TRANSFER_SINGLE_TOPIC = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
TRANSFER_BATCH_TOPIC = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
POSITION_SPLIT_TOPIC = "0x2e6bb91f8cbcda0c93623c54d0403a43514fabc40084ec96b6d5379a74786298"
POSITIONS_MERGE_TOPIC = "0x09da510257321e03a985cc7ec0d53c6e94a86ce880ee111718335b2e535a0928"
PAYOUT_REDEMPTION_TOPIC = "0x2e6840742f56b6c2c8f8b89e3a35a60e0a5c48b111666ff0597143922da50937"

CTF_SCALE = Decimal("1000000")  # 6 decimal places on Polygon


@dataclass
class DecodedChainEvent:
    event_name: str
    contract_address: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_time: datetime
    wallet: str
    condition_id: str | None
    token_id: str | None
    from_address: str | None
    to_address: str | None
    amount: Decimal
    raw_log: dict[str, Any]
    batch_index: int = 0


def topic_to_address(topic_hex: str) -> str:
    """Extract a 20-byte checksummed/normalized 0x hex address from a 32-byte log topic."""
    clean = topic_hex[2:] if topic_hex.startswith("0x") else topic_hex
    return "0x" + clean[-40:].lower()


def decode_log(log: dict[str, Any], target_wallet: str) -> list[DecodedChainEvent]:
    """
    Decode a single raw Ethereum JSON-RPC log dictionary into one or more DecodedChainEvent objects.
    Returns an empty list if the log does not match supported CTF or ERC-1155 topics.
    """
    topics: list[str] = log.get("topics", [])
    if not topics:
        return []

    topic0 = topics[0].lower()
    contract_address = str(log.get("address", "")).lower()
    tx_hash = str(log.get("transactionHash", "")).lower()
    log_idx = int(str(log.get("logIndex", "0x0")), 16)
    block_num = int(str(log.get("blockNumber", "0x0")), 16)

    # A real block timestamp must be supplied by the caller.
    ts_raw = log.get("blockTimestamp")
    if ts_raw is None:
        raise ValueError("blockTimestamp is required")
    ts_val = int(ts_raw, 16) if isinstance(ts_raw, str) and ts_raw.startswith("0x") else int(ts_raw)
    block_time = datetime.fromtimestamp(ts_val, tz=timezone.utc)

    data_hex = log.get("data", "0x")
    if data_hex.startswith("0x"):
        data_hex = data_hex[2:]
    raw_bytes = bytes.fromhex(data_hex) if data_hex else b""

    # 1. ERC-1155 TransferSingle
    # TransferSingle(address operator, address from, address to, uint256 id, uint256 value)
    if topic0 == TRANSFER_SINGLE_TOPIC and len(topics) >= 4:
        from_addr = topic_to_address(topics[2])
        to_addr = topic_to_address(topics[3])
        if target_wallet.lower() not in (from_addr, to_addr):
            return []

        if len(raw_bytes) >= 64:
            token_id = str(int.from_bytes(raw_bytes[0:32], "big"))
            raw_val = int.from_bytes(raw_bytes[32:64], "big")
            amount = Decimal(raw_val) / CTF_SCALE

            return [
                DecodedChainEvent(
                    event_name="TransferSingle",
                    contract_address=contract_address,
                    transaction_hash=tx_hash,
                    log_index=log_idx,
                    block_number=block_num,
                    block_time=block_time,
                    wallet=target_wallet.lower(),
                    condition_id=None,
                    token_id=token_id,
                    from_address=from_addr,
                    to_address=to_addr,
                    amount=amount,
                    raw_log=log,
                )
            ]

    # 2. ERC-1155 TransferBatch
    # TransferBatch(address operator, address from, address to, uint256[] ids, uint256[] values)
    elif topic0 == TRANSFER_BATCH_TOPIC and len(topics) >= 4:
        from_addr = topic_to_address(topics[2])
        to_addr = topic_to_address(topics[3])
        if target_wallet.lower() not in (from_addr, to_addr):
            return []

        if len(raw_bytes) >= 64:
            offset_ids = int.from_bytes(raw_bytes[0:32], "big")
            offset_vals = int.from_bytes(raw_bytes[32:64], "big")

            if offset_ids + 32 <= len(raw_bytes) and offset_vals + 32 <= len(raw_bytes):
                count_ids = int.from_bytes(raw_bytes[offset_ids : offset_ids + 32], "big")
                decoded_ids = [
                    str(
                        int.from_bytes(
                            raw_bytes[offset_ids + 32 * (1 + i) : offset_ids + 32 * (2 + i)],
                            "big",
                        )
                    )
                    for i in range(count_ids)
                ]

                count_vals = int.from_bytes(raw_bytes[offset_vals : offset_vals + 32], "big")
                decoded_vals = [
                    Decimal(
                        int.from_bytes(
                            raw_bytes[offset_vals + 32 * (1 + i) : offset_vals + 32 * (2 + i)],
                            "big",
                        )
                    )
                    / CTF_SCALE
                    for i in range(count_vals)
                ]

                results = []
                for i in range(min(len(decoded_ids), len(decoded_vals))):
                    results.append(
                        DecodedChainEvent(
                            event_name="TransferBatch",
                            contract_address=contract_address,
                            transaction_hash=tx_hash,
                            log_index=log_idx,
                            block_number=block_num,
                            block_time=block_time,
                            wallet=target_wallet.lower(),
                            condition_id=None,
                            token_id=decoded_ids[i],
                            from_address=from_addr,
                            to_address=to_addr,
                            amount=decoded_vals[i],
                            raw_log=log,
                            batch_index=i,
                        )
                    )
                return results

    # 3. PositionSplit
    # PositionSplit(address stakeholder, address collateralToken, bytes32 parentCollectionId,
    #               bytes32 conditionId, uint256[] partition, uint256 amount)
    elif topic0 == POSITION_SPLIT_TOPIC and len(topics) >= 4:
        stakeholder = topic_to_address(topics[1])
        if target_wallet.lower() != stakeholder:
            return []
        condition_id = topics[3].lower()

        amount = Decimal("0")
        if len(raw_bytes) >= 96:
            raw_amount = int.from_bytes(raw_bytes[64:96], "big")
            amount = Decimal(raw_amount) / CTF_SCALE

        return [
            DecodedChainEvent(
                event_name="PositionSplit",
                contract_address=contract_address,
                transaction_hash=tx_hash,
                log_index=log_idx,
                block_number=block_num,
                block_time=block_time,
                wallet=target_wallet.lower(),
                condition_id=condition_id,
                token_id=None,
                from_address=None,
                to_address=stakeholder,
                amount=amount,
                raw_log=log,
            )
        ]

    # 4. PositionsMerge
    # PositionsMerge(address stakeholder, address collateralToken, bytes32 parentCollectionId,
    #                bytes32 conditionId, uint256[] partition, uint256 amount)
    elif topic0 == POSITIONS_MERGE_TOPIC and len(topics) >= 4:
        stakeholder = topic_to_address(topics[1])
        if target_wallet.lower() != stakeholder:
            return []
        condition_id = topics[3].lower()

        amount = Decimal("0")
        if len(raw_bytes) >= 96:
            raw_amount = int.from_bytes(raw_bytes[64:96], "big")
            amount = Decimal(raw_amount) / CTF_SCALE

        return [
            DecodedChainEvent(
                event_name="PositionsMerge",
                contract_address=contract_address,
                transaction_hash=tx_hash,
                log_index=log_idx,
                block_number=block_num,
                block_time=block_time,
                wallet=target_wallet.lower(),
                condition_id=condition_id,
                token_id=None,
                from_address=stakeholder,
                to_address=None,
                amount=amount,
                raw_log=log,
            )
        ]

    # 5. PayoutRedemption
    # PayoutRedemption(address redeemer, address collateralToken, bytes32 parentCollectionId,
    #                  bytes32 conditionId, uint256[] indexSets, uint256 payout)
    elif topic0 == PAYOUT_REDEMPTION_TOPIC and len(topics) >= 2:
        redeemer = topic_to_address(topics[1])
        if target_wallet.lower() != redeemer:
            return []
        condition_id = None
        amount = Decimal("0")

        if len(raw_bytes) >= 96:
            condition_id = "0x" + raw_bytes[0:32].hex().lower()
            raw_payout = int.from_bytes(raw_bytes[64:96], "big")
            amount = Decimal(raw_payout) / CTF_SCALE

        return [
            DecodedChainEvent(
                event_name="PayoutRedemption",
                contract_address=contract_address,
                transaction_hash=tx_hash,
                log_index=log_idx,
                block_number=block_num,
                block_time=block_time,
                wallet=target_wallet.lower(),
                condition_id=condition_id,
                token_id=None,
                from_address=None,
                to_address=redeemer,
                amount=amount,
                raw_log=log,
            )
        ]

    return []
