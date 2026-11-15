import re

_HEX_ADDRESS_REGEX = re.compile(r"^0x[0-9a-fA-F]{40}$")


def normalize_and_validate_address(address: str) -> str:
    """
    Validate and normalize an Ethereum/Polygon wallet address.

    Rules:
    - Must be a string.
    - Must start with '0x' followed by exactly 40 hexadecimal characters (42 chars total).
    - Normalized output is stripped of whitespace and converted to lowercase.

    Raises ValueError if the address is invalid.
    """
    if not isinstance(address, str):
        raise ValueError(f"Wallet address must be a string, got {type(address).__name__}")

    cleaned = address.strip()

    if not _HEX_ADDRESS_REGEX.match(cleaned):
        raise ValueError(
            f"Invalid wallet address: '{address}'. "
            "Address must start with '0x' followed by 40 hex characters."
        )

    return cleaned.lower()
