import pytest

from app.storage.validation import normalize_and_validate_address


def test_normalize_and_validate_good_address():
    # Lowercase standard address
    addr_lower = "0x951634357346bb291df117f9ac410100feea2104"
    assert normalize_and_validate_address(addr_lower) == addr_lower

    # Mixed-case / checksum address
    addr_mixed = "0x951634357346BB291dF117f9Ac410100FEea2104"
    assert normalize_and_validate_address(addr_mixed) == addr_lower

    # With leading/trailing whitespace
    addr_whitespace = "  0x951634357346bb291df117f9ac410100feea2104  "
    assert normalize_and_validate_address(addr_whitespace) == addr_lower


def test_normalize_and_validate_idempotent():
    raw_address = "0x951634357346BB291dF117f9Ac410100FEea2104"
    first_pass = normalize_and_validate_address(raw_address)
    second_pass = normalize_and_validate_address(first_pass)
    assert first_pass == second_pass
    assert second_pass == "0x951634357346bb291df117f9ac410100feea2104"


@pytest.mark.parametrize(
    "bad_input",
    [
        "",  # Empty string
        "0x123",  # Too short
        "0x" + "a" * 39,  # 39 hex chars (41 total)
        "0x" + "a" * 41,  # 41 hex chars (43 total)
        "951634357346bb291df117f9ac410100feea2104",  # Missing 0x prefix
        "0x951634357346bb291df117f9ac410100feea210z",  # Non-hex character 'z'
        "0x951634357346bb291df117f9ac410100feea210 ",  # Inner space
        None,  # NoneType
        1234567890,  # Int
    ],
)
def test_normalize_and_validate_rejects_bad_input(bad_input):
    with pytest.raises(ValueError):
        normalize_and_validate_address(bad_input)
