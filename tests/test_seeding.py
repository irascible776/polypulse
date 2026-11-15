import pytest

from app.storage.seeding import load_wallets_from_yaml


def test_load_wallets_from_yaml_valid(tmp_path):
    yaml_content = """
wallets:
  - address: "0x951634357346bb291df117f9ac410100feea2104"
    label: "AppleTiiime"
    tracking_start: "2026-11-12T00:00:00Z"
  - address: "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"
    label: "JnStPrdctns"
"""
    p = tmp_path / "wallets.yaml"
    p.write_text(yaml_content)

    wallets = load_wallets_from_yaml(p)
    assert len(wallets) == 2
    assert wallets[0]["address"] == "0x951634357346bb291df117f9ac410100feea2104"
    assert wallets[0]["label"] == "AppleTiiime"
    assert wallets[1]["address"] == "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92"
    assert wallets[1]["label"] == "JnStPrdctns"


def test_load_wallets_from_yaml_normalizes_address(tmp_path):
    # Checksum address with spaces
    yaml_content = """
wallets:
  - address: "  0x951634357346BB291dF117f9Ac410100FEea2104  "
    label: "MixedCase"
"""
    p = tmp_path / "wallets.yaml"
    p.write_text(yaml_content)

    wallets = load_wallets_from_yaml(p)
    assert len(wallets) == 1
    assert wallets[0]["address"] == "0x951634357346bb291df117f9ac410100feea2104"


def test_load_wallets_from_yaml_rejects_invalid_address(tmp_path):
    yaml_content = """
wallets:
  - address: "not_a_valid_hex_address"
"""
    p = tmp_path / "wallets.yaml"
    p.write_text(yaml_content)

    with pytest.raises(ValueError):
        load_wallets_from_yaml(p)


def test_actual_repo_wallets_yaml():
    wallets = load_wallets_from_yaml("wallets.yaml")
    assert len(wallets) >= 3
    addresses = [w["address"] for w in wallets]
    assert "0x951634357346bb291df117f9ac410100feea2104" in addresses
    assert "0xa98a9d23e7211c22d2259b68b1bc9d47b3013b92" in addresses
    assert "0x0000000000000000000000000000000000000001" in addresses
