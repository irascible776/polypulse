"""Stable wallet bucket assignment shared by workers and database routing."""

import hashlib

WALLET_BUCKETS = 16


def wallet_bucket(wallet: str) -> int:
    return hashlib.sha256(wallet.lower().encode("utf-8")).digest()[0] % WALLET_BUCKETS
