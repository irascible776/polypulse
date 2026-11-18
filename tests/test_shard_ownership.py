from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from app.storage.database import async_session_factory
from app.storage.models import TrackedWallet
from app.storage.sharding import wallet_bucket


@pytest.mark.asyncio
async def test_database_rejects_wallet_on_wrong_shard():
    wallets = {}
    while len(wallets) < 2:
        wallet = "0x" + uuid4().hex + "0" * 8
        wallets.setdefault(0 if wallet_bucket(wallet) < 8 else 1, wallet)
    async with async_session_factory() as session:
        await session.execute(text("UPDATE wallet_shard_settings SET shard_id = 0 WHERE id = 1"))
        session.add(TrackedWallet(address=wallets[0]))
        await session.flush()
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                session.add(TrackedWallet(address=wallets[1]))
                await session.flush()
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                await session.execute(
                    text("UPDATE tracked_wallets SET address = :new WHERE address = :old"),
                    {"new": wallets[1], "old": wallets[0]},
                )
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text("DELETE FROM wallet_shard_settings"))
        await session.rollback()
