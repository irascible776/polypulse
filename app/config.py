import os
import re
import socket
import uuid
from decimal import Decimal
from functools import lru_cache
from typing import Any, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

_BOOT_ID = uuid.uuid4().hex[:6]


def normalize_database_url(
    raw_url: str,
    *,
    driver: str = "postgresql+psycopg",
    sslmode: Optional[str] = None,
    include_keepalives: bool = True,
) -> tuple[str, dict[str, Any]]:
    """
    Normalize a PostgreSQL URL for SQLAlchemy (+psycopg) or raw psycopg (postgresql).
    Strips sslmode/keepalive params from the URL query and returns them in connect_args
    so libpq never receives duplicate parameters.
    """
    url_str = raw_url.strip()
    if url_str.startswith("postgres://"):
        url_str = "postgresql://" + url_str[len("postgres://") :]
    parsed = make_url(url_str)
    query = dict(parsed.query)
    url_sslmode = query.pop("sslmode", None)
    for key in ("keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count"):
        query.pop(key, None)

    clean = parsed.set(drivername=driver, query=query)
    connect_args: dict[str, Any] = {}
    if include_keepalives:
        connect_args.update(
            {
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 3,
            }
        )
    effective_sslmode = sslmode or (
        url_sslmode[0] if isinstance(url_sslmode, (list, tuple)) else url_sslmode
    )
    if effective_sslmode:
        connect_args["sslmode"] = str(effective_sslmode)
    return clean.render_as_string(hide_password=False), connect_args


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database configuration (PostgreSQL 16 with psycopg 3 async driver)
    database_url: str
    secondary_database_url: Optional[str] = None
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=5, ge=0)
    db_pool_timeout: float = Field(default=30.0, gt=0)
    db_pool_recycle: int = Field(default=300, ge=30)
    db_sslmode: Optional[str] = None
    worker_concurrency: int = Field(default=4, ge=1, le=16)
    ingestion_copy_threshold: int = Field(default=1000, ge=0)
    price_raw_retention_days: int = Field(default=7, ge=1)
    accounting_parallel_processes: int = Field(default=0, ge=0, le=16)
    accounting_parallel_wallets: str = ""
    accounting_condition_replay_wallets: str = ""
    accounting_parallel_timeout_seconds: int = Field(default=120, ge=1)
    accounting_lock_timeout_seconds: int = Field(default=5, ge=1)
    accounting_statement_timeout_seconds: int = Field(default=120, ge=1)
    accounting_attempt_timeout_seconds: float = Field(default=120, gt=0)
    wallet_cycle_timeout_seconds: float = Field(default=180, gt=0)
    accounting_checkpoint_keep: int = Field(default=32, ge=1)
    accounting_checkpoint_interval: int = Field(default=1, ge=1)

    # Application settings
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    environment: str = "development"
    log_level: str = "INFO"
    app_instance_id: Optional[str] = None
    fleet_heartbeat_seconds: float = Field(default=10.0, gt=0)
    shutdown_drain_seconds: float = Field(default=0.0, ge=0)

    # CORS & WebSocket origin policy
    cors_origins: str = ""
    cors_origin_regex: Optional[str] = None
    ws_allow_empty_origin: bool = True

    # Polymarket API endpoints
    canonical_source: str = "data_api"
    data_api_base_url: str = "https://data-api.polymarket.com"
    gamma_api_base_url: str = "https://gamma-api.polymarket.com"
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Optional Polygon RPC endpoint (TODO(verify) for chain slice)
    polygon_rpc_url: Optional[str] = None

    # Serving layer security & authentication
    api_key: Optional[str] = Field(default=None, validation_alias="POLYPULSE_API_KEY")
    max_spread: Decimal = Decimal("0.10")
    live_quote_staleness_seconds: float = 60.0
    last_trade_staleness_seconds: float = 21600.0
    ingestion_stale_seconds: float = 300.0
    reconcile_stale_seconds: float = 1800.0
    resolution_stale_seconds: float = 900.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    def parsed_cors_origins(self) -> list[str]:
        return [
            origin.strip().rstrip("/") for origin in self.cors_origins.split(",") if origin.strip()
        ]

    def is_origin_allowed(self, origin: Optional[str]) -> bool:
        origins = self.parsed_cors_origins()
        regex = (self.cors_origin_regex or "").strip()
        if not origins and not regex:
            return True
        if not origin:
            return self.ws_allow_empty_origin
        clean = origin.strip().rstrip("/")
        if "*" in origins or clean in origins:
            return True
        if regex and re.fullmatch(regex, clean):
            return True
        return False

    def resolve_instance_id(self) -> str:
        base = (
            (self.app_instance_id or "").strip()
            or os.environ.get("RENDER_INSTANCE_ID", "").strip()
            or socket.gethostname()
        )
        return f"{base}-{_BOOT_ID}"

    def sqlalchemy_url_and_args(self, raw_url: Optional[str] = None) -> tuple[str, dict[str, Any]]:
        return normalize_database_url(
            raw_url or self.database_url,
            driver="postgresql+psycopg",
            sslmode=self.db_sslmode,
            include_keepalives=True,
        )

    def psycopg_url_and_args(self, raw_url: Optional[str] = None) -> tuple[str, dict[str, Any]]:
        return normalize_database_url(
            raw_url or self.database_url,
            driver="postgresql",
            sslmode=self.db_sslmode,
            include_keepalives=True,
        )


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
