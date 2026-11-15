from decimal import Decimal
from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database configuration (PostgreSQL 16 with psycopg 3 async driver)
    database_url: str

    # Application settings
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    environment: str = "development"
    log_level: str = "INFO"

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


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings."""
    return Settings()
