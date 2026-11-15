from app.config import Settings, get_settings


def test_settings_load_defaults():
    settings = get_settings()
    assert settings.app_port == 8000
    assert settings.canonical_source == "data_api"
    assert "polypulse" in settings.database_url
    assert settings.data_api_base_url == "https://data-api.polymarket.com"


def test_settings_custom_env(monkeypatch):
    monkeypatch.setenv("CANONICAL_SOURCE", "polygon_chain")
    monkeypatch.setenv("APP_PORT", "9000")
    custom = Settings()
    assert custom.canonical_source == "polygon_chain"
    assert custom.app_port == 9000
