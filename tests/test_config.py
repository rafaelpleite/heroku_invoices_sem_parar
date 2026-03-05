import pytest

import app.config as config_module
from app.config import load_settings, normalize_database_url


def test_normalize_database_url_keeps_postgresql_unchanged() -> None:
    value = "postgresql://user:pass@localhost:5432/mydb?sslmode=require"
    assert normalize_database_url(value) == value


def test_normalize_database_url_converts_sqlalchemy_style() -> None:
    value = "postgresql+psycopg2://user:pass@localhost:5432/mydb"
    assert normalize_database_url(value) == "postgresql://user:pass@localhost:5432/mydb"


def test_normalize_database_url_converts_postgres_alias() -> None:
    value = "postgres://user:pass@localhost:5432/mydb"
    assert normalize_database_url(value) == "postgresql://user:pass@localhost:5432/mydb"


def test_load_settings_uses_normalized_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_dotenv", lambda: None)
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://user:pass@localhost:5432/mydb")
    monkeypatch.setenv("API_BEARER_KEY", "api-key")
    monkeypatch.setenv("HEROKU_API_KEY", "token")
    monkeypatch.setenv("INVOICE_DEBUG_LOGS", "true")
    monkeypatch.setenv("INVOICE_DEBUG_BODY_LIMIT", "500")
    settings = load_settings()
    assert settings.database_url == "postgresql://user:pass@localhost:5432/mydb"
    assert settings.api_bearer_key == "api-key"
    assert settings.invoice_debug_logs is True
    assert settings.invoice_debug_body_limit == 500


def test_load_settings_rejects_empty_database_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_dotenv", lambda: None)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("API_BEARER_KEY", "api-key")
    monkeypatch.setenv("HEROKU_API_KEY", "token")
    with pytest.raises(RuntimeError, match="Missing required environment variable: DATABASE_URL"):
        load_settings()


def test_load_settings_rejects_empty_api_bearer_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_dotenv", lambda: None)
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/mydb")
    monkeypatch.delenv("API_BEARER_KEY", raising=False)
    monkeypatch.setenv("HEROKU_API_KEY", "token")
    with pytest.raises(RuntimeError, match="Missing required environment variable: API_BEARER_KEY"):
        load_settings()


def test_load_settings_clamps_debug_body_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config_module, "load_dotenv", lambda: None)
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/mydb")
    monkeypatch.setenv("API_BEARER_KEY", "api-key")
    monkeypatch.setenv("HEROKU_API_KEY", "token")
    monkeypatch.setenv("INVOICE_DEBUG_BODY_LIMIT", "10")
    settings = load_settings()
    assert settings.invoice_debug_body_limit == 50
