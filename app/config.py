import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    heroku_api_key: str
    invoice_api_base_url: str
    log_level: str
    request_timeout_seconds: int
    db_pool_minconn: int
    db_pool_maxconn: int


def load_settings() -> Settings:
    load_dotenv()

    database_url = os.getenv("DATABASE_URL", "").strip()
    heroku_api_key = os.getenv("HEROKU_API_KEY", "").strip()
    invoice_api_base_url = os.getenv(
        "INVOICE_API_BASE_URL",
        "https://semparar-production.herokuapp.com/api/v1/invoices/",
    ).strip()
    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()

    if not database_url:
        raise RuntimeError("Missing required environment variable: DATABASE_URL")
    if not heroku_api_key:
        raise RuntimeError("Missing required environment variable: HEROKU_API_KEY")

    if not invoice_api_base_url.endswith("/"):
        invoice_api_base_url = f"{invoice_api_base_url}/"

    return Settings(
        database_url=database_url,
        heroku_api_key=heroku_api_key,
        invoice_api_base_url=invoice_api_base_url,
        log_level=log_level or "INFO",
        request_timeout_seconds=20,
        db_pool_minconn=1,
        db_pool_maxconn=20,
    )

