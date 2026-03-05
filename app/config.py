import os
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    heroku_api_key: str
    invoice_api_base_url: str
    log_level: str
    invoice_api_timeout_seconds: int
    pdf_download_timeout_seconds: int
    db_pool_minconn: int
    db_pool_maxconn: int
    stale_running_job_minutes: int
    max_batches: int
    invoice_debug_logs: bool
    invoice_debug_body_limit: int


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def normalize_database_url(raw_database_url: str) -> str:
    database_url = raw_database_url.strip()

    if database_url.startswith("postgresql+psycopg2://"):
        database_url = "postgresql://" + database_url[len("postgresql+psycopg2://") :]
    elif database_url.startswith("postgres://"):
        database_url = "postgresql://" + database_url[len("postgres://") :]

    parsed = urlparse(database_url)
    if parsed.scheme != "postgresql":
        raise RuntimeError(
            "Invalid DATABASE_URL scheme. Expected postgresql:// "
            "(postgresql+psycopg2:// and postgres:// are also accepted)."
        )
    if not parsed.netloc:
        raise RuntimeError("Invalid DATABASE_URL format: missing host/credentials section")

    return database_url


def load_settings() -> Settings:
    load_dotenv()

    raw_database_url = os.getenv("DATABASE_URL", "").strip()
    heroku_api_key = os.getenv("HEROKU_API_KEY", "").strip()
    invoice_api_base_url = os.getenv(
        "INVOICE_API_BASE_URL",
        "https://semparar-production.herokuapp.com/api/v1/invoices/",
    ).strip()
    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    invoice_api_timeout_seconds = int(os.getenv("INVOICE_API_TIMEOUT_SECONDS", "20"))
    pdf_download_timeout_seconds = int(os.getenv("PDF_DOWNLOAD_TIMEOUT_SECONDS", "20"))
    stale_running_job_minutes = int(os.getenv("STALE_RUNNING_JOB_MINUTES", "30"))
    max_batches = int(os.getenv("MAX_BATCHES", "32"))
    invoice_debug_logs = _parse_bool_env("INVOICE_DEBUG_LOGS", False)
    invoice_debug_body_limit = int(os.getenv("INVOICE_DEBUG_BODY_LIMIT", "300"))

    if not raw_database_url:
        raise RuntimeError("Missing required environment variable: DATABASE_URL")
    if not heroku_api_key:
        raise RuntimeError("Missing required environment variable: HEROKU_API_KEY")

    database_url = normalize_database_url(raw_database_url)

    if not invoice_api_base_url.endswith("/"):
        invoice_api_base_url = f"{invoice_api_base_url}/"

    return Settings(
        database_url=database_url,
        heroku_api_key=heroku_api_key,
        invoice_api_base_url=invoice_api_base_url,
        log_level=log_level or "INFO",
        invoice_api_timeout_seconds=max(1, invoice_api_timeout_seconds),
        pdf_download_timeout_seconds=max(1, pdf_download_timeout_seconds),
        db_pool_minconn=1,
        db_pool_maxconn=20,
        stale_running_job_minutes=max(1, stale_running_job_minutes),
        max_batches=max(1, max_batches),
        invoice_debug_logs=invoice_debug_logs,
        invoice_debug_body_limit=max(50, invoice_debug_body_limit),
    )
