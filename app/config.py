import os
from dataclasses import dataclass

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


def load_settings() -> Settings:
    load_dotenv()

    database_url = os.getenv("DATABASE_URL", "").strip()
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
        invoice_api_timeout_seconds=max(1, invoice_api_timeout_seconds),
        pdf_download_timeout_seconds=max(1, pdf_download_timeout_seconds),
        db_pool_minconn=1,
        db_pool_maxconn=20,
        stale_running_job_minutes=max(1, stale_running_job_minutes),
        max_batches=max(1, max_batches),
    )
