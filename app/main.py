import logging
from contextlib import asynccontextmanager
from threading import Thread
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator
from psycopg2 import errorcodes
from psycopg2.extras import Json, RealDictCursor, execute_values

from app.config import Settings, load_settings
from app.db import Database
from app.sql import (
    CANCEL_JOB_INVOICES_SQL,
    CANCEL_JOB_SQL,
    INSERT_JOB_INVOICE_VALUES_SQL,
    INSERT_JOB_SQL,
    SELECT_JOB_RESULTS_SQL,
    SELECT_JOB_STATUS_SQL,
    SELECT_JOB_WITH_COUNTERS_SQL,
)
from app.worker import run_job

logger = logging.getLogger(__name__)
MAX_BATCHES = 32
INVOICE_INSERT_CHUNK_SIZE = 5000


class JobCreateRequest(BaseModel):
    phrases: list[str]
    batches: int = Field(ge=1, le=MAX_BATCHES)
    invoices: list[str]

    @field_validator("phrases")
    @classmethod
    def validate_phrases(cls, values: list[str]) -> list[str]:
        if not values:
            raise ValueError("phrases must not be empty")
        cleaned: list[str] = []
        for value in values:
            item = value.strip() if isinstance(value, str) else ""
            if not item:
                raise ValueError("phrases must contain non-empty strings")
            cleaned.append(item)
        return cleaned

    @field_validator("invoices")
    @classmethod
    def validate_invoices(cls, values: list[str]) -> list[str]:
        if not values:
            raise ValueError("invoices must not be empty")
        cleaned: list[str] = []
        seen: set[str] = set()
        for value in values:
            item = value.strip() if isinstance(value, str) else ""
            if not item:
                raise ValueError("invoices must contain non-empty strings")
            if item in seen:
                raise ValueError("duplicate invoice_id values are not allowed in the same job")
            seen.add(item)
            cleaned.append(item)
        return cleaned


class JobCreateResponse(BaseModel):
    job_id: str


class JobCancelResponse(BaseModel):
    job_id: str
    status: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    db = Database(
        dsn=settings.database_url,
        minconn=settings.db_pool_minconn,
        maxconn=settings.db_pool_maxconn,
    )
    db.init_pool()
    db.init_schema()
    stale_jobs_count = db.reconcile_stale_running_jobs(settings.stale_running_job_minutes)
    app.state.settings = settings
    app.state.db = db
    logger.info("event=app_started stale_jobs_marked_error=%s", stale_jobs_count)
    try:
        yield
    finally:
        db.close()
        logger.info("event=app_stopped")


app = FastAPI(
    title="Invoice Phrase Search API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs", response_model=JobCreateResponse, status_code=status.HTTP_201_CREATED)
def create_job(payload: JobCreateRequest, request: Request) -> JobCreateResponse:
    db: Database = request.app.state.db
    settings: Settings = request.app.state.settings
    if payload.batches > settings.max_batches:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"batches must be <= {settings.max_batches}",
        )

    job_id = str(uuid4())

    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    INSERT_JOB_SQL,
                    (job_id, payload.batches, Json(payload.phrases), len(payload.invoices)),
                )
                _insert_job_invoices(
                    cur=cur,
                    job_id=job_id,
                    invoices=payload.invoices,
                    batches=payload.batches,
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            if getattr(exc, "pgcode", None) == errorcodes.UNIQUE_VIOLATION:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="duplicate_invoice_id_in_job",
                ) from exc
            logger.exception("job_id=%s event=create_job_failed", job_id)
            raise

    worker = Thread(
        target=run_job,
        args=(job_id, db, settings),
        daemon=True,
        name=f"worker-job-{job_id}",
    )
    worker.start()
    logger.info(
        "job_id=%s event=job_created invoices=%s batches=%s",
        job_id,
        len(payload.invoices),
        payload.batches,
    )
    return JobCreateResponse(job_id=job_id)


@app.post("/jobs/{job_id}/cancel", response_model=JobCancelResponse)
def cancel_job(job_id: str, request: Request) -> JobCancelResponse:
    db: Database = request.app.state.db
    with db.get_conn() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(SELECT_JOB_STATUS_SQL, (job_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")

                current_status = row["status"]
                if current_status == "running":
                    cur.execute(CANCEL_JOB_SQL, (job_id,))
                    cur.execute(CANCEL_JOB_INVOICES_SQL, (job_id,))
                    current_status = "canceled"
                    logger.info("job_id=%s event=job_canceled", job_id)

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception:
            conn.rollback()
            logger.exception("job_id=%s event=cancel_job_failed", job_id)
            raise

    return JobCancelResponse(job_id=job_id, status=current_status)


@app.get("/jobs/{job_id}")
def get_job(job_id: str, request: Request) -> dict[str, Any]:
    db: Database = request.app.state.db
    with db.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SELECT_JOB_WITH_COUNTERS_SQL, (job_id,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")

    return {
        "job_id": str(row["job_id"]),
        "status": row["status"],
        "total": int(row["total_invoices"]),
        "queued": int(row["queued"]),
        "running": int(row["running"]),
        "finished": int(row["finished"]),
        "error": int(row["error"]),
        "canceled": int(row["canceled"]),
        "created_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
    }


@app.get("/jobs/{job_id}/results")
def get_job_results(job_id: str, request: Request) -> dict[str, Any]:
    db: Database = request.app.state.db
    with db.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SELECT_JOB_STATUS_SQL, (job_id,))
            job_row = cur.fetchone()
            if not job_row:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job_not_found")

            if job_row["status"] not in {"finished", "error", "canceled"}:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="job_not_finished",
                )

            cur.execute(SELECT_JOB_RESULTS_SQL, (job_id,))
            rows = cur.fetchall()

    return {
        "job_id": str(job_row["job_id"]),
        "status": job_row["status"],
        "results": [
            {
                "invoice_id": row["invoice_id"],
                "batch_id": row["batch_id"],
                "status": row["status"],
                "found": row["found"],
                "result_label": row["result_label"],
                "error_code": row["error_code"],
                "attempts": row["attempts"],
                "matched_phrases": row["matched_phrases"],
                "pdf_url": row["pdf_url"],
                "last_error": row["last_error"],
                "updated_at": row["updated_at"],
            }
            for row in rows
        ],
    }


def _insert_job_invoices(
    cur,
    job_id: str,
    invoices: list[str],
    batches: int,
    chunk_size: int = INVOICE_INSERT_CHUNK_SIZE,
) -> None:
    chunk: list[tuple[str, str, int, str]] = []
    for index, invoice_id in enumerate(invoices):
        chunk.append((job_id, invoice_id, index % batches, "queued"))
        if len(chunk) >= chunk_size:
            execute_values(
                cur,
                INSERT_JOB_INVOICE_VALUES_SQL,
                chunk,
                template="(%s, %s, %s, %s)",
            )
            chunk.clear()
    if chunk:
        execute_values(
            cur,
            INSERT_JOB_INVOICE_VALUES_SQL,
            chunk,
            template="(%s, %s, %s, %s)",
        )
