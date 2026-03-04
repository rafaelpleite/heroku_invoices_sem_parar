import logging
from contextlib import asynccontextmanager
from threading import Thread
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field, field_validator
from psycopg2.extras import Json, RealDictCursor

from app.config import Settings, load_settings
from app.db import Database
from app.sql import (
    INSERT_JOB_INVOICE_SQL,
    INSERT_JOB_SQL,
    SELECT_JOB_RESULTS_SQL,
    SELECT_JOB_STATUS_SQL,
    SELECT_JOB_WITH_COUNTERS_SQL,
)
from app.worker import run_job

logger = logging.getLogger(__name__)


class JobCreateRequest(BaseModel):
    phrases: list[str]
    batches: int = Field(ge=1)
    invoices: list[str]

    @field_validator("phrases", "invoices")
    @classmethod
    def validate_non_empty_string_list(cls, values: list[str]) -> list[str]:
        if not values:
            raise ValueError("List must not be empty")
        cleaned: list[str] = []
        for value in values:
            item = value.strip() if isinstance(value, str) else ""
            if not item:
                raise ValueError("List items must be non-empty strings")
            cleaned.append(item)
        return cleaned


class JobCreateResponse(BaseModel):
    job_id: str


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
    app.state.settings = settings
    app.state.db = db
    logger.info("event=app_started")
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
    job_id = str(uuid4())

    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    INSERT_JOB_SQL,
                    (job_id, payload.batches, Json(payload.phrases), len(payload.invoices)),
                )
                rows = [
                    (job_id, invoice_id, index % payload.batches)
                    for index, invoice_id in enumerate(payload.invoices)
                ]
                cur.executemany(INSERT_JOB_INVOICE_SQL, rows)
            conn.commit()
        except Exception:
            conn.rollback()
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

            if job_row["status"] not in {"finished", "error"}:
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
                "updated_at": row["updated_at"],
            }
            for row in rows
        ],
    }

