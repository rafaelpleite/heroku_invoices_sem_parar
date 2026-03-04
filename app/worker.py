import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from psycopg2.extras import RealDictCursor

from app.config import Settings
from app.db import Database
from app.invoice_search import buscar_fatura
from app.sql import (
    COUNT_JOB_ERRORS_SQL,
    FINALIZE_JOB_ERROR_SQL,
    FINALIZE_JOB_FINISHED_SQL,
    FORCE_JOB_ERROR_SQL,
    MARK_INVOICE_RUNNING_SQL,
    MARK_REMAINING_WORKER_ERROR_SQL,
    SELECT_BATCH_INVOICES_SQL,
    SELECT_JOB_METADATA_SQL,
    SET_JOB_STARTED_SQL,
    UPDATE_INVOICE_RESULT_SQL,
)

logger = logging.getLogger(__name__)


def run_job(job_id: str, db: Database, settings: Settings) -> None:
    logger.info("job_id=%s event=run_job_started", job_id)
    try:
        job_metadata = _fetch_job_metadata(job_id=job_id, db=db)
        if not job_metadata:
            logger.error("job_id=%s event=job_not_found_in_worker", job_id)
            return

        phrases = job_metadata["phrases"] or []
        batches = int(job_metadata["batches"])
        _mark_job_started(job_id=job_id, db=db)

        any_batch_failed = False
        with ThreadPoolExecutor(max_workers=batches) as executor:
            futures = {
                executor.submit(process_batch, job_id, batch_id, phrases, db, settings): batch_id
                for batch_id in range(batches)
            }
            for future in as_completed(futures):
                batch_id = futures[future]
                try:
                    future.result()
                    logger.info("job_id=%s batch_id=%s event=batch_finished", job_id, batch_id)
                except Exception:
                    any_batch_failed = True
                    logger.exception(
                        "job_id=%s batch_id=%s event=batch_crashed",
                        job_id,
                        batch_id,
                    )

        if any_batch_failed:
            _mark_remaining_worker_error(job_id=job_id, db=db)

        _finalize_job(job_id=job_id, db=db)
        logger.info("job_id=%s event=run_job_finished", job_id)
    except Exception as exc:
        logger.exception("job_id=%s event=run_job_fatal_error", job_id)
        _mark_remaining_worker_error(job_id=job_id, db=db)
        _force_job_error(job_id=job_id, db=db, error_message=f"worker_fatal: {exc}")


def process_batch(
    job_id: str,
    batch_id: int,
    phrases: list[str],
    db: Database,
    settings: Settings,
) -> None:
    logger.info("job_id=%s batch_id=%s event=process_batch_started", job_id, batch_id)
    invoices = _fetch_batch_invoices(job_id=job_id, batch_id=batch_id, db=db)

    for invoice in invoices:
        invoice_row_id = invoice["id"]
        invoice_id = invoice["invoice_id"]
        logger.info(
            "job_id=%s batch_id=%s invoice_id=%s event=invoice_processing_started",
            job_id,
            batch_id,
            invoice_id,
        )
        _mark_invoice_running(invoice_row_id=invoice_row_id, db=db)

        result = buscar_fatura(
            invoice_id=invoice_id,
            phrases=phrases,
            base_url=settings.invoice_api_base_url,
            api_key=settings.heroku_api_key,
            timeout=settings.request_timeout_seconds,
            max_attempts=3,
            logger=logger,
            log_context=f"job_id={job_id} batch_id={batch_id}",
        )

        _update_invoice_result(
            invoice_row_id=invoice_row_id,
            result=result,
            db=db,
        )
        logger.info(
            "job_id=%s batch_id=%s invoice_id=%s event=invoice_processing_finished status=%s result_label=%s",
            job_id,
            batch_id,
            invoice_id,
            result["status"],
            result["result_label"],
        )

    logger.info("job_id=%s batch_id=%s event=process_batch_finished", job_id, batch_id)


def _fetch_job_metadata(job_id: str, db: Database) -> dict[str, Any] | None:
    with db.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SELECT_JOB_METADATA_SQL, (job_id,))
            return cur.fetchone()


def _mark_job_started(job_id: str, db: Database) -> None:
    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(SET_JOB_STARTED_SQL, (job_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _fetch_batch_invoices(job_id: str, batch_id: int, db: Database) -> list[dict[str, Any]]:
    with db.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SELECT_BATCH_INVOICES_SQL, (job_id, batch_id))
            return list(cur.fetchall())


def _mark_invoice_running(invoice_row_id: int, db: Database) -> None:
    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(MARK_INVOICE_RUNNING_SQL, (invoice_row_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _update_invoice_result(invoice_row_id: int, result: dict[str, Any], db: Database) -> None:
    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    UPDATE_INVOICE_RESULT_SQL,
                    (
                        result["status"],
                        result["found"],
                        result["result_label"],
                        result["error_code"],
                        int(result.get("attempts", 1)),
                        invoice_row_id,
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _mark_remaining_worker_error(job_id: str, db: Database) -> None:
    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(MARK_REMAINING_WORKER_ERROR_SQL, (job_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _finalize_job(job_id: str, db: Database) -> None:
    with db.get_conn() as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(COUNT_JOB_ERRORS_SQL, (job_id,))
                row = cur.fetchone()
                error_count = int(row["error_count"]) if row else 0
                if error_count > 0:
                    cur.execute(FINALIZE_JOB_ERROR_SQL, (job_id,))
                else:
                    cur.execute(FINALIZE_JOB_FINISHED_SQL, (job_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def _force_job_error(job_id: str, db: Database, error_message: str) -> None:
    with db.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(FORCE_JOB_ERROR_SQL, (error_message, job_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise

