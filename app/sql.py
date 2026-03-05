SCHEMA_STATEMENTS = [
    "CREATE SCHEMA IF NOT EXISTS heroku",
    """
    CREATE TABLE IF NOT EXISTS heroku.jobs (
        job_id UUID PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at TIMESTAMPTZ NULL,
        finished_at TIMESTAMPTZ NULL,
        status TEXT NOT NULL,
        batches INT NOT NULL,
        phrases JSONB NOT NULL,
        total_invoices INT NOT NULL,
        error_message TEXT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS heroku.job_invoices (
        id BIGSERIAL PRIMARY KEY,
        job_id UUID NOT NULL REFERENCES heroku.jobs(job_id) ON DELETE CASCADE,
        invoice_id TEXT NOT NULL,
        batch_id INT NOT NULL,
        status TEXT NOT NULL,
        attempts INT NOT NULL DEFAULT 0,
        found BOOLEAN NULL,
        result_label TEXT NULL,
        error_code INT NULL,
        matched_phrases JSONB NULL,
        pdf_url TEXT NULL,
        last_error TEXT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "ALTER TABLE heroku.jobs DROP CONSTRAINT IF EXISTS jobs_status_check",
    """
    ALTER TABLE heroku.jobs
    ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('running', 'finished', 'error', 'canceled'))
    """,
    "ALTER TABLE heroku.job_invoices DROP CONSTRAINT IF EXISTS job_invoices_status_check",
    """
    ALTER TABLE heroku.job_invoices
    ADD CONSTRAINT job_invoices_status_check
    CHECK (status IN ('queued', 'running', 'finished', 'error', 'canceled'))
    """,
    "ALTER TABLE heroku.job_invoices ADD COLUMN IF NOT EXISTS matched_phrases JSONB NULL",
    "ALTER TABLE heroku.job_invoices ADD COLUMN IF NOT EXISTS pdf_url TEXT NULL",
    "ALTER TABLE heroku.job_invoices ADD COLUMN IF NOT EXISTS last_error TEXT NULL",
    "CREATE INDEX IF NOT EXISTS idx_job_invoices_job_id ON heroku.job_invoices(job_id)",
    "CREATE INDEX IF NOT EXISTS idx_job_invoices_job_batch ON heroku.job_invoices(job_id, batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_job_invoices_job_status ON heroku.job_invoices(job_id, status)",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_job_invoices_job_invoice ON heroku.job_invoices(job_id, invoice_id)",
]

INSERT_JOB_SQL = """
INSERT INTO heroku.jobs (job_id, status, batches, phrases, total_invoices)
VALUES (%s, 'running', %s, %s, %s)
"""

INSERT_JOB_INVOICE_VALUES_SQL = """
INSERT INTO heroku.job_invoices (job_id, invoice_id, batch_id, status)
VALUES %s
"""

SELECT_JOB_WITH_COUNTERS_SQL = """
SELECT
    j.job_id,
    j.status,
    j.created_at,
    j.started_at,
    j.finished_at,
    j.total_invoices,
    COALESCE(COUNT(*) FILTER (WHERE ji.status = 'queued'), 0) AS queued,
    COALESCE(COUNT(*) FILTER (WHERE ji.status = 'running'), 0) AS running,
    COALESCE(COUNT(*) FILTER (WHERE ji.status = 'finished'), 0) AS finished,
    COALESCE(COUNT(*) FILTER (WHERE ji.status = 'error'), 0) AS error,
    COALESCE(COUNT(*) FILTER (WHERE ji.status = 'canceled'), 0) AS canceled
FROM heroku.jobs j
LEFT JOIN heroku.job_invoices ji ON ji.job_id = j.job_id
WHERE j.job_id = %s
GROUP BY j.job_id
"""

SELECT_JOB_STATUS_SQL = "SELECT job_id, status FROM heroku.jobs WHERE job_id = %s"

SELECT_JOB_RESULTS_SQL = """
SELECT
    invoice_id,
    batch_id,
    status,
    found,
    result_label,
    error_code,
    attempts,
    matched_phrases,
    pdf_url,
    last_error,
    updated_at
FROM heroku.job_invoices
WHERE job_id = %s
ORDER BY id
"""

SELECT_JOB_METADATA_SQL = """
SELECT job_id, phrases, batches, status
FROM heroku.jobs
WHERE job_id = %s
"""

SET_JOB_STARTED_SQL = """
UPDATE heroku.jobs
SET status = 'running', started_at = COALESCE(started_at, NOW())
WHERE job_id = %s AND status = 'running'
"""

CLAIM_NEXT_INVOICE_SQL = """
UPDATE heroku.job_invoices
SET status = 'running', attempts = attempts + 1, updated_at = NOW()
WHERE id = (
    SELECT id
    FROM heroku.job_invoices
    WHERE job_id = %s AND batch_id = %s AND status = 'queued'
    ORDER BY id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, invoice_id, attempts
"""

MARK_INVOICE_CANCELED_SQL = """
UPDATE heroku.job_invoices
SET
    status = 'canceled',
    found = NULL,
    result_label = 'cancelado',
    error_code = NULL,
    matched_phrases = NULL,
    last_error = 'canceled_by_user',
    updated_at = NOW()
WHERE id = %s AND status = 'running'
"""

UPDATE_INVOICE_RESULT_SQL = """
UPDATE heroku.job_invoices
SET
    status = %s,
    found = %s,
    result_label = %s,
    error_code = %s,
    attempts = GREATEST(attempts, %s),
    matched_phrases = %s,
    pdf_url = COALESCE(%s, pdf_url),
    last_error = %s,
    updated_at = NOW()
WHERE id = %s AND status <> 'canceled'
RETURNING id
"""

MARK_REMAINING_WORKER_ERROR_SQL = """
UPDATE heroku.job_invoices
SET
    status = 'error',
    found = NULL,
    result_label = 'erro_worker',
    error_code = NULL,
    matched_phrases = NULL,
    last_error = 'worker_crash',
    updated_at = NOW()
WHERE job_id = %s AND status IN ('queued', 'running')
"""

COUNT_JOB_ERRORS_SQL = """
SELECT COUNT(*) AS error_count
FROM heroku.job_invoices
WHERE job_id = %s AND status = 'error'
"""

FINALIZE_JOB_FINISHED_SQL = """
UPDATE heroku.jobs
SET status = 'finished', finished_at = NOW(), error_message = NULL
WHERE job_id = %s AND status = 'running'
"""

FINALIZE_JOB_ERROR_SQL = """
UPDATE heroku.jobs
SET status = 'error', finished_at = NOW(), error_message = 'one_or_more_invoices_failed'
WHERE job_id = %s AND status = 'running'
"""

FORCE_JOB_ERROR_SQL = """
UPDATE heroku.jobs
SET status = 'error', finished_at = NOW(), error_message = %s
WHERE job_id = %s AND status <> 'canceled'
"""

CANCEL_JOB_SQL = """
UPDATE heroku.jobs
SET status = 'canceled', finished_at = NOW(), error_message = 'canceled_by_user'
WHERE job_id = %s AND status = 'running'
"""

CANCEL_JOB_INVOICES_SQL = """
UPDATE heroku.job_invoices
SET
    status = 'canceled',
    found = NULL,
    result_label = 'cancelado',
    error_code = NULL,
    matched_phrases = NULL,
    last_error = 'canceled_by_user',
    updated_at = NOW()
WHERE job_id = %s AND status IN ('queued', 'running')
"""

SET_JOB_CANCELED_FINISHED_AT_SQL = """
UPDATE heroku.jobs
SET finished_at = COALESCE(finished_at, NOW())
WHERE job_id = %s AND status = 'canceled'
"""

MARK_STALE_RUNNING_JOBS_SQL = """
UPDATE heroku.jobs
SET status = 'error', finished_at = NOW(), error_message = 'stale_running_job_on_startup'
WHERE status = 'running'
  AND COALESCE(started_at, created_at) < (NOW() - MAKE_INTERVAL(mins => %s))
RETURNING job_id
"""

MARK_STALE_JOB_INVOICES_SQL = """
UPDATE heroku.job_invoices
SET
    status = 'error',
    found = NULL,
    result_label = 'erro_worker',
    error_code = NULL,
    matched_phrases = NULL,
    last_error = 'stale_running_job_on_startup',
    updated_at = NOW()
WHERE job_id = %s AND status IN ('queued', 'running')
"""
