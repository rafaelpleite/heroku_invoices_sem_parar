SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS jobs (
        job_id UUID PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at TIMESTAMPTZ NULL,
        finished_at TIMESTAMPTZ NULL,
        status TEXT NOT NULL CHECK (status IN ('running', 'finished', 'error')),
        batches INT NOT NULL,
        phrases JSONB NOT NULL,
        total_invoices INT NOT NULL,
        error_message TEXT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS job_invoices (
        id BIGSERIAL PRIMARY KEY,
        job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
        invoice_id TEXT NOT NULL,
        batch_id INT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'finished', 'error')),
        attempts INT NOT NULL DEFAULT 0,
        found BOOLEAN NULL,
        result_label TEXT NULL,
        error_code INT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_job_invoices_job_id ON job_invoices(job_id)",
    "CREATE INDEX IF NOT EXISTS idx_job_invoices_job_batch ON job_invoices(job_id, batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_job_invoices_job_status ON job_invoices(job_id, status)",
]

INSERT_JOB_SQL = """
INSERT INTO jobs (job_id, status, batches, phrases, total_invoices)
VALUES (%s, 'running', %s, %s, %s)
"""

INSERT_JOB_INVOICE_SQL = """
INSERT INTO job_invoices (job_id, invoice_id, batch_id, status)
VALUES (%s, %s, %s, 'queued')
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
    COALESCE(COUNT(*) FILTER (WHERE ji.status = 'error'), 0) AS error
FROM jobs j
LEFT JOIN job_invoices ji ON ji.job_id = j.job_id
WHERE j.job_id = %s
GROUP BY j.job_id
"""

SELECT_JOB_STATUS_SQL = "SELECT job_id, status FROM jobs WHERE job_id = %s"

SELECT_JOB_RESULTS_SQL = """
SELECT invoice_id, batch_id, status, found, result_label, error_code, attempts, updated_at
FROM job_invoices
WHERE job_id = %s
ORDER BY id
"""

SELECT_JOB_METADATA_SQL = """
SELECT job_id, phrases, batches
FROM jobs
WHERE job_id = %s
"""

SET_JOB_STARTED_SQL = """
UPDATE jobs
SET status = 'running', started_at = COALESCE(started_at, NOW())
WHERE job_id = %s
"""

SELECT_BATCH_INVOICES_SQL = """
SELECT id, invoice_id
FROM job_invoices
WHERE job_id = %s AND batch_id = %s
ORDER BY id
"""

MARK_INVOICE_RUNNING_SQL = """
UPDATE job_invoices
SET status = 'running', attempts = attempts + 1, updated_at = NOW()
WHERE id = %s
"""

UPDATE_INVOICE_RESULT_SQL = """
UPDATE job_invoices
SET
    status = %s,
    found = %s,
    result_label = %s,
    error_code = %s,
    attempts = GREATEST(attempts, %s),
    updated_at = NOW()
WHERE id = %s
"""

MARK_REMAINING_WORKER_ERROR_SQL = """
UPDATE job_invoices
SET
    status = 'error',
    found = NULL,
    result_label = 'erro_worker',
    error_code = NULL,
    updated_at = NOW()
WHERE job_id = %s AND status IN ('queued', 'running')
"""

COUNT_JOB_ERRORS_SQL = """
SELECT COUNT(*) AS error_count
FROM job_invoices
WHERE job_id = %s AND status = 'error'
"""

FINALIZE_JOB_FINISHED_SQL = """
UPDATE jobs
SET status = 'finished', finished_at = NOW(), error_message = NULL
WHERE job_id = %s
"""

FINALIZE_JOB_ERROR_SQL = """
UPDATE jobs
SET status = 'error', finished_at = NOW(), error_message = 'one_or_more_invoices_failed'
WHERE job_id = %s
"""

FORCE_JOB_ERROR_SQL = """
UPDATE jobs
SET status = 'error', finished_at = NOW(), error_message = %s
WHERE job_id = %s
"""

