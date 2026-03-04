from contextlib import contextmanager
from typing import Generator

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from app.sql import MARK_STALE_JOB_INVOICES_SQL, MARK_STALE_RUNNING_JOBS_SQL, SCHEMA_STATEMENTS


class Database:
    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 20):
        self._dsn = dsn
        self._minconn = minconn
        self._maxconn = maxconn
        self._pool: ThreadedConnectionPool | None = None

    def init_pool(self) -> None:
        if self._pool is not None:
            return
        self._pool = ThreadedConnectionPool(
            minconn=self._minconn,
            maxconn=self._maxconn,
            dsn=self._dsn,
        )

    def close(self) -> None:
        if self._pool is None:
            return
        self._pool.closeall()
        self._pool = None

    @contextmanager
    def get_conn(self) -> Generator:
        if self._pool is None:
            raise RuntimeError("Database pool not initialized")
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def init_schema(self) -> None:
        with self.get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    for stmt in SCHEMA_STATEMENTS:
                        cur.execute(stmt)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def reconcile_stale_running_jobs(self, stale_minutes: int) -> int:
        with self.get_conn() as conn:
            try:
                stale_job_ids: list[str] = []
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(MARK_STALE_RUNNING_JOBS_SQL, (stale_minutes,))
                    rows = cur.fetchall()
                    stale_job_ids = [str(row["job_id"]) for row in rows]
                    for job_id in stale_job_ids:
                        cur.execute(MARK_STALE_JOB_INVOICES_SQL, (job_id,))
                conn.commit()
                return len(stale_job_ids)
            except Exception:
                conn.rollback()
                raise
