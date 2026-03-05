import logging
from contextlib import contextmanager
from typing import Generator

from psycopg2 import InterfaceError, OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from app.sql import MARK_STALE_JOB_INVOICES_SQL, MARK_STALE_RUNNING_JOBS_SQL, SCHEMA_STATEMENTS

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 20):
        self._dsn = dsn
        self._minconn = minconn
        self._maxconn = maxconn
        self._pool: ThreadedConnectionPool | None = None

    def init_pool(self) -> None:
        if self._pool is not None:
            return
        try:
            self._pool = ThreadedConnectionPool(
                minconn=self._minconn,
                maxconn=self._maxconn,
                dsn=self._dsn,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
            )
        except Exception as exc:
            raise RuntimeError(
                "Failed to initialize Postgres connection pool. "
                "Check DATABASE_URL format and credentials. Accepted URL formats: "
                "postgresql://..., postgresql+psycopg2://..., postgres://..."
            ) from exc

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
            if getattr(conn, "closed", 1):
                self._pool.putconn(conn, close=True)
            else:
                self._pool.putconn(conn)

    def safe_rollback(self, conn) -> None:
        try:
            conn.rollback()
        except Exception as exc:  # never mask the original failure in cleanup path
            logger.warning("event=safe_rollback_skipped error=%s", exc)

    @staticmethod
    def is_transient_db_error(exc: Exception) -> bool:
        return isinstance(exc, (OperationalError, InterfaceError))

    def init_schema(self) -> None:
        with self.get_conn() as conn:
            try:
                with conn.cursor() as cur:
                    for stmt in SCHEMA_STATEMENTS:
                        cur.execute(stmt)
                conn.commit()
            except Exception:
                self.safe_rollback(conn)
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
                self.safe_rollback(conn)
                raise
