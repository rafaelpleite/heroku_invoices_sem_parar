from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from psycopg2 import InterfaceError, OperationalError

from app.config import Settings
from app.main import JobCreateRequest, cancel_job, create_job, get_job, get_job_results
from app.sql import (
    CANCEL_JOB_INVOICES_SQL,
    CANCEL_JOB_SQL,
    INSERT_JOB_SQL,
    SELECT_JOB_RESULTS_SQL,
    SELECT_JOB_STATUS_SQL,
    SELECT_JOB_WITH_COUNTERS_SQL,
)


class FakeCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None, execute_exception=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.executed = []
        self.execute_exception = execute_exception

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        if self.execute_exception is not None:
            raise self.execute_exception
        self.executed.append((sql, params))

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    def fetchall(self):
        return self.fetchall_values.pop(0) if self.fetchall_values else []


class FakeConn:
    def __init__(self, cursor: FakeCursor, rollback_exception: Exception | None = None):
        self._cursor = cursor
        self.commit_count = 0
        self.rollback_count = 0
        self.rollback_exception = rollback_exception

    def cursor(self, cursor_factory=None):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1
        if self.rollback_exception is not None:
            raise self.rollback_exception


class FakeDB:
    def __init__(self, conn: FakeConn):
        self._conn = conn
        self.safe_rollback_count = 0

    @contextmanager
    def get_conn(self):
        yield self._conn

    def safe_rollback(self, conn):
        self.safe_rollback_count += 1
        try:
            conn.rollback()
        except Exception:
            return

    @staticmethod
    def is_transient_db_error(exc: Exception) -> bool:
        return isinstance(exc, (OperationalError, InterfaceError))


class FakeThread:
    instances = []

    def __init__(self, target, args=(), daemon=False, name=None):
        self.target = target
        self.args = args
        self.daemon = daemon
        self.name = name
        self.started = False
        FakeThread.instances.append(self)

    def start(self):
        self.started = True


def build_request(fake_db: FakeDB, max_batches: int = 32) -> SimpleNamespace:
    settings = Settings(
        database_url="postgresql://x",
        heroku_api_key="token",
        invoice_api_base_url="https://base/",
        log_level="INFO",
        invoice_api_timeout_seconds=20,
        pdf_download_timeout_seconds=20,
        db_pool_minconn=1,
        db_pool_maxconn=20,
        stale_running_job_minutes=30,
        max_batches=max_batches,
        invoice_debug_logs=False,
        invoice_debug_body_limit=300,
    )
    app = SimpleNamespace(state=SimpleNamespace(db=fake_db, settings=settings))
    return SimpleNamespace(app=app)


def test_job_request_validation_errors():
    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate({"phrases": [], "batches": 1, "invoices": ["1"]})

    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate({"phrases": ["ok"], "batches": 1, "invoices": []})

    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate({"phrases": ["ok"], "batches": 0, "invoices": ["1"]})

    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate({"phrases": ["ok"], "batches": 33, "invoices": ["1"]})

    with pytest.raises(ValidationError):
        JobCreateRequest.model_validate(
            {"phrases": ["ok"], "batches": 1, "invoices": ["1", "1"]}
        )


def test_create_job_inserts_job_and_starts_thread(monkeypatch: pytest.MonkeyPatch):
    from app import main

    FakeThread.instances = []
    monkeypatch.setattr(main, "Thread", FakeThread)
    monkeypatch.setattr(main, "uuid4", lambda: UUID("11111111-1111-1111-1111-111111111111"))

    insert_calls = []

    def fake_insert_job_invoices(cur, job_id, invoices, batches, chunk_size=5000):
        insert_calls.append(
            {
                "job_id": job_id,
                "invoices": invoices,
                "batches": batches,
                "chunk_size": chunk_size,
            }
        )

    monkeypatch.setattr(main, "_insert_job_invoices", fake_insert_job_invoices)

    cursor = FakeCursor()
    conn = FakeConn(cursor)
    db = FakeDB(conn)
    request = build_request(db)
    payload = JobCreateRequest(
        phrases=["A", "B"],
        batches=3,
        invoices=["100", "200", "300", "400"],
    )

    response = create_job(payload, request)

    assert response.job_id == "11111111-1111-1111-1111-111111111111"
    assert conn.commit_count == 1
    assert conn.rollback_count == 0
    assert len(cursor.executed) == 1
    assert cursor.executed[0][0] == INSERT_JOB_SQL
    assert insert_calls == [
        {
            "job_id": "11111111-1111-1111-1111-111111111111",
            "invoices": ["100", "200", "300", "400"],
            "batches": 3,
            "chunk_size": 5000,
        }
    ]

    assert len(FakeThread.instances) == 1
    thread = FakeThread.instances[0]
    assert thread.daemon is True
    assert thread.started is True
    assert thread.args[0] == "11111111-1111-1111-1111-111111111111"


def test_create_job_returns_503_on_transient_db_error():
    transient_error = OperationalError("could not receive data from server: Operation timed out")
    cursor = FakeCursor(execute_exception=transient_error)
    conn = FakeConn(cursor, rollback_exception=InterfaceError("connection already closed"))
    db = FakeDB(conn)
    request = build_request(db)
    payload = JobCreateRequest(
        phrases=["A"],
        batches=1,
        invoices=["100"],
    )

    with pytest.raises(HTTPException) as exc:
        create_job(payload, request)

    assert exc.value.status_code == 503
    assert exc.value.detail == "database_unavailable"
    assert db.safe_rollback_count == 1


def test_create_job_rejects_batches_above_configured_cap(monkeypatch: pytest.MonkeyPatch):
    from app import main

    monkeypatch.setattr(main, "Thread", FakeThread)
    cursor = FakeCursor()
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn), max_batches=8)

    payload = JobCreateRequest(
        phrases=["A"],
        batches=9,
        invoices=["100"],
    )

    with pytest.raises(HTTPException) as exc:
        create_job(payload, request)

    assert exc.value.status_code == 422
    assert "batches must be <=" in exc.value.detail


def test_cancel_job_changes_running_job_to_canceled():
    cursor = FakeCursor(fetchone_values=[{"job_id": "job-1", "status": "running"}])
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn))

    response = cancel_job("job-1", request)

    assert response.job_id == "job-1"
    assert response.status == "canceled"
    assert conn.commit_count == 1
    assert cursor.executed[0][0] == SELECT_JOB_STATUS_SQL
    assert cursor.executed[1][0] == CANCEL_JOB_SQL
    assert cursor.executed[2][0] == CANCEL_JOB_INVOICES_SQL


def test_cancel_job_is_idempotent_for_finished_job():
    cursor = FakeCursor(fetchone_values=[{"job_id": "job-1", "status": "finished"}])
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn))

    response = cancel_job("job-1", request)

    assert response.status == "finished"
    assert conn.commit_count == 1
    assert len(cursor.executed) == 1


def test_cancel_job_returns_503_on_transient_db_error():
    transient_error = OperationalError("could not receive data from server: Operation timed out")
    cursor = FakeCursor(execute_exception=transient_error)
    conn = FakeConn(cursor, rollback_exception=InterfaceError("connection already closed"))
    db = FakeDB(conn)
    request = build_request(db)

    with pytest.raises(HTTPException) as exc:
        cancel_job("job-1", request)

    assert exc.value.status_code == 503
    assert exc.value.detail == "database_unavailable"
    assert db.safe_rollback_count == 1


def test_get_job_returns_status_and_counters():
    now = datetime.now(timezone.utc)
    cursor = FakeCursor(
        fetchone_values=[
            {
                "job_id": "job-1",
                "status": "running",
                "created_at": now,
                "started_at": now,
                "finished_at": None,
                "total_invoices": 10,
                "queued": 3,
                "running": 2,
                "finished": 4,
                "error": 1,
                "canceled": 0,
            }
        ]
    )
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn))

    data = get_job("job-1", request)

    assert cursor.executed[0][0] == SELECT_JOB_WITH_COUNTERS_SQL
    assert data["job_id"] == "job-1"
    assert data["status"] == "running"
    assert data["total"] == 10
    assert data["queued"] == 3
    assert data["running"] == 2
    assert data["finished"] == 4
    assert data["error"] == 1
    assert data["canceled"] == 0


def test_get_job_not_found():
    cursor = FakeCursor(fetchone_values=[None])
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn))

    with pytest.raises(HTTPException) as exc:
        get_job("missing", request)
    assert exc.value.status_code == 404
    assert exc.value.detail == "job_not_found"


def test_get_job_results_returns_409_while_running():
    cursor = FakeCursor(fetchone_values=[{"job_id": "job-1", "status": "running"}])
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn))

    with pytest.raises(HTTPException) as exc:
        get_job_results("job-1", request)

    assert cursor.executed[0][0] == SELECT_JOB_STATUS_SQL
    assert exc.value.status_code == 409
    assert exc.value.detail == "job_not_finished"


def test_get_job_results_returns_rows_when_canceled():
    now = datetime.now(timezone.utc)
    cursor = FakeCursor(
        fetchone_values=[{"job_id": "job-1", "status": "canceled"}],
        fetchall_values=[
            [
                {
                    "invoice_id": "100",
                    "batch_id": 0,
                    "status": "finished",
                    "found": True,
                    "result_label": "notificado",
                    "error_code": None,
                    "attempts": 1,
                    "matched_phrases": ["token"],
                    "pdf_url": "https://pdf",
                    "last_error": None,
                    "updated_at": now,
                },
                {
                    "invoice_id": "200",
                    "batch_id": 1,
                    "status": "canceled",
                    "found": None,
                    "result_label": "cancelado",
                    "error_code": None,
                    "attempts": 1,
                    "matched_phrases": None,
                    "pdf_url": None,
                    "last_error": "canceled_by_user",
                    "updated_at": now,
                },
            ]
        ],
    )
    conn = FakeConn(cursor)
    request = build_request(FakeDB(conn))

    response = get_job_results("job-1", request)

    assert cursor.executed[0][0] == SELECT_JOB_STATUS_SQL
    assert cursor.executed[1][0] == SELECT_JOB_RESULTS_SQL
    assert response["job_id"] == "job-1"
    assert response["status"] == "canceled"
    assert len(response["results"]) == 2
    assert response["results"][0]["matched_phrases"] == ["token"]
    assert response["results"][1]["last_error"] == "canceled_by_user"
