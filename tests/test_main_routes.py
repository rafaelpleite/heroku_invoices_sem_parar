from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.config import Settings
from app.main import JobCreateRequest, create_job, get_job, get_job_results
from app.sql import (
    INSERT_JOB_INVOICE_SQL,
    INSERT_JOB_SQL,
    SELECT_JOB_RESULTS_SQL,
    SELECT_JOB_STATUS_SQL,
    SELECT_JOB_WITH_COUNTERS_SQL,
)


class FakeCursor:
    def __init__(self, fetchone_values=None, fetchall_values=None):
        self.fetchone_values = list(fetchone_values or [])
        self.fetchall_values = list(fetchall_values or [])
        self.executed = []
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def executemany(self, sql, seq):
        self.executemany_calls.append((sql, list(seq)))

    def fetchone(self):
        return self.fetchone_values.pop(0) if self.fetchone_values else None

    def fetchall(self):
        return self.fetchall_values.pop(0) if self.fetchall_values else []


class FakeConn:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self, cursor_factory=None):
        return self._cursor

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1


class FakeDB:
    def __init__(self, conn: FakeConn):
        self._conn = conn

    @contextmanager
    def get_conn(self):
        yield self._conn


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


def build_request(fake_db: FakeDB) -> SimpleNamespace:
    settings = Settings(
        database_url="postgresql://x",
        heroku_api_key="token",
        invoice_api_base_url="https://base/",
        log_level="INFO",
        request_timeout_seconds=20,
        db_pool_minconn=1,
        db_pool_maxconn=20,
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


def test_create_job_inserts_rows_splits_batches_and_starts_thread(monkeypatch: pytest.MonkeyPatch):
    from app import main

    FakeThread.instances = []
    monkeypatch.setattr(main, "Thread", FakeThread)
    monkeypatch.setattr(main, "uuid4", lambda: UUID("11111111-1111-1111-1111-111111111111"))

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
    assert cursor.executed[0][1][0] == "11111111-1111-1111-1111-111111111111"
    assert cursor.executed[0][1][1] == 3
    assert cursor.executed[0][1][3] == 4

    assert len(cursor.executemany_calls) == 1
    sql, rows = cursor.executemany_calls[0]
    assert sql == INSERT_JOB_INVOICE_SQL
    assert rows == [
        ("11111111-1111-1111-1111-111111111111", "100", 0),
        ("11111111-1111-1111-1111-111111111111", "200", 1),
        ("11111111-1111-1111-1111-111111111111", "300", 2),
        ("11111111-1111-1111-1111-111111111111", "400", 0),
    ]

    assert len(FakeThread.instances) == 1
    thread = FakeThread.instances[0]
    assert thread.daemon is True
    assert thread.started is True
    assert thread.args[0] == "11111111-1111-1111-1111-111111111111"


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


def test_get_job_results_returns_rows_when_finished():
    now = datetime.now(timezone.utc)
    cursor = FakeCursor(
        fetchone_values=[{"job_id": "job-1", "status": "finished"}],
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
                    "updated_at": now,
                },
                {
                    "invoice_id": "200",
                    "batch_id": 1,
                    "status": "error",
                    "found": None,
                    "result_label": "erro_401",
                    "error_code": 401,
                    "attempts": 3,
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
    assert response["status"] == "finished"
    assert len(response["results"]) == 2
    assert response["results"][0]["result_label"] == "notificado"
    assert response["results"][1]["result_label"] == "erro_401"

