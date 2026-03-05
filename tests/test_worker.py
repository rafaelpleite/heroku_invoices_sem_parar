from contextlib import contextmanager

from psycopg2.extras import Json

from app import worker


class FakeCursor:
    def __init__(self, fetchone_value):
        self.fetchone_value = fetchone_value
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self.fetchone_value


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
        self.safe_rollback_count = 0

    @contextmanager
    def get_conn(self):
        yield self._conn

    def safe_rollback(self, conn):
        self.safe_rollback_count += 1
        conn.rollback()


def _build_result(matched_phrases):
    return {
        "status": "finished",
        "found": True,
        "result_label": "notificado",
        "error_code": None,
        "attempts": 1,
        "matched_phrases": matched_phrases,
        "pdf_url": "https://example.com/invoice.pdf",
        "last_error": None,
    }


def test_update_invoice_result_adapts_matched_phrases_as_json() -> None:
    cursor = FakeCursor(fetchone_value={"id": 1})
    conn = FakeConn(cursor)
    db = FakeDB(conn)

    updated = worker._update_invoice_result(
        invoice_row_id=123,
        result=_build_result(["a", "b"]),
        db=db,
    )

    assert updated is True
    assert conn.commit_count == 1
    assert db.safe_rollback_count == 0

    _, params = cursor.executed[0]
    assert isinstance(params[5], Json)
    assert params[5].adapted == ["a", "b"]


def test_update_invoice_result_preserves_null_matched_phrases() -> None:
    cursor = FakeCursor(fetchone_value={"id": 1})
    conn = FakeConn(cursor)
    db = FakeDB(conn)

    updated = worker._update_invoice_result(
        invoice_row_id=456,
        result=_build_result(None),
        db=db,
    )

    assert updated is True

    _, params = cursor.executed[0]
    assert params[5] is None

