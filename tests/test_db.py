from app.db import Database
from psycopg2 import InterfaceError


class DummyConn:
    def __init__(self, closed: int = 0, rollback_exception: Exception | None = None):
        self.closed = closed
        self.rollback_called = 0
        self.rollback_exception = rollback_exception

    def rollback(self):
        self.rollback_called += 1
        if self.rollback_exception is not None:
            raise self.rollback_exception


class DummyPool:
    def __init__(self, conn: DummyConn):
        self.conn = conn
        self.putconn_calls: list[dict] = []

    def getconn(self):
        return self.conn

    def putconn(self, conn, close=False):
        self.putconn_calls.append({"conn": conn, "close": close})


def test_safe_rollback_calls_rollback_for_open_connection() -> None:
    db = Database(dsn="postgresql://example")
    conn = DummyConn()
    db.safe_rollback(conn)
    assert conn.rollback_called == 1


def test_safe_rollback_swallows_interface_error_for_closed_connection() -> None:
    db = Database(dsn="postgresql://example")
    conn = DummyConn(rollback_exception=InterfaceError("connection already closed"))
    db.safe_rollback(conn)
    assert conn.rollback_called == 1


def test_get_conn_returns_dead_connection_to_pool_as_closed() -> None:
    db = Database(dsn="postgresql://example")
    conn = DummyConn(closed=1)
    pool = DummyPool(conn)
    db._pool = pool

    with db.get_conn() as acquired:
        assert acquired is conn

    assert pool.putconn_calls == [{"conn": conn, "close": True}]


def test_get_conn_returns_live_connection_to_pool_without_force_close() -> None:
    db = Database(dsn="postgresql://example")
    conn = DummyConn(closed=0)
    pool = DummyPool(conn)
    db._pool = pool

    with db.get_conn() as acquired:
        assert acquired is conn

    assert pool.putconn_calls == [{"conn": conn, "close": False}]

