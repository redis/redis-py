import contextlib
import multiprocessing

import pytest
import redis
from redis.connection import Connection, ConnectionPool
from redis.exceptions import ConnectionError

from .conftest import _get_client


@contextlib.contextmanager
def exit_callback(callback, *args):
    try:
        yield
    finally:
        callback(*args)


class TestMultiprocessing:
    # On macOS and newly non-macOS POSIX systems (since Python 3.14),
    # the default method has been changed to forkserver.
    # The code in this module does not work with it,
    # hence the explicit change to 'fork'
    # See https://github.com/python/cpython/issues/125714
    if multiprocessing.get_start_method() in ["forkserver", "spawn"]:
        _mp_context = multiprocessing.get_context(method="fork")
    else:
        _mp_context = multiprocessing.get_context()

    # Test connection sharing between forks.
    # See issue #1085 for details.

    # use a multi-connection client as that's the only type that is
    # actually fork/process-safe
    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request=request, single_connection_client=False)

    def test_close_connection_in_child(self, master_host):
        """
        A connection owned by a parent and closed by a child doesn't
        destroy the file descriptors so a parent can still use it.
        """
        conn = Connection(host=master_host[0], port=master_host[1])
        conn.send_command("ping")
        assert conn.read_response() == b"PONG"

        def target(conn):
            conn.send_command("ping")
            assert conn.read_response() == b"PONG"
            conn.disconnect()

        proc = self._mp_context.Process(target=target, args=(conn,))
        proc.start()
        proc.join(3)
        assert proc.exitcode == 0

        # The connection was created in the parent but disconnected in the
        # child. The child called socket.close() but did not call
        # socket.shutdown() because it wasn't the "owning" process.
        # Therefore the connection still works in the parent.
        conn.send_command("ping")
        assert conn.read_response() == b"PONG"

    def test_close_connection_in_parent(self, master_host):
        """
        A connection owned by a parent is unusable by a child if the parent
        (the owning process) closes the connection.
        """
        conn = Connection(host=master_host[0], port=master_host[1])
        conn.send_command("ping")
        assert conn.read_response() == b"PONG"

        def target(conn, ev):
            ev.wait()
            # the parent closed the connection. because it also created the
            # connection, the connection is shutdown and the child
            # cannot use it.
            with pytest.raises(ConnectionError):
                conn.send_command("ping")

        ev = multiprocessing.Event()
        proc = self._mp_context.Process(target=target, args=(conn, ev))
        proc.start()

        conn.disconnect()
        ev.set()

        proc.join(3)
        assert proc.exitcode == 0

    @pytest.mark.parametrize("max_connections", [2, None])
    def test_release_parent_connection_from_pool_in_child_process(
        self, max_connections, master_host
    ):
        """
        A connection owned by a parent should not decrease the _created_connections
        counter in child when released - when the child process starts to use the
        pool it resets all the counters that have been set in the parent process.
        """

        pool = ConnectionPool.from_url(
            f"redis://{master_host[0]}:{master_host[1]}",
            max_connections=max_connections,
        )

        parent_conn = pool.get_connection()

        def target(pool, parent_conn):
            with exit_callback(pool.disconnect):
                child_conn = pool.get_connection()
                assert child_conn.pid != parent_conn.pid
                pool.release(child_conn)
                assert pool._created_connections == 1
                assert child_conn in pool._available_connections
                pool.release(parent_conn)
                assert pool._created_connections == 1
                assert child_conn in pool._available_connections
                assert parent_conn not in pool._available_connections

        proc = self._mp_context.Process(target=target, args=(pool, parent_conn))
        proc.start()
        proc.join(3)
        assert proc.exitcode == 0

    @pytest.mark.parametrize("max_connections", [1, 2, None])
    def test_pool(self, max_connections, master_host):
        """
        A child will create its own connections when using a pool created
        by a parent.
        """
        pool = ConnectionPool.from_url(
            f"redis://{master_host[0]}:{master_host[1]}",
            max_connections=max_connections,
        )

        conn = pool.get_connection()
        main_conn_pid = conn.pid
        with exit_callback(pool.release, conn):
            conn.send_command("ping")
            assert conn.read_response() == b"PONG"

        def target(pool):
            with exit_callback(pool.disconnect):
                conn = pool.get_connection()
                assert conn.pid != main_conn_pid
                with exit_callback(pool.release, conn):
                    assert conn.send_command("ping") is None
                    assert conn.read_response() == b"PONG"

        proc = self._mp_context.Process(target=target, args=(pool,))
        proc.start()
        proc.join(3)
        assert proc.exitcode == 0

        # Check that connection is still alive after fork process has exited
        # and disconnected the connections in its pool
        conn = pool.get_connection()
        with exit_callback(pool.release, conn):
            assert conn.send_command("ping") is None
            assert conn.read_response() == b"PONG"

    @pytest.mark.parametrize("max_connections", [1, 2, None])
    def test_close_pool_in_main(self, max_connections, master_host):
        """
        A child process that uses the same pool as its parent isn't affected
        when the parent disconnects all connections within the pool.
        """
        pool = ConnectionPool.from_url(
            f"redis://{master_host[0]}:{master_host[1]}",
            max_connections=max_connections,
        )

        conn = pool.get_connection()
        assert conn.send_command("ping") is None
        assert conn.read_response() == b"PONG"

        def target(pool, disconnect_event):
            conn = pool.get_connection()
            with exit_callback(pool.release, conn):
                assert conn.send_command("ping") is None
                assert conn.read_response() == b"PONG"
                disconnect_event.wait()
                assert conn.send_command("ping") is None
                assert conn.read_response() == b"PONG"

        ev = multiprocessing.Event()

        proc = self._mp_context.Process(target=target, args=(pool, ev))
        proc.start()

        pool.disconnect()
        ev.set()
        proc.join(3)
        assert proc.exitcode == 0

    def test_redis_client(self, r):
        "A redis client created in a parent can also be used in a child"
        assert r.ping() is True

        def target(client):
            assert client.ping() is True
            del client

        proc = self._mp_context.Process(target=target, args=(r,))
        proc.start()
        proc.join(3)
        assert proc.exitcode == 0

        assert r.ping() is True
