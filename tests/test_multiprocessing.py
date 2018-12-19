import pytest
import multiprocessing
import contextlib

from redis.connection import Connection, ConnectionPool


@contextlib.contextmanager
def exit_callback(callback, *args):
    try:
        yield
    finally:
        callback(*args)


class TestMultiprocessing(object):
    # Test connection sharing between forks.
    # See issue #1085 for details.

    def test_connection(self):
        conn = Connection()
        assert conn.send_command('ping') is None
        assert conn.read_response() == b'PONG'

        def target(conn):
            assert conn.send_command('ping') is None
            assert conn.read_response() == b'PONG'
            conn.disconnect()

        proc = multiprocessing.Process(target=target, args=(conn,))
        proc.start()
        proc.join(3)
        assert proc.exitcode is 0

        # Check that connection is still alive after fork process has exited.
        conn.send_command('ping')
        assert conn.read_response() == b'PONG'

    def test_close_connection_in_main(self):
        conn = Connection()
        assert conn.send_command('ping') is None
        assert conn.read_response() == b'PONG'

        def target(conn, ev):
            ev.wait()
            assert conn.send_command('ping') is None
            assert conn.read_response() == b'PONG'

        ev = multiprocessing.Event()
        proc = multiprocessing.Process(target=target, args=(conn, ev))
        proc.start()

        conn.disconnect()
        ev.set()

        proc.join(3)
        assert proc.exitcode is 0

    @pytest.mark.parametrize('max_connections', [1, 2, None])
    def test_pool(self, max_connections):
        pool = ConnectionPool.from_url('redis://localhost',
                                       max_connections=max_connections)

        conn = pool.get_connection('ping')
        with exit_callback(pool.release, conn):
            assert conn.send_command('ping') is None
            assert conn.read_response() == b'PONG'

        def target(pool):
            with exit_callback(pool.disconnect):
                conn = pool.get_connection('ping')
                with exit_callback(pool.release, conn):
                    assert conn.send_command('ping') is None
                    assert conn.read_response() == b'PONG'

        proc = multiprocessing.Process(target=target, args=(pool,))
        proc.start()
        proc.join(3)
        assert proc.exitcode is 0

        # Check that connection is still alive after fork process has exited.
        conn = pool.get_connection('ping')
        with exit_callback(pool.release, conn):
            conn.send_command('ping')
            assert conn.read_response() == b'PONG'

    @pytest.mark.parametrize('max_connections', [1, 2, None])
    def test_close_pool_in_main(self, max_connections):
        pool = ConnectionPool.from_url('redis://localhost',
                                       max_connections=max_connections)

        conn = pool.get_connection('ping')
        assert conn.send_command('ping') is None
        assert conn.read_response() == b'PONG'

        def target(pool, disconnect_event):
            conn = pool.get_connection('ping')
            with exit_callback(pool.release, conn):
                assert conn.send_command('ping') is None
                assert conn.read_response() == b'PONG'
                disconnect_event.wait()
                assert conn.send_command('ping') is None
                assert conn.read_response() == b'PONG'

        ev = multiprocessing.Event()

        proc = multiprocessing.Process(target=target, args=(pool, ev))
        proc.start()

        pool.disconnect()
        ev.set()
        proc.join(3)
        assert proc.exitcode is 0

    def test_redis(self, r):
        assert r.ping() is True

        def target(redis):
            assert redis.ping() is True
            del redis

        proc = multiprocessing.Process(target=target, args=(r,))
        proc.start()
        proc.join(3)
        assert proc.exitcode is 0

        assert r.ping() is True
