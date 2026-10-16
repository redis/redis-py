import threading
import uuid
from unittest.mock import patch

import pytest

from redis._parsers import _HiredisParser, _RESP3Parser
from redis.cache import CacheConfig, TrackingMode
from redis.connection import BlockingConnectionPool, ConnectionPool
from redis.maint_notifications import MaintNotificationsConfig
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt


@pytest.mark.fixed_client
@pytest.mark.onlynoncluster
@skip_if_server_version_lt("7.4.0")
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize(
    "parser_class",
    [
        _RESP3Parser,
        pytest.param(
            _HiredisParser,
            marks=pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="requires hiredis"),
        ),
    ],
)
@pytest.mark.parametrize("tracking_mode", list(TrackingMode))
class TestCachePoolOwnership:
    @pytest.fixture
    def pool(self, request, pool_class, parser_class, tracking_mode):
        config = CacheConfig(
            tracking_mode=tracking_mode,
            cache_predicate=(
                None if tracking_mode is TrackingMode.PLAIN else lambda *_: True
            ),
        )
        pool = pool_class.from_url(
            request.config.getoption("--redis-url"),
            protocol=3,
            parser_class=parser_class,
            cache_config=config,
            max_connections=4,
            socket_timeout=1,
            maint_notifications_config=MaintNotificationsConfig(enabled=False),
        )
        yield pool
        pool.disconnect()

    @pytest.mark.parametrize("invalidate", [False, True])
    def test_busy_cache_owner_keeps_its_command_reply(self, pool, r2, invalidate):
        """A valid cached value does not make its owner's socket idle."""
        foo, bar = f"{uuid.uuid4()}:foo", f"{uuid.uuid4()}:bar"
        r2.mset({foo: "old", bar: "B"})
        owner = pool.get_connection()
        borrower = pool.get_connection()
        try:
            owner.send_command("GET", foo, keys=[foo])
            assert owner.read_response() == b"old"

            # Leave a different command's reply on the checked-out owner's socket.
            owner.send_command("GET", bar, keys=[bar])
            assert owner.can_read(timeout=1)
            if invalidate:
                r2.set(foo, "new")

            borrower.send_command("GET", foo, keys=[foo])
            assert borrower.read_response() == (b"new" if invalidate else b"old")
            assert owner.read_response() == b"B"
        finally:
            pool.release(owner)
            pool.release(borrower)

    def test_idle_cache_owner_still_serves_hits_and_invalidations(self, pool, r2):
        key = str(uuid.uuid4())
        r2.set(key, "old")
        owner = pool.get_connection()
        borrower = pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"old"
        pool.release(owner)

        try:
            with patch.object(
                borrower._conn,
                "send_packed_command",
                wraps=borrower._conn.send_packed_command,
            ) as send:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"old"
                send.assert_not_called()

                r2.set(key, "new")
                assert owner.can_read(timeout=1)
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"new"
                send.assert_called_once()
        finally:
            pool.release(borrower)

    @pytest.mark.parametrize("shared_cache", [False, True])
    def test_checkout_waits_for_an_idle_owner_drain(self, pool, r2, shared_cache):
        """Queue checkout must join the pool lock before touching the socket."""
        key = str(uuid.uuid4())
        r2.set(key, "value")
        borrower_pool = pool
        if shared_cache:
            other_pool_class = (
                ConnectionPool
                if isinstance(pool, BlockingConnectionPool)
                else BlockingConnectionPool
            )
            borrower_pool = other_pool_class(cache=pool.cache, **pool.connection_kwargs)
        owner = pool.get_connection()
        borrower = borrower_pool.get_connection()
        owner.send_command("GET", key, keys=[key])
        assert owner.read_response() == b"value"
        pool.release(owner)

        draining = threading.Event()
        finish_drain = threading.Event()
        checkout_attempted = threading.Event()
        errors = []
        real_can_read = owner._conn.can_read
        real_ensure = pool.ensure_connection
        real_pool_lock = pool._lock

        class ObservedPoolLock:
            def __enter__(self):
                if threading.current_thread() is checkout_thread:
                    checkout_attempted.set()
                return real_pool_lock.__enter__()

            def __exit__(self, *args):
                return real_pool_lock.__exit__(*args)

        def pause_drain(*args, **kwargs):
            if threading.current_thread() is reader:
                draining.set()
                assert finish_drain.wait(3)
            return real_can_read(*args, **kwargs)

        def ensure_connection(connection):
            overlaps_drain = not finish_drain.is_set()
            checkout_attempted.set()
            assert not overlaps_drain, "checkout used a socket while it was drained"
            return real_ensure(connection)

        def read_cached():
            try:
                borrower.send_command("GET", key, keys=[key])
                assert borrower.read_response() == b"value"
            except BaseException as exc:
                errors.append(exc)

        def checkout():
            try:
                connection = pool.get_connection()
                pool.release(connection)
            except BaseException as exc:
                errors.append(exc)

        reader = threading.Thread(target=read_cached, daemon=True)
        checkout_thread = threading.Thread(target=checkout, daemon=True)
        try:
            with (
                patch.object(owner._conn, "can_read", side_effect=pause_drain),
                patch.object(pool, "ensure_connection", side_effect=ensure_connection),
                patch.object(pool, "_lock", ObservedPoolLock()),
            ):
                reader.start()
                assert draining.wait(3)
                checkout_thread.start()
                assert checkout_attempted.wait(3)
                finish_drain.set()
                reader.join(3)
                checkout_thread.join(3)
                assert not reader.is_alive()
                assert not checkout_thread.is_alive()
                assert not errors
        finally:
            finish_drain.set()
            reader.join(3)
            if checkout_thread.ident is not None:
                checkout_thread.join(3)
            borrower_pool.release(borrower)
            if shared_cache:
                borrower_pool.disconnect()
