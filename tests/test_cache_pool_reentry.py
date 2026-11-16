import gc
import multiprocessing
import socket
import threading
import uuid
import weakref
from itertools import chain
from unittest.mock import Mock, patch

import pytest

from redis import Redis
from redis._parsers import _HiredisParser, _RESP3Parser
from redis.cache import CacheConfig
from redis.cluster import ClusterNode, NodesManager
from redis.connection import BlockingConnectionPool, ConnectionPool
from redis.event import AfterConnectionReleasedEvent
from redis.exceptions import AuthenticationError
from redis.maint_notifications import (
    MaintNotificationsConfig,
    MaintNotificationsConnectionHandler,
    MaintNotificationsPoolHandler,
    NodeMovingNotification,
    OSSMaintNotificationsHandler,
    OSSNodeMigratedNotification,
)
from redis.multidb.event import (
    ActiveDatabaseChanged,
    CloseConnectionOnActiveDatabaseChanged,
)
from redis.sentinel import SentinelConnectionPool
from redis.utils import HIREDIS_AVAILABLE

from .conftest import skip_if_server_version_lt

pytestmark = [
    pytest.mark.fixed_client,
    pytest.mark.onlynoncluster,
    skip_if_server_version_lt("7.4.0"),
]

PARSERS = [
    _RESP3Parser,
    pytest.param(
        _HiredisParser,
        marks=pytest.mark.skipif(not HIREDIS_AVAILABLE, reason="requires hiredis"),
    ),
]


@pytest.mark.parametrize("parser_class", PARSERS)
def test_moving_retires_connection_published_by_blocking_release(request, parser_class):
    pool = BlockingConnectionPool.from_url(
        request.config.getoption("--redis-url"),
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=1,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled="auto"),
    )
    owner = pool.get_connection()
    owner.send_command("CLIENT", "ID")
    previous_id = owner.read_response()
    published, finish_release = threading.Event(), threading.Event()
    original_put = pool.pool.put_nowait
    errors = []

    def put(connection):
        original_put(connection)
        published.set()
        assert finish_release.wait(5), "MOVING handler did not finish"

    def release():
        try:
            pool.release(owner)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=release, daemon=True)
    handler = pool._maint_notifications_pool_handler
    moved_host = "127.0.0.1" if owner.host == "localhost" else "localhost"
    checked_out = None
    try:
        with patch.object(pool.pool, "put_nowait", put):
            worker.start()
            assert published.wait(3)
            assert owner._in_use_lock.locked() and not owner.should_reconnect()
            with patch.object(handler, "_schedule"):
                handler.handle_notification(
                    NodeMovingNotification(1, moved_host, owner.port, 60)
                )
            assert owner.host == moved_host
            assert owner.is_connected, "MOVING disconnected the releasing owner"
            finish_release.set()
            worker.join(3)
            assert not worker.is_alive() and not errors, errors
        checked_out = pool.get_connection()
        checked_out.send_command("CLIENT", "ID")
        assert checked_out.read_response() != previous_id
        checked_out.send_command("PING")
        assert checked_out.read_response() == b"PONG"
    finally:
        finish_release.set()
        worker.join(5)
        if checked_out is not None:
            pool.release(checked_out)
        handler.cancel_scheduled_tasks()
        pool.close()


def _run_bounded(target, args):
    worker = multiprocessing.get_context("spawn").Process(target=target, args=args)
    worker.start()
    try:
        worker.join(10)
        assert not worker.is_alive(), "cached pool shutdown deadlocked"
        assert worker.exitcode == 0
    finally:
        if worker.is_alive():
            worker.terminate()
        worker.join(5)
        worker.close()


def _moving_during_idle_auth(url, parser_class, outcome):
    config = MaintNotificationsConfig(enabled="auto")
    pool = ConnectionPool.from_url(
        url,
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=2,
        socket_timeout=1,
        maint_notifications_config=config,
    )
    auth, owner = (pool.get_connection() for _ in range(2))
    peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
    borrower = peers.get_connection()
    writer = Redis.from_url(url)
    key = str(uuid.uuid4())
    writer.set(key, "old")
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old"
    writer.set(key, "new")
    pool.release(auth)
    pool.release(owner)
    handler = MaintNotificationsPoolHandler(pool, config)
    pool._maint_notifications_pool_handler = handler
    servers = []
    for connection in (auth, owner):
        connection._conn.disconnect()
        client, server = socket.socketpair()
        server.settimeout(3)
        servers.append(server)
        connection._conn._sock = client
        connection._conn._parser.on_connect(connection._conn)
        connection._conn._parser.set_node_moving_push_handler(
            handler.handle_notification
        )
    moving = b">4\r\n+MOVING\r\n:%d\r\n:60\r\n_\r\n"
    servers[1].sendall(moving % 201)
    peer_moving, authenticating = threading.Event(), threading.Event()
    errors = []
    original_dispatch = handler._dispatch_handoff_event
    original_acquire = auth._try_acquire_idle
    original_read = auth.read_response

    class Cancelled(BaseException):
        pass

    def dispatch(*args):
        if threading.current_thread() is reader:
            # MOVING owns the handler lock, before asking for the pool lock.
            peer_moving.set()
            assert authenticating.wait(3)
        return original_dispatch(*args)

    def acquire():
        result = original_acquire()
        assert result
        authenticating.set()
        return result

    def read(*args, **kwargs):
        result = original_read(*args, **kwargs)
        if outcome == "cancel":
            raise Cancelled
        return result

    token = Mock()
    token.try_get.return_value = "default"
    token.get_value.return_value = "test-token"
    command = b"".join(auth._conn.pack_command("AUTH", "default", "test-token"))

    def respond():
        try:
            received = b""
            while len(received) < len(command):
                part = servers[0].recv(len(command) - len(received))
                assert part, "AUTH closed before sending the command"
                received += part
            assert received == command
            servers[0].sendall(
                moving % 202
                + (
                    b"-WRONGPASS invalid token\r\n"
                    if outcome == "auth_error"
                    else b"+OK\r\n"
                )
            )
        except BaseException as error:
            errors.append(error)

    def borrow():
        try:
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"new"
        except BaseException as error:
            errors.append(error)

    reader = threading.Thread(target=borrow, daemon=True)
    server = threading.Thread(target=respond, daemon=True)
    try:
        with (
            patch.object(handler, "_dispatch_handoff_event", dispatch),
            patch.object(handler, "_schedule"),
            patch.object(auth, "_try_acquire_idle", acquire),
            patch.object(auth, "read_response", read),
        ):
            reader.start()
            assert peer_moving.wait(3), "peer did not start MOVING processing"
            server.start()
            if outcome == "success":
                pool.re_auth_callback(token)
            else:
                with pytest.raises(
                    AuthenticationError if outcome == "auth_error" else Cancelled
                ):
                    pool.re_auth_callback(token)
            reader.join(3)
            server.join(3)
            assert not reader.is_alive() and not server.is_alive()
            assert not errors, errors
            assert {n.id for n in handler._processed_notifications} == {201, 202}
            for connection in (auth, owner):
                assert not connection._idle_maintenance
                assert not connection._in_use_lock.locked()
                assert (
                    connection._conn._parser.node_moving_push_handler_func
                    == handler.handle_notification
                )
        # The socket-pair credentials are not credentials for the real server.
        for connection in (auth, owner):
            connection.set_re_auth_token(None)
        pool.disconnect()
        with Redis(connection_pool=pool) as client:
            assert client.get(key) == b"new"
            assert client.ping()
    finally:
        authenticating.set()
        for server_socket in servers:
            server_socket.close()
        reader.join(3)
        if server.ident is not None:
            server.join(3)
        handler.cancel_scheduled_tasks()
        peers.release(borrower)
        peers.close()
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("outcome", ["success", "auth_error", "cancel"])
def test_idle_auth_and_peer_moving_release_pool_locks(request, parser_class, outcome):
    _run_bounded(
        _moving_during_idle_auth,
        (request.config.getoption("--redis-url"), parser_class, outcome),
    )


def _finalizer_during_shutdown(url, pool_class, parser_class, automatic, operation):
    pool = pool_class.from_url(
        url,
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(),
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    owner = pool.get_connection()
    if operation != "release":
        pool.release(owner)
    enabled, thresholds = gc.isenabled(), gc.get_threshold()
    gc.disable()
    victim = Redis.from_pool(pool)
    victim.reference_cycle = victim
    abandoned = weakref.ref(victim)
    del victim
    original = owner._conn.disconnect
    collected = False

    def disconnect(*args, **kwargs):
        nonlocal collected
        if not collected:
            collected = True
            if automatic:
                gc.set_threshold(1, 1, 1)
                gc.enable()
                allocations = [[] for _ in range(100)]
                assert allocations
            else:
                gc.collect()
            assert abandoned() is None, "pool-owning client was not finalized"
        return original(*args, **kwargs)

    try:
        with patch.object(owner._conn, "disconnect", disconnect):
            if operation == "release":
                owner.mark_for_reconnect()
                pool.release(owner)
            elif operation == "free_disconnect":
                pool.disconnect_free_connections()
            else:
                pool.close()
        assert collected
        assert not owner.is_connected
        assert not owner._in_use_lock.locked()
        assert not owner._idle_maintenance
        reused = pool.get_connection()
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        gc.set_threshold(*thresholds)
        if enabled:
            gc.enable()
        else:
            gc.disable()
        pool.close()


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("operation", ["close", "release", "free_disconnect"])
def test_finalizer_during_pool_shutdown(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_shutdown,
        (
            request.config.getoption("--redis-url"),
            pool_class,
            parser_class,
            automatic,
            operation,
        ),
    )


def _finalizer_during_release_listener_reconnect(url, parser_class, automatic):
    pool = ConnectionPool.from_url(
        url,
        protocol=3,
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=2,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    owner, released = (pool.get_connection() for _ in range(2))
    # The peer shares the cache, but closing the owner's pool must not close
    # the peer's active socket while it waits for the owner's drain to finish.
    peers = ConnectionPool(cache=pool.cache, **pool.connection_kwargs)
    borrower = peers.get_connection()
    key = str(uuid.uuid4())
    writer = Redis.from_url(url)
    writer.set(key, "old")
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old"
    pool.release(owner)
    draining, finalizing = threading.Event(), threading.Event()
    errors = []
    original_poll = owner._conn.can_read
    original_disconnect = released._conn.disconnect
    collected = False
    enabled, thresholds = gc.isenabled(), gc.get_threshold()
    gc.disable()
    victim = Redis.from_pool(pool)
    victim.reference_cycle = victim
    abandoned = weakref.ref(victim)
    del victim

    def poll(*args, **kwargs):
        if threading.current_thread() is reader:
            draining.set()
            assert finalizing.wait(5), "release did not reach its final disconnect"
        return original_poll(*args, **kwargs)

    def read():
        try:
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"old"
        except BaseException as error:
            errors.append(error)

    def mark_for_reconnect(event):
        if event.connection is released:
            assert not released.should_reconnect()
            released.mark_for_reconnect()

    def disconnect(*args, **kwargs):
        nonlocal collected
        if not collected:
            collected = True
            finalizing.set()
            if automatic:
                gc.set_threshold(1, 1, 1)
                gc.enable()
                allocations = [[] for _ in range(100)]
                assert allocations
            else:
                gc.collect()
            assert abandoned() is None, "pool-owning client was not finalized"
        return original_disconnect(*args, **kwargs)

    listener = Mock()
    listener.listen.side_effect = mark_for_reconnect
    reader = threading.Thread(target=read, daemon=True)
    pool._event_dispatcher.register_listeners(
        {AfterConnectionReleasedEvent: [listener]}
    )
    try:
        with (
            patch.object(owner._conn, "can_read", poll),
            patch.object(released._conn, "disconnect", disconnect),
        ):
            reader.start()
            assert draining.wait(3), "peer did not start draining the idle owner"
            pool.release(released)
            reader.join(3)
            assert not reader.is_alive(), "release and idle drain deadlocked"
            assert not errors, errors
        assert collected
        assert not released.is_connected
        assert not owner._in_use_lock.locked()
        assert not owner._idle_maintenance
        pool._event_dispatcher.unregister_listeners(
            {AfterConnectionReleasedEvent: [listener]}
        )
        reused = pool.get_connection()
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        gc.set_threshold(*thresholds)
        if enabled:
            gc.enable()
        else:
            gc.disable()
        peers.release(borrower)
        peers.close()
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
def test_finalizer_during_release_listener_reconnect(request, parser_class, automatic):
    _run_bounded(
        _finalizer_during_release_listener_reconnect,
        (request.config.getoption("--redis-url"), parser_class, automatic),
    )


def _finalizer_during_pool_lock(
    url, parser_class, automatic, operation, pool_class=ConnectionPool
):
    writer = Redis.from_url(url)
    address = writer.connection_pool.connection_kwargs
    host, port = address.get("host", "localhost"), address.get("port", 6379)
    proactive = operation in (
        "proactive_reconnect",
        "moving_reconnect",
        "scheduled_reconnect",
        "cancel_moving",
    )
    kwargs = dict(
        protocol=3,
        db=address.get("db", 0),
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=3 if operation == "checkout" else 2,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(
            enabled="auto" if proactive else False
        ),
    )
    if operation == "rejected_release":
        manager = Mock()
        manager.discover_master.return_value = host, port
        pool = SentinelConnectionPool("cache-reentry", manager, **kwargs)
    else:
        pool = pool_class(host=host, port=port, **kwargs)
    owner, released = (pool.get_connection() for _ in range(2))
    peers = ConnectionPool(host=host, port=port, cache=pool.cache, **kwargs)
    borrower = peers.get_connection()
    key = str(uuid.uuid4())
    writer.set(key, "old")
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old"
    pool.release(owner)
    if operation in (
        "topology_cleanup",
        "smigrated",
        "multidb",
        "multidb_cluster",
        "proactive_reconnect",
        "moving_reconnect",
        "scheduled_reconnect",
        "cancel_moving",
    ):
        pool.release(released)
    reconnect_handler = None
    if operation == "cancel_moving":
        reconnect_handler = pool._maint_notifications_pool_handler
        with patch.object(reconnect_handler, "_schedule"):
            reconnect_handler.handle_notification(
                NodeMovingNotification(2, None, port, 60)
            )
        assert reconnect_handler._pending_handoffs
    draining, finalizing = threading.Event(), threading.Event()
    errors = []
    original_poll = owner._conn.can_read
    original_disconnect = released._conn.disconnect
    collected = False
    reserved, resume_checkout = threading.Event(), threading.Event()
    checked_out = []
    original_acquire = owner._acquire
    connection_class = pool.connection_class
    original_free = pool._get_free_connections
    original_used = pool._get_in_use_connections
    enabled, thresholds = gc.isenabled(), gc.get_threshold()
    gc.disable()
    victim = Redis.from_pool(pool)
    victim.reference_cycle = victim
    abandoned = weakref.ref(victim)
    del victim

    def poll(*args, **kwargs):
        if threading.current_thread() is reader:
            draining.set()
            assert finalizing.wait(5), "pool operation did not reach collection"
        return original_poll(*args, **kwargs)

    def read():
        try:
            borrower.send_command("GET", key, keys=[key])
            assert borrower.read_response() == b"old"
        except BaseException as error:
            errors.append(error)

    def collect():
        nonlocal collected
        if collected:
            return
        collected = True
        finalizing.set()
        if automatic:
            gc.set_threshold(1, 1, 1)
            gc.enable()
            allocations = [[] for _ in range(100)]
            assert allocations
        else:
            gc.collect()
        assert abandoned() is None, "pool-owning client was not finalized"

    def disconnect(*args, **kwargs):
        if operation in (
            "rejected_release",
            "topology_cleanup",
            "smigrated",
            "multidb",
            "multidb_cluster",
            "proactive_reconnect",
            "moving_reconnect",
            "scheduled_reconnect",
        ):
            collect()
        return original_disconnect(*args, **kwargs)

    def snapshot(*args):
        if operation == "disconnect_snapshot":
            collect()
        return chain(*args)

    def free_connections():
        if operation in ("blocking_snapshot", "blocking_maintenance_snapshot"):
            collect()
        elif operation == "connection_count_free":
            # Snapshot allocation can run GC while holding this pool lock.
            with pool._lock:
                collect()
        return original_free()

    def used_connections():
        if operation in ("connection_count_used", "cancel_moving"):
            with pool._lock:
                collect()
        return original_used()

    def configure(*args, **kwargs):
        if operation == "maintenance_config":
            collect()
        return MaintNotificationsConnectionHandler(*args, **kwargs)

    def create(*args, **kwargs):
        if operation == "checkout":
            collect()
        return connection_class(*args, **kwargs)

    def acquire():
        reserved.set()
        assert resume_checkout.wait(5), "new connection was not created"
        original_acquire()

    def checkout():
        try:
            checked_out.append(pool.get_connection())
        except BaseException as error:
            errors.append(error)

    reader = threading.Thread(target=read, daemon=True)
    waiter = threading.Thread(target=checkout, daemon=True)
    try:
        with (
            patch.object(owner._conn, "can_read", poll),
            patch.object(released._conn, "disconnect", disconnect),
            patch("redis.connection.chain", snapshot),
            patch("redis.connection.MaintNotificationsConnectionHandler", configure),
            patch.object(pool, "connection_class", create),
            patch.object(pool, "_get_free_connections", free_connections),
            patch.object(pool, "_get_in_use_connections", used_connections),
        ):
            reader.start()
            assert draining.wait(3), "peer did not start draining the idle owner"
            if operation == "rejected_release":
                pool.proxy.master_address = ("previous-master", port)
                assert not pool.owns_connection(released)
                pool.release(released)
                assert released not in pool._in_use_connections
                assert released not in pool._available_connections
                assert not released._in_use
                assert pool._created_connections == 1
            elif operation in ("connection_count_free", "connection_count_used"):
                counts = pool.get_connection_count()
                assert [count for count, _ in counts] == [1, 1]
                pool.release(released)
            elif operation == "cancel_moving":
                pool.close()
                assert not reconnect_handler._pending_handoffs
                assert not reconnect_handler._scheduled_timers
                assert pool.connection_kwargs["host"] == host
                assert pool.connection_kwargs["socket_timeout"] == 1
            elif operation == "topology_cleanup":
                NodesManager._free_pool_connections(pool)
            elif operation in ("blocking_snapshot", "blocking_maintenance_snapshot"):
                pool.set_in_maintenance(operation == "blocking_maintenance_snapshot")
                pool.disconnect(inuse_connections=False)
                pool.set_in_maintenance(False)
                pool.release(released)
            elif operation == "maintenance_config":
                pool.update_maint_notifications_config(
                    MaintNotificationsConfig(enabled="auto")
                )
            elif operation == "checkout":
                # Reserve the draining owner without using its socket. The
                # next checkout must allocate a new connection under the lock.
                with patch.object(owner, "_acquire", acquire):
                    waiter.start()
                    assert reserved.wait(3)
                    if isinstance(pool, BlockingConnectionPool):
                        pool.set_in_maintenance(True)
                    fresh = pool.get_connection()
                    if isinstance(pool, BlockingConnectionPool):
                        pool.set_in_maintenance(False)
                    resume_checkout.set()
                    waiter.join(3)
                    assert not waiter.is_alive() and not errors, errors
                for connection in [fresh, *checked_out]:
                    connection.send_command("PING")
                    assert connection.read_response() == b"PONG"
                    pool.release(connection)
                pool.release(released)
            elif operation in ("multidb", "multidb_cluster"):
                client = Redis(connection_pool=pool)
                old_database = Mock(client=client)
                if operation == "multidb_cluster":
                    old_database.client = Mock()
                    old_database.client.nodes_manager.nodes_cache = {
                        "old": ClusterNode(host, port, redis_connection=client)
                    }
                CloseConnectionOnActiveDatabaseChanged().listen(
                    ActiveDatabaseChanged(old_database, Mock(), Mock())
                )
                if isinstance(pool, BlockingConnectionPool):
                    assert not pool._in_maintenance
            elif operation in (
                "proactive_reconnect",
                "moving_reconnect",
                "scheduled_reconnect",
            ):
                reconnect_handler = pool._maint_notifications_pool_handler
                if operation == "moving_reconnect":
                    # Exercise the synchronous reconnect under MOVING's outer
                    # pool lock without leaving delayed timers after the test.
                    with patch.object(reconnect_handler, "_schedule"):
                        reconnect_handler.handle_notification(
                            NodeMovingNotification(1, host, port, 60)
                        )
                elif operation == "scheduled_reconnect":
                    with patch("redis.maint_notifications.threading.Timer") as timer:
                        reconnect_handler._schedule(
                            0, reconnect_handler.run_proactive_reconnect
                        )
                        timer.call_args.args[1]()
                        assert not reconnect_handler._scheduled_timers
                else:
                    reconnect_handler.run_proactive_reconnect()
            elif operation == "smigrated":
                node = ClusterNode(
                    host, port, redis_connection=Redis(connection_pool=pool)
                )
                cluster = Mock()
                cluster.nodes_manager._initialization_lock = threading.RLock()
                cluster.nodes_manager.get_node.return_value = node
                cluster.nodes_manager.nodes_cache = {}
                handler = OSSMaintNotificationsHandler(
                    cluster, MaintNotificationsConfig(enabled=True)
                )
                notification = OSSNodeMigratedNotification(
                    1, {f"{host}:{port}": [{f"{host}:{port + 1}": "0-16383"}]}
                )
                handler.handle_notification(notification)
                assert notification in handler._processed_notifications
                assert not handler._in_progress
            else:
                pool.disconnect()
                pool.release(released)
            reader.join(3)
            assert not reader.is_alive(), "pool shutdown and idle drain deadlocked"
            assert not errors, errors
        if reconnect_handler is not None:
            reconnect_handler.cancel_scheduled_tasks()
        assert collected
        if operation != "checkout":
            assert not owner.is_connected
        assert not released.is_connected
        assert not owner._in_use_lock.locked()
        assert not owner._idle_maintenance
        reused = pool.get_connection()
        reused.send_command("PING")
        assert reused.read_response() == b"PONG"
        pool.release(reused)
    finally:
        resume_checkout.set()
        if waiter.ident is not None:
            waiter.join(5)
        gc.set_threshold(*thresholds)
        if enabled:
            gc.enable()
        else:
            gc.disable()
        peers.release(borrower)
        peers.close()
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("operation", ["maintenance_config", "checkout"])
def test_finalizer_during_pool_configuration_and_checkout(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_pool_lock,
        (
            request.config.getoption("--redis-url"),
            parser_class,
            automatic,
            operation,
            pool_class,
        ),
    )


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize(
    "operation", ["connection_count_free", "connection_count_used", "cancel_moving"]
)
def test_finalizer_during_metrics_and_moving_cancellation(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_pool_lock,
        (
            request.config.getoption("--redis-url"),
            parser_class,
            automatic,
            operation,
            pool_class,
        ),
    )


@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize(
    "operation", ["blocking_snapshot", "blocking_maintenance_snapshot"]
)
def test_finalizer_during_blocking_free_snapshot(
    request, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_pool_lock,
        (
            request.config.getoption("--redis-url"),
            parser_class,
            automatic,
            operation,
            BlockingConnectionPool,
        ),
    )


@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("operation", ["disconnect_snapshot", "rejected_release"])
def test_finalizer_during_pool_lock(request, parser_class, automatic, operation):
    _run_bounded(
        _finalizer_during_pool_lock,
        (request.config.getoption("--redis-url"), parser_class, automatic, operation),
    )


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("operation", ["topology_cleanup", "smigrated"])
def test_finalizer_during_topology_cleanup(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_pool_lock,
        (
            request.config.getoption("--redis-url"),
            parser_class,
            automatic,
            operation,
            pool_class,
        ),
    )


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize("operation", ["multidb", "multidb_cluster"])
def test_finalizer_during_database_change(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_pool_lock,
        (
            request.config.getoption("--redis-url"),
            parser_class,
            automatic,
            operation,
            pool_class,
        ),
    )


@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
@pytest.mark.parametrize("parser_class", PARSERS)
@pytest.mark.parametrize("automatic", [False, True])
@pytest.mark.parametrize(
    "operation", ["proactive_reconnect", "moving_reconnect", "scheduled_reconnect"]
)
def test_finalizer_during_proactive_reconnect(
    request, pool_class, parser_class, automatic, operation
):
    _run_bounded(
        _finalizer_during_pool_lock,
        (
            request.config.getoption("--redis-url"),
            parser_class,
            automatic,
            operation,
            pool_class,
        ),
    )


def _sentinel_auth_during_idle_drain(url, parser_class):
    writer = Redis.from_url(url)
    address = writer.connection_pool.connection_kwargs
    host, port = address.get("host", "localhost"), address.get("port", 6379)
    manager = Mock()
    manager.discover_master.return_value = host, port
    pool = SentinelConnectionPool(
        "cache-reentry",
        manager,
        protocol=3,
        db=address.get("db", 0),
        parser_class=parser_class,
        cache_config=CacheConfig(),
        max_connections=3,
        socket_timeout=1,
        maint_notifications_config=MaintNotificationsConfig(enabled=False),
    )
    owner, borrower, reconnect = (pool.get_connection() for _ in range(3))
    reconnect.disconnect()
    key = str(uuid.uuid4())
    writer.set(key, "old")
    owner.send_command("GET", key, keys=[key])
    assert owner.read_response() == b"old"
    pool.release(owner)
    pool.release(reconnect)
    # A fresh discovery of the same reachable server retires free sockets.
    pool.proxy.master_address = ("previous-master", port)
    draining, discovering = threading.Event(), threading.Event()
    errors = []
    original_poll = owner._conn.can_read
    original_discover = pool.proxy.get_master_address

    def poll(*args, **kwargs):
        if threading.current_thread() is reader:
            draining.set()
            assert discovering.wait(5), "AUTH did not start Sentinel discovery"
        return original_poll(*args, **kwargs)

    def discover():
        discovering.set()
        return original_discover()

    def read():
        borrower.send_command("GET", key, keys=[key])
        assert borrower.read_response() == b"old"

    token = Mock()
    token.try_get.return_value = "default"
    token.get_value.return_value = ""

    def run(callback):
        try:
            callback()
        except BaseException as error:
            errors.append(error)

    reader = threading.Thread(target=run, args=(read,), daemon=True)
    auth = threading.Thread(
        target=run, args=(lambda: pool.re_auth_callback(token),), daemon=True
    )
    try:
        with (
            patch.object(owner._conn, "can_read", poll),
            patch.object(pool.proxy, "get_master_address", discover),
        ):
            reader.start()
            assert draining.wait(3)
            auth.start()
            for worker in (reader, auth):
                worker.join(3)
            assert not reader.is_alive() and not auth.is_alive(), (
                "Sentinel AUTH and idle-drain cleanup waited on each other's locks"
            )
            assert not errors
        assert not owner.is_connected
        writer.set(key, "new")
        borrower.send_command("GET", key, keys=[key])
        assert borrower.read_response() == b"new"
        borrower.send_command("PING")
        assert borrower.read_response() == b"PONG"
    finally:
        pool.release(borrower)
        pool.close()
        writer.delete(key)
        writer.close()


@pytest.mark.parametrize("parser_class", PARSERS)
def test_sentinel_auth_during_idle_drain(request, parser_class):
    _run_bounded(
        _sentinel_auth_during_idle_drain,
        (request.config.getoption("--redis-url"), parser_class),
    )
