import asyncio
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest

import redis.asyncio as redis
from redis._defaults import DEFAULT_SOCKET_CONNECT_TIMEOUT, DEFAULT_SOCKET_TIMEOUT
from redis.asyncio.connection import (
    BlockingConnectionPool,
    Connection,
    ConnectionPool,
    UnixDomainSocketConnection,
)
from redis.asyncio.maint_notifications import (
    AsyncMaintNotificationsConnectionHandler,
    AsyncMaintNotificationsPoolHandler,
    AsyncOSSMaintNotificationsHandler,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, ResponseError
from redis.maint_notifications import (
    EndpointType,
    MaintenanceState,
    MaintNotificationsConfig,
    NodeFailedOverNotification,
    NodeFailingOverNotification,
    NodeMigratedNotification,
    NodeMigratingNotification,
    NodeMovingNotification,
    OSSNodeMigratedNotification,
)
from redis.utils import SENTINEL


DEFAULT_HOST = "12.45.34.56"
DEFAULT_PORT = 6379
MOVED_HOST = "1.2.3.4"
MOVED_PORT = 6380
RELAXED_TIMEOUT = 30


class DummyAsyncConnection:
    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, peer=DEFAULT_HOST):
        self.host = host
        self.port = port
        self.peer = peer
        self.socket_timeout = DEFAULT_SOCKET_TIMEOUT
        self.socket_connect_timeout = DEFAULT_SOCKET_CONNECT_TIMEOUT
        self.orig_host_address = host
        self.orig_socket_timeout = DEFAULT_SOCKET_TIMEOUT
        self.orig_socket_connect_timeout = DEFAULT_SOCKET_CONNECT_TIMEOUT
        self.maintenance_state = MaintenanceState.NONE
        self.maintenance_notification_hash = None
        self.connected = True
        self.disconnect_calls = 0
        self.timeout_updates = []
        self.reset_notifications_calls = 0
        self._should_reconnect = False

    def __repr__(self):
        return f"<DummyAsyncConnection(host={self.host},port={self.port})>"

    def getpeername(self):
        return self.peer

    def get_resolved_ip(self):
        return self.peer

    def set_tmp_settings(
        self,
        tmp_host_address: str | object | None = SENTINEL,
        tmp_relaxed_timeout: float | None = -1,
    ):
        if tmp_host_address and tmp_host_address != SENTINEL:
            self.host = str(tmp_host_address)
        if tmp_relaxed_timeout != -1:
            self.socket_timeout = tmp_relaxed_timeout
            self.socket_connect_timeout = tmp_relaxed_timeout

    def reset_tmp_settings(
        self,
        reset_host_address=False,
        reset_relaxed_timeout=False,
    ):
        if reset_host_address:
            self.host = self.orig_host_address
        if reset_relaxed_timeout:
            self.socket_timeout = self.orig_socket_timeout
            self.socket_connect_timeout = self.orig_socket_connect_timeout

    def update_current_socket_timeout(self, relaxed_timeout=None):
        self.timeout_updates.append(relaxed_timeout)

    def reset_received_notifications(self):
        self.reset_notifications_calls += 1

    def mark_for_reconnect(self):
        self._should_reconnect = True

    def should_reconnect(self):
        return self._should_reconnect

    async def disconnect(self, *args, **kwargs):
        self.disconnect_calls += 1
        self.connected = False


class UnsupportedAsyncConnection:
    def __init__(self, host=DEFAULT_HOST, **kwargs):
        self.host = host
        self.kwargs = kwargs


def _assert_connection_in_moving_state(
    connection,
    *,
    expected_host=MOVED_HOST,
    expected_timeout=RELAXED_TIMEOUT,
    expected_notification_hash=None,
):
    assert connection.maintenance_state == MaintenanceState.MOVING
    if expected_notification_hash is not None:
        assert connection.maintenance_notification_hash == expected_notification_hash
    assert connection.host == expected_host
    assert connection.socket_timeout == expected_timeout
    assert connection.socket_connect_timeout == expected_timeout


def _assert_connection_in_default_state(connection, *, expected_host=DEFAULT_HOST):
    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.maintenance_notification_hash is None
    assert connection.host == expected_host
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT


@pytest.mark.asyncio
async def test_async_connection_handler_applies_start_and_end_notifications():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ) as notification_count,
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeMigratingNotification(id=1, ttl=5))
        await handler.handle_notification(NodeMigratedNotification(id=1))

    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
    assert connection.timeout_updates == [RELAXED_TIMEOUT, -1]
    assert connection.reset_notifications_calls == 1
    assert notification_count.await_count == 2
    assert relaxed_timeout_metric.await_count == 2
    assert relaxed_timeout_metric.await_args_list[0].kwargs["relaxed"] is True
    assert relaxed_timeout_metric.await_args_list[1].kwargs["relaxed"] is False


@pytest.mark.asyncio
async def test_async_connection_handler_skips_timeout_changes_when_moving():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = DummyAsyncConnection()
    connection.maintenance_state = MaintenanceState.MOVING
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeFailingOverNotification(id=1, ttl=5))

    assert connection.maintenance_state == MaintenanceState.MOVING
    assert connection.timeout_updates == []
    relaxed_timeout_metric.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_connection_handler_handles_failover_start_and_end():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeFailingOverNotification(id=2, ttl=5))
        await handler.handle_notification(NodeFailedOverNotification(id=2))

    assert connection.maintenance_state == MaintenanceState.NONE
    assert connection.timeout_updates == [RELAXED_TIMEOUT, -1]
    assert relaxed_timeout_metric.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start_notification,end_notification",
    [
        (NodeMigratingNotification(id=1, ttl=5), NodeMigratedNotification(id=1)),
        (
            NodeFailingOverNotification(id=2, ttl=5),
            NodeFailedOverNotification(id=2),
        ),
    ],
)
async def test_async_connection_handler_ignores_relaxed_timeout_when_disabled(
    start_notification, end_notification
):
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=-1)
    connection = DummyAsyncConnection()
    handler = AsyncMaintNotificationsConnectionHandler(connection, config)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ) as notification_count,
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(start_notification)
        await handler.handle_notification(end_notification)

    _assert_connection_in_default_state(connection)
    assert connection.timeout_updates == []
    assert notification_count.await_count == 2
    relaxed_timeout_metric.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_pool_handler_logs_non_moving_notifications():
    config = MaintNotificationsConfig(enabled=True)
    pool = MagicMock()
    handler = AsyncMaintNotificationsPoolHandler(pool, config)

    with (
        mock.patch.object(handler, "remove_expired_notifications", new=AsyncMock()),
        mock.patch.object(
            handler, "handle_node_moving_notification", new=AsyncMock()
        ) as handle_moving,
        mock.patch("redis.asyncio.maint_notifications.logger.error") as log_error,
    ):
        await handler.handle_notification(NodeMigratingNotification(id=1, ttl=5))

    handle_moving.assert_not_awaited()
    log_error.assert_called_once()


@pytest.mark.asyncio
async def test_async_parser_routes_maintenance_push_notifications_to_connection_handler():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await connection._parser.handle_push_response(["MIGRATING", 1, 5])
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        assert connection.socket_timeout == RELAXED_TIMEOUT

        await connection._parser.handle_push_response(["MIGRATED", 1])
        assert connection.maintenance_state == MaintenanceState.NONE
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT

        await connection._parser.handle_push_response(["FAILING_OVER", 2, 5])
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        assert connection.socket_timeout == RELAXED_TIMEOUT

        await connection._parser.handle_push_response(["FAILED_OVER", 2])
        assert connection.maintenance_state == MaintenanceState.NONE
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT


@pytest.mark.asyncio
async def test_async_parser_routes_moving_push_notification_to_pool_handler():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = pool.make_connection()
    pool._in_use_connections.add(connection)
    connection._maint_notifications_pool_handler._schedule = MagicMock()

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await connection._parser.handle_push_response(
            ["MOVING", 1, 5, f"{MOVED_HOST}:{MOVED_PORT}"]
        )

    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    assert pool.connection_kwargs["maintenance_notification_hash"] == hash(
        NodeMovingNotification(1, MOVED_HOST, MOVED_PORT, 5)
    )
    assert pool.connection_kwargs["host"] == MOVED_HOST


@pytest.mark.asyncio
async def test_async_parser_routes_moving_push_without_target():
    config = MaintNotificationsConfig(enabled=True, relaxed_timeout=RELAXED_TIMEOUT)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = pool.make_connection()
    pool._in_use_connections.add(connection)
    connection._maint_notifications_pool_handler._schedule = MagicMock()

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await connection._parser.handle_push_response(["MOVING", 1, 5, None])

    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    connection._maint_notifications_pool_handler._schedule.assert_has_calls(
        [
            mock.call(
                2.5,
                connection._maint_notifications_pool_handler.run_proactive_reconnect,
                None,
            ),
            mock.call(
                5,
                connection._maint_notifications_pool_handler.handle_node_moved_notification,
                mock.ANY,
            ),
        ]
    )


def test_async_connection_installs_maintenance_parser_handlers():
    config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )

    connection = pool.make_connection()
    pool_handler = pool._maint_notifications_pool_handler

    node_moving_handler = connection._parser.node_moving_push_handler_func
    maintenance_handler = connection._parser.maintenance_push_handler_func

    assert node_moving_handler is not None
    assert node_moving_handler.__self__.connection is connection
    assert node_moving_handler.__self__.pool is pool
    assert node_moving_handler.__self__._lock is pool_handler._lock
    assert (
        node_moving_handler.__self__._processed_notifications
        is pool_handler._processed_notifications
    )

    assert maintenance_handler is not None
    assert (
        maintenance_handler.__self__
        is connection._maint_notifications_connection_handler
    )
    assert connection._maint_notifications_connection_handler.config is config


@pytest.mark.asyncio
async def test_async_pool_update_config_wires_existing_connections():
    disabled_config = MaintNotificationsConfig(enabled=False)
    enabled_config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=disabled_config,
    )
    free_connection = pool.make_connection()
    in_use_connection = pool.make_connection()
    pool._available_connections.append(free_connection)
    pool._in_use_connections.add(in_use_connection)

    await pool.update_maint_notifications_config(enabled_config)

    assert free_connection._parser.node_moving_push_handler_func is not None
    assert free_connection._parser.maintenance_push_handler_func is not None
    assert free_connection.maint_notifications_config is enabled_config
    assert not free_connection.should_reconnect()

    assert in_use_connection._parser.node_moving_push_handler_func is not None
    assert in_use_connection._parser.maintenance_push_handler_func is not None
    assert in_use_connection.maint_notifications_config is enabled_config
    assert in_use_connection.should_reconnect()

    assert pool.connection_kwargs["maint_notifications_config"] is enabled_config
    assert pool.connection_kwargs["maint_notifications_pool_handler"] is (
        pool._maint_notifications_pool_handler
    )


@pytest.mark.asyncio
async def test_async_pool_update_oss_handler_wires_existing_connections():
    """update_maint_notifications_config wires an OSS cluster handler onto every
    existing connection in the pool.

    Each connection's parser receives the OSS cluster and maintenance push
    handlers and the connection references the handler. In-use connections are
    marked for reconnect; free connections are wired then disconnected.
    """
    config = MaintNotificationsConfig(enabled=True)
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    free_connection = pool.make_connection()
    in_use_connection = pool.make_connection()
    pool._available_connections.append(free_connection)
    pool._in_use_connections.add(in_use_connection)

    oss_handler = AsyncOSSMaintNotificationsHandler(MagicMock(), config)

    await pool.update_maint_notifications_config(
        config, oss_cluster_maint_notifications_handler=oss_handler
    )

    # In-use connection: parser has the OSS + maintenance push handlers wired
    # and the connection is marked for reconnect.
    assert in_use_connection._oss_cluster_maint_notifications_handler is oss_handler
    in_use_oss_func = in_use_connection._parser.oss_cluster_maint_push_handler_func
    assert in_use_oss_func is not None
    assert in_use_oss_func.__func__ is oss_handler.handle_notification.__func__
    assert in_use_connection._parser.maintenance_push_handler_func is not None
    assert in_use_connection.should_reconnect()

    # Free connection — the idle OSS path, wired then disconnected.
    assert free_connection._oss_cluster_maint_notifications_handler is oss_handler
    assert free_connection._parser.oss_cluster_maint_push_handler_func is not None
    assert free_connection._parser.maintenance_push_handler_func is not None

    # Existing connections must not retain the orphaned pool-handler binding.
    # A default (enabled) pool wires node_moving on each connection at __init__;
    # switching to OSS mode must clear it so no existing connection is configured
    # with both the node-moving and OSS cluster handlers.
    assert in_use_connection._parser.node_moving_push_handler_func is None
    assert in_use_connection._maint_notifications_pool_handler is None
    assert free_connection._parser.node_moving_push_handler_func is None
    assert free_connection._maint_notifications_pool_handler is None

    # OSS cluster mode and pool-handler mode are mutually exclusive. The pool
    # was created with the default (enabled) config, which wires a pool handler
    # in __init__; switching to OSS mode must clear it from both the pool
    # attribute and the shared connection kwargs so future connections are not
    # configured with both handlers.
    assert pool._maint_notifications_pool_handler is None
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs
    assert (
        pool.connection_kwargs.get("oss_cluster_maint_notifications_handler")
        is oss_handler
    )

    # A connection created after the switch gets only the OSS cluster handler.
    new_connection = pool.make_connection()
    assert new_connection._maint_notifications_pool_handler is None
    assert new_connection._parser.node_moving_push_handler_func is None
    assert new_connection._oss_cluster_maint_notifications_handler is oss_handler
    assert new_connection._parser.oss_cluster_maint_push_handler_func is not None


def _make_async_oss_handler():
    cluster_client = MagicMock()
    # Keep the affected-nodes set and the node-reconnect loop empty so the test
    # focuses purely on how src/dest addresses are parsed.
    cluster_client.nodes_manager.get_node.return_value = None
    cluster_client.nodes_manager.nodes_cache.values.return_value = []
    cluster_client.nodes_manager.initialize = AsyncMock()
    config = MaintNotificationsConfig(enabled=True)
    return AsyncOSSMaintNotificationsHandler(cluster_client, config), cluster_client


@pytest.mark.asyncio
async def test_async_handle_smigrated_parses_ipv4_addresses():
    handler, cluster_client = _make_async_oss_handler()
    notification = OSSNodeMigratedNotification(
        id=1,
        nodes_to_slots_mapping={"127.0.0.1:6379": [{"127.0.0.1:6380": "1-100"}]},
    )

    await handler.handle_oss_maintenance_completed_notification(notification)

    cluster_client.nodes_manager.get_node.assert_called_once_with(
        host="127.0.0.1", port=6379
    )
    cluster_client.nodes_manager.initialize.assert_called_once_with(
        additional_startup_nodes_info=[("127.0.0.1", 6380)],
    )


@pytest.mark.asyncio
async def test_async_handle_smigrated_parses_ipv6_addresses():
    handler, cluster_client = _make_async_oss_handler()
    # IPv6 literals contain multiple colons; a naive split(":") would raise
    # ValueError on the host/port unpack.
    notification = OSSNodeMigratedNotification(
        id=1,
        nodes_to_slots_mapping={"2001:db8::1:6379": [{"2001:db8::2:6380": "1-100"}]},
    )

    await handler.handle_oss_maintenance_completed_notification(notification)

    cluster_client.nodes_manager.get_node.assert_called_once_with(
        host="2001:db8::1", port=6379
    )
    cluster_client.nodes_manager.initialize.assert_called_once_with(
        additional_startup_nodes_info=[("2001:db8::2", 6380)],
    )


@pytest.mark.asyncio
async def test_async_handshake_sends_maint_notifications_command():
    config = MaintNotificationsConfig(
        enabled=True,
        endpoint_type=EndpointType.INTERNAL_FQDN,
    )
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection.send_command = AsyncMock()
    connection.read_response = AsyncMock(return_value=b"OK")

    await connection.activate_maint_notifications_handling_if_enabled(
        check_health=False
    )

    connection.send_command.assert_awaited_once_with(
        "CLIENT",
        "MAINT_NOTIFICATIONS",
        "ON",
        "moving-endpoint-type",
        "internal-fqdn",
        check_health=False,
    )


@pytest.mark.asyncio
async def test_async_handshake_auto_suppresses_response_error():
    config = MaintNotificationsConfig(enabled="auto")
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection.send_command = AsyncMock()
    connection.read_response = AsyncMock(side_effect=ResponseError("unsupported"))

    await connection.activate_maint_notifications_handling_if_enabled()

    connection.send_command.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_handshake_enabled_raises_response_error():
    config = MaintNotificationsConfig(enabled=True)
    connection = Connection(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection.send_command = AsyncMock()
    connection.read_response = AsyncMock(side_effect=ResponseError("unsupported"))

    with pytest.raises(ResponseError):
        await connection.activate_maint_notifications_handling_if_enabled()


def test_async_pool_auto_enables_maint_notifications_for_default_resp3():
    pool = ConnectionPool(host=DEFAULT_HOST, port=DEFAULT_PORT)

    assert pool.maint_notifications_enabled() == "auto"
    assert isinstance(
        pool.connection_kwargs["maint_notifications_config"],
        MaintNotificationsConfig,
    )
    assert pool.connection_kwargs["maint_notifications_pool_handler"] is (
        pool._maint_notifications_pool_handler
    )


def test_async_pool_does_not_auto_enable_maint_notifications_without_valid_host():
    pool = ConnectionPool(host=None, port=DEFAULT_PORT, protocol=3)

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs


def test_async_pool_rejects_enabled_maint_notifications_without_valid_host():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications are not supported for connections without a host",
    ):
        ConnectionPool(
            host=None,
            port=DEFAULT_PORT,
            protocol=3,
            maint_notifications_config=config,
        )


def test_async_custom_connection_does_not_auto_enable_maint_notifications():
    pool = ConnectionPool(
        connection_class=UnsupportedAsyncConnection,
        host=DEFAULT_HOST,
        protocol=3,
    )

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs


def test_async_custom_connection_rejects_enabled_maint_notifications_config():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match=(
            "Maintenance notifications are not supported for connection class "
            "UnsupportedAsyncConnection"
        ),
    ):
        ConnectionPool(
            connection_class=UnsupportedAsyncConnection,
            host=DEFAULT_HOST,
            protocol=3,
            maint_notifications_config=config,
        )


def test_async_unix_socket_pool_does_not_auto_enable_maint_notifications():
    pool = ConnectionPool(
        connection_class=UnixDomainSocketConnection,
        path="/tmp/redis.sock",
        protocol=3,
    )

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs
    assert "orig_host_address" not in pool.connection_kwargs


@pytest.mark.asyncio
async def test_async_unix_socket_pool_auto_maint_notifications_noop():
    config = MaintNotificationsConfig(enabled="auto")
    pool = ConnectionPool(
        connection_class=UnixDomainSocketConnection,
        path="/tmp/redis.sock",
        protocol=3,
    )

    await pool.update_maint_notifications_config(config)

    assert pool._maint_notifications_pool_handler is None
    assert not pool.maint_notifications_enabled()
    assert "maint_notifications_config" not in pool.connection_kwargs
    assert "maint_notifications_pool_handler" not in pool.connection_kwargs


def test_async_unix_socket_pool_rejects_enabled_maint_notifications():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications are not supported for Unix domain socket connections",
    ):
        ConnectionPool(
            connection_class=UnixDomainSocketConnection,
            path="/tmp/redis.sock",
            protocol=3,
            maint_notifications_config=config,
        )


def test_async_redis_unix_socket_rejects_enabled_maint_notifications():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications are not supported with Unix domain socket connections",
    ):
        redis.Redis(
            unix_socket_path="/tmp/redis.sock",
            protocol=3,
            maint_notifications_config=config,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
async def test_async_pool_applies_and_cleans_up_moving_notification(pool_class):
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = pool_class(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    active_matching = DummyAsyncConnection(peer=DEFAULT_HOST)
    active_other = DummyAsyncConnection(peer="8.8.8.8")
    free_matching = DummyAsyncConnection(peer=DEFAULT_HOST)
    free_other = DummyAsyncConnection(peer="8.8.8.8")
    pool._in_use_connections.update({active_matching, active_other})
    pool._available_connections.extend([free_matching, free_other])
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=True,
    )

    for connection in (active_matching, free_matching):
        assert connection.maintenance_state == MaintenanceState.MOVING
        assert connection.maintenance_notification_hash == hash(notification)
        assert connection.host == MOVED_HOST
        assert connection.socket_timeout == RELAXED_TIMEOUT
        assert connection.socket_connect_timeout == RELAXED_TIMEOUT
        assert connection.timeout_updates == [RELAXED_TIMEOUT]

    assert active_matching.should_reconnect()
    assert free_matching.disconnect_calls == 1
    assert active_other.maintenance_state == MaintenanceState.NONE
    assert free_other.maintenance_state == MaintenanceState.NONE

    assert pool.connection_kwargs["host"] == MOVED_HOST
    assert pool.connection_kwargs["port"] == DEFAULT_PORT
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    assert pool.connection_kwargs["maintenance_notification_hash"] == hash(notification)

    await pool.cleanup_moving_notification(
        notification_hash=hash(notification),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    for connection in (active_matching, free_matching):
        assert connection.maintenance_state == MaintenanceState.NONE
        assert connection.maintenance_notification_hash is None
        assert connection.host == DEFAULT_HOST
        assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
        assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
        assert connection.timeout_updates == [RELAXED_TIMEOUT, -1]

    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    assert pool.connection_kwargs["port"] == DEFAULT_PORT
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.NONE
    assert pool.connection_kwargs["maintenance_notification_hash"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
async def test_async_pool_moving_with_disabled_relaxed_timeout_preserves_timeouts(
    pool_class,
):
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=-1,
    )
    pool = pool_class(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=True,
    )

    assert connection.maintenance_state == MaintenanceState.MOVING
    assert connection.maintenance_notification_hash == hash(notification)
    assert connection.host == MOVED_HOST
    assert connection.socket_timeout == DEFAULT_SOCKET_TIMEOUT
    assert connection.socket_connect_timeout == DEFAULT_SOCKET_CONNECT_TIMEOUT
    assert connection.timeout_updates == [-1]


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_class", [ConnectionPool, BlockingConnectionPool])
async def test_async_new_connections_inherit_moving_settings_until_cleanup(pool_class):
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = pool_class(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )

    moving_connection = pool.make_connection()
    _assert_connection_in_moving_state(
        moving_connection,
        expected_notification_hash=hash(notification),
    )
    assert moving_connection.orig_host_address == DEFAULT_HOST

    await pool.cleanup_moving_notification(
        notification_hash=hash(notification),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    restored_connection = pool.make_connection()
    _assert_connection_in_default_state(restored_connection)
    assert restored_connection.orig_host_address == DEFAULT_HOST


@pytest.mark.asyncio
async def test_async_migrated_after_moving_does_not_clear_moving_state():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    connection = DummyAsyncConnection()
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    pool._in_use_connections.add(connection)
    moving_notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=moving_notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )

    handler = AsyncMaintNotificationsConnectionHandler(connection, config)
    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await handler.handle_notification(NodeMigratedNotification(id=2))

    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(moving_notification),
    )
    assert pool.connection_kwargs["maintenance_state"] == MaintenanceState.MOVING
    relaxed_timeout_metric.assert_not_awaited()


@pytest.mark.asyncio
async def test_async_overlapping_moving_cleanup_only_reverts_latest_notification():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    first = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )
    second_host = "5.6.7.8"
    second = NodeMovingNotification(
        id=2,
        new_node_host=second_host,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(first, config, DEFAULT_HOST)
    await pool.apply_moving_notification(second, config, DEFAULT_HOST)

    assert pool.connection_kwargs["host"] == second_host
    _assert_connection_in_moving_state(
        connection,
        expected_host=second_host,
        expected_notification_hash=hash(second),
    )

    await pool.cleanup_moving_notification(
        notification_hash=hash(first),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    assert pool.connection_kwargs["host"] == second_host
    assert pool.connection_kwargs["maintenance_notification_hash"] == hash(second)
    _assert_connection_in_moving_state(
        connection,
        expected_host=second_host,
        expected_notification_hash=hash(second),
    )

    await pool.cleanup_moving_notification(
        notification_hash=hash(second),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    assert pool.connection_kwargs["host"] == DEFAULT_HOST
    _assert_connection_in_default_state(connection)


@pytest.mark.asyncio
async def test_async_moving_without_target_marks_for_reconnect_on_delayed_run():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    active = DummyAsyncConnection(peer=DEFAULT_HOST)
    free = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(active)
    pool._available_connections.append(free)
    notification = NodeMovingNotification(
        id=1,
        new_node_host=None,
        new_node_port=None,
        ttl=5,
    )

    await pool.apply_moving_notification(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )

    assert not active.should_reconnect()
    assert free.disconnect_calls == 0
    _assert_connection_in_moving_state(
        active,
        expected_host=DEFAULT_HOST,
        expected_notification_hash=hash(notification),
    )

    await pool.run_proactive_reconnect(DEFAULT_HOST)

    assert active.should_reconnect()
    assert free.disconnect_calls == 1


@pytest.mark.asyncio
async def test_async_moving_notifications_only_update_matching_address_connections():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host="test.address.com",
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    key1_connections = [
        DummyAsyncConnection(host="test.address.com", peer="1.2.3.4"),
        DummyAsyncConnection(host="test.address.com", peer="1.2.3.4"),
    ]
    key2_connections = [
        DummyAsyncConnection(host="test.address.com", peer="5.6.7.8"),
        DummyAsyncConnection(host="test.address.com", peer="5.6.7.8"),
    ]
    key3_connection = DummyAsyncConnection(host="test.address.com", peer="9.10.11.12")
    pool._in_use_connections.update(
        {key1_connections[0], key2_connections[0], key3_connection}
    )
    pool._available_connections.extend([key1_connections[1], key2_connections[1]])
    first = NodeMovingNotification(
        id=1,
        new_node_host="13.14.15.16",
        new_node_port=DEFAULT_PORT,
        ttl=5,
    )
    second = NodeMovingNotification(
        id=2,
        new_node_host="17.18.19.20",
        new_node_port=DEFAULT_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(first, config, "1.2.3.4")

    for connection in key1_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="13.14.15.16",
            expected_notification_hash=hash(first),
        )
    for connection in (*key2_connections, key3_connection):
        _assert_connection_in_default_state(
            connection, expected_host="test.address.com"
        )

    await pool.apply_moving_notification(second, config, "5.6.7.8")

    for connection in key1_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="13.14.15.16",
            expected_notification_hash=hash(first),
        )
    for connection in key2_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="17.18.19.20",
            expected_notification_hash=hash(second),
        )
    _assert_connection_in_default_state(
        key3_connection, expected_host="test.address.com"
    )

    key2_handler = AsyncMaintNotificationsConnectionHandler(key2_connections[0], config)
    key3_handler = AsyncMaintNotificationsConnectionHandler(key3_connection, config)
    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await key2_handler.handle_notification(NodeMigratingNotification(id=3, ttl=5))
        await key2_handler.handle_notification(NodeMigratedNotification(id=3))
        await key3_handler.handle_notification(NodeMigratingNotification(id=4, ttl=5))
        await key3_handler.handle_notification(NodeMigratedNotification(id=4))

    _assert_connection_in_moving_state(
        key2_connections[0],
        expected_host="17.18.19.20",
        expected_notification_hash=hash(second),
    )
    _assert_connection_in_default_state(
        key3_connection, expected_host="test.address.com"
    )

    await pool.cleanup_moving_notification(
        notification_hash=hash(first),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    for connection in key1_connections:
        _assert_connection_in_default_state(
            connection, expected_host="test.address.com"
        )
    for connection in key2_connections:
        _assert_connection_in_moving_state(
            connection,
            expected_host="17.18.19.20",
            expected_notification_hash=hash(second),
        )

    await pool.cleanup_moving_notification(
        notification_hash=hash(second),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    for connection in (*key1_connections, *key2_connections, key3_connection):
        _assert_connection_in_default_state(
            connection, expected_host="test.address.com"
        )


@pytest.mark.asyncio
async def test_async_moving_migrating_migrated_failed_over_state_transitions():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    free_connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    pool._available_connections.append(free_connection)
    moving = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    await pool.apply_moving_notification(
        moving,
        config,
        DEFAULT_HOST,
        run_proactive_reconnect=True,
    )
    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(moving),
    )
    _assert_connection_in_moving_state(
        free_connection,
        expected_notification_hash=hash(moving),
    )

    connection_handler = AsyncMaintNotificationsConnectionHandler(connection, config)
    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ) as relaxed_timeout_metric,
    ):
        await connection_handler.handle_notification(
            NodeMigratingNotification(id=2, ttl=5)
        )
        await connection_handler.handle_notification(NodeMigratedNotification(id=2))
        await connection_handler.handle_notification(
            NodeFailingOverNotification(id=3, ttl=5)
        )
        await connection_handler.handle_notification(NodeFailedOverNotification(id=3))

    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(moving),
    )
    relaxed_timeout_metric.assert_not_awaited()

    await pool.cleanup_moving_notification(
        notification_hash=hash(moving),
        reset_relaxed_timeout=True,
        reset_host_address=True,
    )

    _assert_connection_in_default_state(connection)
    _assert_connection_in_default_state(free_connection)

    with (
        mock.patch(
            "redis.asyncio.maint_notifications.record_maint_notification_count",
            new=AsyncMock(),
        ),
        mock.patch(
            "redis.asyncio.maint_notifications.record_connection_relaxed_timeout",
            new=AsyncMock(),
        ),
    ):
        await connection_handler.handle_notification(
            NodeFailingOverNotification(id=4, ttl=5)
        )
        assert connection.maintenance_state == MaintenanceState.MAINTENANCE
        await connection_handler.handle_notification(NodeFailedOverNotification(id=4))

    _assert_connection_in_default_state(connection)


@pytest.mark.asyncio
async def test_async_concurrent_moving_notification_handling_is_deduplicated():
    config = MaintNotificationsConfig(
        enabled=True,
        proactive_reconnect=True,
        relaxed_timeout=RELAXED_TIMEOUT,
    )
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=config,
    )
    connection = DummyAsyncConnection(peer=DEFAULT_HOST)
    pool._in_use_connections.add(connection)
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    handler.set_connection(connection)
    handler._schedule = MagicMock()
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ) as handoff_metric:
        await asyncio.gather(
            *(handler.handle_notification(notification) for _ in range(5))
        )

    _assert_connection_in_moving_state(
        connection,
        expected_notification_hash=hash(notification),
    )
    assert len(handler._processed_notifications) == 1
    handler._schedule.assert_called_once_with(
        notification.ttl,
        handler.handle_node_moved_notification,
        notification,
    )
    handoff_metric.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_pool_handler_deduplicates_moving_notifications():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    pool = MagicMock()
    pool.apply_moving_notification = AsyncMock()
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    handler.set_connection(DummyAsyncConnection())
    handler._schedule = MagicMock()
    notification = NodeMovingNotification(
        id=1,
        new_node_host=MOVED_HOST,
        new_node_port=MOVED_PORT,
        ttl=5,
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ) as handoff_metric:
        await handler.handle_notification(notification)
        await handler.handle_notification(notification)

    pool.apply_moving_notification.assert_awaited_once_with(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=True,
    )
    handler._schedule.assert_called_once_with(
        notification.ttl,
        handler.handle_node_moved_notification,
        notification,
    )
    handoff_metric.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_pool_handler_schedules_delayed_reconnect_for_unknown_target():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    pool = MagicMock()
    pool.apply_moving_notification = AsyncMock()
    handler = AsyncMaintNotificationsPoolHandler(pool, config)
    handler.set_connection(DummyAsyncConnection())
    handler._schedule = MagicMock()
    notification = NodeMovingNotification(
        id=1,
        new_node_host=None,
        new_node_port=None,
        ttl=10,
    )

    with mock.patch(
        "redis.asyncio.maint_notifications.record_connection_handoff",
        new=AsyncMock(),
    ):
        await handler.handle_notification(notification)

    pool.apply_moving_notification.assert_awaited_once_with(
        notification=notification,
        config=config,
        moving_address_src=DEFAULT_HOST,
        run_proactive_reconnect=False,
    )
    handler._schedule.assert_has_calls(
        [
            mock.call(
                notification.ttl / 2, handler.run_proactive_reconnect, DEFAULT_HOST
            ),
            mock.call(
                notification.ttl, handler.handle_node_moved_notification, notification
            ),
        ]
    )


@pytest.mark.asyncio
async def test_blocking_pool_locks_get_only_during_maintenance():
    pool = BlockingConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )

    real_lock = pool._lock
    pool._lock = MagicMock(wraps=real_lock)

    fake_conn = MagicMock()
    fake_conn.connect = AsyncMock()
    fake_conn.can_read = AsyncMock(return_value=False)
    fake_conn.should_reconnect = MagicMock(return_value=False)
    fake_conn.re_auth = AsyncMock()
    pool._available_connections.append(fake_conn)

    # Outside maintenance: get_connection does not acquire _lock,
    # but release() still does (it goes through ConnectionPool.release).
    conn = await pool.get_connection()
    await pool.release(conn)
    assert pool._lock.__aenter__.call_count == 1

    # Inside maintenance: get_connection now also acquires _lock.
    pool.set_in_maintenance(True)
    try:
        conn = await pool.get_connection()
        await pool.release(conn)
    finally:
        pool.set_in_maintenance(False)
    assert pool._lock.__aenter__.call_count == 3


@pytest.mark.asyncio
async def test_apply_moving_notification_toggles_set_in_maintenance():
    pool = BlockingConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )
    observed = []
    original = pool.set_in_maintenance

    def spy(value: bool) -> None:
        observed.append(value)
        original(value)

    pool.set_in_maintenance = spy  # type: ignore[assignment]

    notification = NodeMovingNotification(
        id=1, new_node_host=MOVED_HOST, new_node_port=MOVED_PORT, ttl=5
    )
    await pool.apply_moving_notification(
        notification=notification,
        config=pool._maint_notifications_pool_handler.config,
        moving_address_src=None,
        run_proactive_reconnect=False,
    )

    assert observed == [True, False]


@pytest.mark.asyncio
async def test_cancel_scheduled_tasks_cancels_pending_and_awaits():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    handler = AsyncMaintNotificationsPoolHandler(MagicMock(), config)

    callback = AsyncMock()
    handler._schedule(60, callback)
    handler._schedule(60, callback)
    tasks = tuple(handler._scheduled_tasks)
    assert len(tasks) == 2

    await handler.cancel_scheduled_tasks()

    for task in tasks:
        assert task.done()
        assert task.cancelled()
    callback.assert_not_awaited()
    assert handler._scheduled_tasks == set()


@pytest.mark.asyncio
async def test_cancel_scheduled_tasks_noop_when_empty():
    config = MaintNotificationsConfig(enabled=True, proactive_reconnect=True)
    handler = AsyncMaintNotificationsPoolHandler(MagicMock(), config)
    await handler.cancel_scheduled_tasks()
    assert handler._scheduled_tasks == set()


@pytest.mark.asyncio
async def test_pool_aclose_cancels_handler_scheduled_tasks():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )
    handler = pool._maint_notifications_pool_handler
    assert handler is not None

    callback = AsyncMock()
    handler._schedule(60, callback)
    handler._schedule(60, callback)
    tasks = tuple(handler._scheduled_tasks)
    assert len(tasks) == 2

    await pool.aclose()

    for task in tasks:
        assert task.cancelled()
    callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_pool_disconnect_does_not_cancel_scheduled_tasks():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(
            enabled=True, proactive_reconnect=True
        ),
    )
    handler = pool._maint_notifications_pool_handler
    assert handler is not None

    handler._schedule(60, AsyncMock())
    tasks = tuple(handler._scheduled_tasks)

    try:
        await pool.disconnect()
        for task in tasks:
            assert not task.done()
    finally:
        await handler.cancel_scheduled_tasks()


@pytest.mark.asyncio
async def test_async_pool_ensure_connection_allows_pending_push_when_enabled():
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(enabled=True),
    )
    connection = MagicMock()
    connection.connect = AsyncMock()
    connection.can_read = AsyncMock(return_value=True)
    connection.disconnect = AsyncMock()

    await pool.ensure_connection(connection)

    connection.disconnect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled, raises", [(True, False), (False, True)])
async def test_async_pool_ensure_connection_handles_pending_push_after_reconnect(
    enabled, raises
):
    pool = ConnectionPool(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        protocol=3,
        maint_notifications_config=MaintNotificationsConfig(enabled=enabled),
    )
    connection = MagicMock()
    connection.connect = AsyncMock()
    connection.can_read = AsyncMock(side_effect=[RedisConnectionError("closed"), True])
    connection.disconnect = AsyncMock()

    if raises:
        with pytest.raises(RedisConnectionError, match="Connection not ready"):
            await pool.ensure_connection(connection)
    else:
        await pool.ensure_connection(connection)

    assert connection.connect.await_count == 2
    connection.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_read_response_reschedules_active_socket_timeout():
    connection = Connection(socket_timeout=0.01)

    class RelaxingParser:
        async def read_response(self, disable_decoding=False, push_request=False):
            connection.update_current_socket_timeout(0.2)
            await asyncio.sleep(0.05)
            return b"OK"

    connection._parser = RelaxingParser()

    assert await connection.read_response() == b"OK"
    assert connection._active_read_timeout is None


@pytest.mark.asyncio
async def test_read_response_reschedules_active_socket_timeout_to_blocking():
    connection = Connection(socket_timeout=0.01)

    class RelaxingParser:
        async def read_response(self, disable_decoding=False, push_request=False):
            connection.update_current_socket_timeout(None)
            await asyncio.sleep(0.05)
            return b"OK"

    connection._parser = RelaxingParser()

    assert await connection.read_response() == b"OK"
    assert connection._active_read_timeout is None


@pytest.mark.asyncio
async def test_read_response_does_not_reschedule_explicit_timeout():
    connection = Connection(socket_timeout=0.2)

    class RelaxingParser:
        async def read_response(self, disable_decoding=False, push_request=False):
            connection.update_current_socket_timeout(0.2)
            await asyncio.sleep(0.05)
            return b"OK"

    connection._parser = RelaxingParser()

    assert await connection.read_response(timeout=0.01) is None


def test_async_redis_maint_notifications_config_forwarded_to_internal_pool():
    config = MaintNotificationsConfig(enabled=True)
    client = redis.Redis(protocol=3, maint_notifications_config=config)

    assert (
        client.connection_pool.connection_kwargs["maint_notifications_config"] is config
    )
    assert client.connection_pool._maint_notifications_pool_handler.config is config


def test_async_redis_maint_notifications_config_requires_resp3():
    config = MaintNotificationsConfig(enabled=True)

    with pytest.raises(
        RedisError,
        match="Maintenance notifications handlers on connection are only supported",
    ):
        redis.Redis(protocol=2, maint_notifications_config=config)


def test_async_redis_maint_notifications_config_ignored_with_explicit_pool():
    pool = MagicMock()
    pool.connection_kwargs = {"protocol": 2, "legacy_responses": True}
    pool.get_encoder.return_value = MagicMock()
    config = MaintNotificationsConfig(enabled=True)

    client = redis.Redis(connection_pool=pool, maint_notifications_config=config)

    assert client.connection_pool is pool
    assert "maint_notifications_config" not in pool.connection_kwargs


async def _make_retry_call(do, fail, is_retryable=None, with_failure_count=False):
    return await do()


def _mock_single_connection_pool(connection):
    pool = MagicMock()
    pool.get_connection = AsyncMock(return_value=connection)
    pool.release = AsyncMock()
    pool.get_encoder.return_value = MagicMock()
    pool.get_protocol.return_value = 2
    return pool


def _mock_single_connection(should_reconnect):
    connection = MagicMock()
    connection.host = "localhost"
    connection.port = DEFAULT_PORT
    connection.db = 0
    connection.should_reconnect.return_value = should_reconnect
    connection.disconnect = AsyncMock()
    connection.connect = AsyncMock()
    connection.retry = MagicMock()
    connection.retry.call_with_retry = _make_retry_call
    connection.retry.get_retries.return_value = 0
    return connection


@pytest.mark.asyncio
async def test_single_connection_client_reconnects_marked_connection():
    connection = _mock_single_connection(should_reconnect=True)
    pool = _mock_single_connection_pool(connection)
    client = redis.Redis(connection_pool=pool, single_connection_client=True)

    async def mock_send(*args, **kwargs):
        return b"PONG"

    client._send_command_parse_response = mock_send

    assert await client.execute_command("PING") == b"PONG"
    connection.disconnect.assert_awaited_once_with(error=None, failure_count=None)
    connection.connect.assert_awaited_once()
    pool.release.assert_not_awaited()

    await client.aclose(close_connection_pool=False)


@pytest.mark.asyncio
async def test_single_connection_client_does_not_reconnect_unmarked_connection():
    connection = _mock_single_connection(should_reconnect=False)
    pool = _mock_single_connection_pool(connection)
    client = redis.Redis(connection_pool=pool, single_connection_client=True)

    async def mock_send(*args, **kwargs):
        return b"PONG"

    client._send_command_parse_response = mock_send

    assert await client.execute_command("PING") == b"PONG"
    connection.disconnect.assert_not_awaited()
    connection.connect.assert_not_awaited()
    pool.release.assert_not_awaited()

    await client.aclose(close_connection_pool=False)
