import asyncio
from dataclasses import dataclass
from typing import List, Optional

import pytest

import redis.asyncio as redis
from redis.asyncio.cluster import ClusterNode
from redis.maint_notifications import MaintNotificationsConfig, MaintenanceState
from tests.maint_notifications.proxy_server_helpers import (
    ProxyInterceptorHelper,
    RespTranslator,
    SlotsRange,
)

NODE_PORT_1 = 15379
NODE_PORT_2 = 15380
NODE_PORT_3 = 15381

NODE_PORT_NEW = 15382

# IP addresses used in tests
NODE_IP_LOCALHOST = "127.0.0.1"
NODE_IP_PROXY = "0.0.0.0"

DEFAULT_SOCKET_TIMEOUT = 5

# Initial cluster node configuration for proxy-based tests
PROXY_CLUSTER_NODES = [
    ClusterNode("127.0.0.1", NODE_PORT_1),
    ClusterNode("127.0.0.1", NODE_PORT_2),
    ClusterNode("127.0.0.1", NODE_PORT_3),
]

CLUSTER_SLOTS_INTERCEPTOR_NAME = "test_topology"


def _preserve_startup_nodes_order(_startup_nodes):
    pass


def _node_free_connections(node: ClusterNode):
    """Return the free/idle connections of an async ClusterNode."""
    return list(node._free)


def _node_in_use_connections(node: ClusterNode):
    """Return the in-use connections of an async ClusterNode.

    In the async cluster, the ClusterNode IS the connection pool. In-use
    connections are those tracked in ``_connections`` but not currently
    sitting in the ``_free`` deque.
    """
    free = set(node._free)
    return [conn for conn in node._connections if conn not in free]


def _node_all_connections(node: ClusterNode):
    """Return all connections (free + in-use) of an async ClusterNode."""
    return list(node._connections)


class TestAsyncClusterMaintNotificationsBase:
    """Base class for async cluster maintenance notifications handling tests."""

    async def _create_cluster_client(
        self,
        max_connections=10,
        maint_config=None,
        protocol=3,
    ) -> "redis.RedisCluster":
        """Create an async RedisCluster instance and initialize its topology.

        Unlike the sync client, the async RedisCluster discovers the cluster
        topology lazily, so we explicitly ``await initialize()`` here to make
        ``nodes_manager.nodes_cache`` available to the tests.

        Note: unlike the sync cluster, the async RedisCluster does not support
        client-side caching (no ``cache_config``) nor a pluggable
        ``connection_pool_class`` (the ClusterNode is its own pool), so the
        corresponding sync config tests are intentionally omitted here.
        """
        if maint_config is None and hasattr(self, "config") and self.config is not None:
            maint_config = self.config

        test_redis_client = redis.RedisCluster(
            protocol=protocol,
            startup_nodes=PROXY_CLUSTER_NODES,
            maint_notifications_config=maint_config,
            max_connections=max_connections,
        )
        await test_redis_client.initialize()

        return test_redis_client

    async def _warm_up_connection_pools(
        self, cluster: "redis.RedisCluster", created_connections_count: int = 3
    ):
        """Warm up node pools by acquiring, connecting and releasing connections.

        The connections must be actually connected so that the proxy can push
        maintenance notifications onto them and so that the maintenance push
        handlers get installed on each connection's parser (this happens during
        ``on_connect``).
        """
        for node in cluster.nodes_manager.nodes_cache.values():
            node_connections = []
            for _ in range(created_connections_count):
                conn = node.acquire_connection()
                await conn.connect()
                node_connections.append(conn)
            for conn in node_connections:
                node.release(conn)
            node_connections.clear()

    async def _drain_maint_notification_tasks(self, cluster: "redis.RedisCluster"):
        """Wait for any in-flight async OSS maintenance handling tasks to finish.

        SMIGRATED notifications are handled by ``AsyncOSSMaintNotificationsHandler``
        in background tasks scheduled from the parser's push callback. Unlike the
        sync client - where SMIGRATED handling completes inline while reading the
        command response - the async client returns from the command before the
        topology update finishes. Tests must drain these tasks before asserting on
        the resulting topology / connection state.
        """
        handler = cluster._oss_cluster_maint_notifications_handler
        if handler is None:
            return
        # The handler may schedule further tasks (e.g. disconnect_free_connections)
        # while draining, so loop until no tasks remain.
        for _ in range(100):
            tasks = [t for t in handler._background_tasks if not t.done()]
            if not tasks:
                break
            await asyncio.gather(*tasks, return_exceptions=True)
        # Give the event loop a chance to run any done-callbacks.
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestAsyncClusterMaintNotificationsConfig(TestAsyncClusterMaintNotificationsBase):
    """Test the maint_notifications_config parameter of async RedisCluster."""

    def _validate_maint_config_on_cluster(
        self,
        cluster: "redis.RedisCluster",
        expected_enabled,
        expected_proactive_reconnect: bool,
        expected_relaxed_timeout: int,
    ) -> None:
        """Validate maint_notifications_config stored on the cluster client.

        Note: the async cluster stores the config on the client itself (via the
        AsyncMaintNotificationsAbstractRedisCluster mixin), not on the
        NodesManager as the sync client does.
        """
        assert cluster.maint_notifications_config is not None
        assert cluster.maint_notifications_config.enabled == expected_enabled
        assert (
            cluster.maint_notifications_config.proactive_reconnect
            == expected_proactive_reconnect
        )
        assert (
            cluster.maint_notifications_config.relaxed_timeout
            == expected_relaxed_timeout
        )

    def _validate_handler_on_nodes(
        self,
        cluster: "redis.RedisCluster",
        should_have_handler: bool = True,
    ) -> None:
        """Validate the OSS cluster handler propagation to nodes' connection kwargs.

        Covers both discovered nodes (nodes_cache, populated during initialize())
        and startup nodes. Startup nodes are built before the maint mixin runs and
        keep their own connection_kwargs snapshot, so they must be wired explicitly;
        if they are not, initialize() opens its CLUSTER SLOTS discovery connection
        on a startup node with no push handler and drops maintenance notifications.
        """
        nodes = list(cluster.nodes_manager.nodes_cache.values())
        startup_nodes = list(cluster.nodes_manager.startup_nodes.values())
        assert len(nodes) > 0, "Cluster should have at least one node"
        assert len(startup_nodes) > 0, "Cluster should have at least one startup node"

        for node in nodes + startup_nodes:
            handler = node.connection_kwargs.get(
                "oss_cluster_maint_notifications_handler"
            )
            if should_have_handler:
                assert handler is not None
                assert handler is cluster._oss_cluster_maint_notifications_handler
            else:
                assert handler is None

    async def test_maint_notifications_config(self):
        """maint_notifications_config is stored on the client and handler propagated."""
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=True, relaxed_timeout=30
        )

        cluster = await self._create_cluster_client(maint_config=maint_config)

        try:
            self._validate_maint_config_on_cluster(cluster, False, True, 30)
            # enabled=False -> no handler created/propagated
            self._validate_handler_on_nodes(cluster, should_have_handler=False)

            # Verify we can execute commands without errors
            await cluster.set("test", "VAL")
            res = await cluster.get("test")
            assert res == b"VAL"
        finally:
            await cluster.aclose()

    async def test_config_propagation_to_new_nodes(self):
        """When a new node is discovered, it receives the same handler via kwargs."""
        maint_config = MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=True, relaxed_timeout=25
        )

        cluster = await self._create_cluster_client(maint_config=maint_config)

        try:
            initial_node_count = len(cluster.nodes_manager.nodes_cache)
            self._validate_handler_on_nodes(cluster, should_have_handler=True)

            # Reinitialize to ensure all nodes are (re)discovered
            await cluster.nodes_manager.initialize()

            new_node_count = len(cluster.nodes_manager.nodes_cache)
            assert new_node_count >= initial_node_count
            self._validate_handler_on_nodes(cluster, should_have_handler=True)
        finally:
            await cluster.aclose()

    async def test_none_config_default_behavior(self):
        """maint_notifications_config=None with protocol 3 -> default config created."""
        cluster = await self._create_cluster_client(maint_config=None)

        try:
            assert cluster.nodes_manager is not None
            # for protocol 3, maint_notifications_config should default to "auto"
            assert cluster.maint_notifications_config is not None
            assert cluster.maint_notifications_config.enabled == "auto"
            assert len(cluster.nodes_manager.nodes_cache) > 0

            await cluster.set("test", "VAL")
            res = await cluster.get("test")
            assert res == b"VAL"
        finally:
            await cluster.aclose()

    async def test_none_config_default_behavior_for_protocol_2(self):
        """maint_notifications_config=None with protocol 2 -> not initialized."""
        cluster = await self._create_cluster_client(protocol=2)

        try:
            assert cluster.nodes_manager is not None
            # for protocol 2, maint_notifications_config should not be created
            assert cluster.maint_notifications_config is None
            assert len(cluster.nodes_manager.nodes_cache) > 0

            await cluster.set("test", "VAL")
            res = await cluster.get("test")
            assert res == b"VAL"
        finally:
            await cluster.aclose()

    async def test_config_with_enabled_false(self):
        """enabled=False -> handlers are not created/propagated."""
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=False, relaxed_timeout=-1
        )

        cluster = await self._create_cluster_client(maint_config=maint_config)

        try:
            self._validate_maint_config_on_cluster(cluster, False, False, -1)
            self._validate_handler_on_nodes(cluster, should_have_handler=False)
            assert cluster._oss_cluster_maint_notifications_handler is None

            await cluster.set("test", "VAL")
            res = await cluster.get("test")
            assert res == b"VAL"
        finally:
            await cluster.aclose()

    async def test_config_with_pipeline_operations(self):
        """maint_notifications_config works with pipelined commands."""
        maint_config = MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=True, relaxed_timeout=10
        )

        cluster = await self._create_cluster_client(maint_config=maint_config)

        try:
            self._validate_maint_config_on_cluster(cluster, "auto", True, 10)
            self._validate_handler_on_nodes(cluster, should_have_handler=True)

            pipe = cluster.pipeline()
            pipe.set("pipe_key1", "value1")
            pipe.set("pipe_key2", "value2")
            pipe.get("pipe_key1")
            pipe.get("pipe_key2")
            results = await pipe.execute()

            assert results[0] is True or results[0] == b"OK"
            assert results[1] is True or results[1] == b"OK"
            assert results[2] == b"value1"
            assert results[3] == b"value2"
        finally:
            await cluster.aclose()

    async def test_explicit_enable_without_resp3_raises(self):
        """Explicit enabled=True with protocol 2 raises RedisError."""
        from redis.exceptions import RedisError

        maint_config = MaintNotificationsConfig(enabled=True)
        with pytest.raises(RedisError):
            redis.RedisCluster(
                protocol=2,
                startup_nodes=PROXY_CLUSTER_NODES,
                maint_notifications_config=maint_config,
            )

    async def test_handler_wired_on_startup_nodes_before_initialize(self):
        """Startup nodes must carry the OSS handler before initialize() runs.

        Regression test for the startup-node kwargs snapshot bug: startup
        ClusterNodes are constructed before the maint mixin runs and keep their
        own connection_kwargs copy. If the handler is injected only into the
        shared nodes_manager.connection_kwargs, the startup nodes stay stale and
        initialize() opens its CLUSTER SLOTS discovery connection with no push
        handler wired, silently dropping maintenance notifications on it.

        This must be asserted *before* initialize(): with the default
        dynamic_startup_nodes=True, initialize() replaces startup_nodes with the
        (handler-wired) discovered nodes, which would mask the original defect.
        """
        maint_config = MaintNotificationsConfig(enabled=True)
        cluster = redis.RedisCluster(
            protocol=3,
            startup_nodes=PROXY_CLUSTER_NODES,
            maint_notifications_config=maint_config,
        )
        try:
            handler = cluster._oss_cluster_maint_notifications_handler
            assert handler is not None

            startup_nodes = list(cluster.nodes_manager.startup_nodes.values())
            assert len(startup_nodes) == len(PROXY_CLUSTER_NODES)
            for node in startup_nodes:
                assert (
                    node.connection_kwargs.get(
                        "oss_cluster_maint_notifications_handler"
                    )
                    is handler
                )
        finally:
            await cluster.aclose()


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestAsyncClusterMaintNotificationsHandler(TestAsyncClusterMaintNotificationsBase):
    """Test AsyncOSSMaintNotificationsHandler propagation to connections."""

    def _validate_connection_handlers(self, conn, cluster_client, config):
        """Validate that the connection's parser handlers are properly set."""
        # The oss cluster handler function is correctly set on the parser
        oss_handler = conn._parser.oss_cluster_maint_push_handler_func
        assert oss_handler is not None
        assert hasattr(oss_handler, "__self__")
        assert hasattr(oss_handler, "__func__")
        assert oss_handler.__self__.cluster_client is cluster_client
        assert (
            oss_handler.__self__._lock
            is cluster_client._oss_cluster_maint_notifications_handler._lock
        )
        assert (
            oss_handler.__self__._processed_notifications
            is cluster_client._oss_cluster_maint_notifications_handler._processed_notifications
        )

        # The maintenance (connection-level) handler is correctly set on the parser
        maint_handler = conn._parser.maintenance_push_handler_func
        assert maint_handler is not None
        assert hasattr(maint_handler, "__self__")
        assert hasattr(maint_handler, "__func__")
        assert maint_handler.__self__ is conn._maint_notifications_connection_handler
        assert (
            maint_handler.__func__
            is conn._maint_notifications_connection_handler.handle_notification.__func__
        )

        assert conn._maint_notifications_connection_handler.config is config

    async def test_oss_maint_handler_propagation(self):
        """OSSMaintNotificationsHandler is propagated to all connections."""
        config = MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=True, relaxed_timeout=30
        )
        cluster = await self._create_cluster_client(maint_config=config)
        try:
            await self._warm_up_connection_pools(cluster, created_connections_count=3)
            for node in cluster.nodes_manager.nodes_cache.values():
                for conn in _node_all_connections(node):
                    assert conn._oss_cluster_maint_notifications_handler is not None
                    self._validate_connection_handlers(
                        conn, cluster, cluster.maint_notifications_config
                    )
        finally:
            await cluster.aclose()


class TestAsyncClusterMaintNotificationsHandlingBase(
    TestAsyncClusterMaintNotificationsBase
):
    """Base class for async maintenance notifications handling tests.

    Uses an autouse async fixture (instead of sync setup_method/teardown_method)
    because cluster creation requires ``await initialize()``.
    """

    @pytest.fixture(autouse=True)
    async def _setup_cluster(self):
        self.proxy_helper = ProxyInterceptorHelper()
        self.proxy_helper.cleanup_interceptors(CLUSTER_SLOTS_INTERCEPTOR_NAME)

        self.config = MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=True, relaxed_timeout=30
        )
        self.cluster = await self._create_cluster_client(maint_config=self.config)
        try:
            yield
        finally:
            await self.cluster.aclose()
            self.proxy_helper.cleanup_interceptors()


@dataclass
class ConnectionStateExpectation:
    """Holds expected connection state details for validation."""

    node_port: int
    changed_connections_count: int = 0
    state: MaintenanceState = MaintenanceState.NONE
    relaxed_timeout: Optional[int] = None


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestAsyncClusterMaintNotificationsHandling(
    TestAsyncClusterMaintNotificationsHandlingBase
):
    """Test maintenance notifications handling with async RedisCluster."""

    def _get_expected_node_state(
        self, expectations_list: List[ConnectionStateExpectation], node_port: int
    ) -> Optional[ConnectionStateExpectation]:
        for expectation in expectations_list:
            if expectation.node_port == node_port:
                return expectation
        return None

    def _validate_connections_states(
        self,
        cluster: "redis.RedisCluster",
        expected_states: List[ConnectionStateExpectation],
    ):
        """Validate per-node connection maintenance state / relaxed timeout."""
        default_maint_state = MaintenanceState.NONE
        default_timeout = None
        for node in list(cluster.nodes_manager.nodes_cache.values()):
            expected_state = self._get_expected_node_state(expected_states, node.port)
            if expected_state is None:
                continue
            changed_connections_count = 0
            for conn in _node_all_connections(node):
                if (
                    conn.maintenance_state != default_maint_state
                    and conn.maintenance_state == expected_state.state
                ) or (
                    conn.socket_timeout != default_timeout
                    and conn.socket_timeout == expected_state.relaxed_timeout
                ):
                    changed_connections_count += 1
            assert changed_connections_count == expected_state.changed_connections_count

    def _validate_removed_node_connections(self, node: ClusterNode):
        """Validate connections in a removed node are disconnected / for reconnect."""
        for conn in _node_free_connections(node):
            assert conn._sock is None
        for conn in _node_in_use_connections(node):
            assert conn.should_reconnect()

    async def test_receive_smigrating_notification(self):
        """Receiving an SMIGRATING notification relaxes timeout on one connection."""
        await self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        notification = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 123,456,5000-7000"
        )
        self.proxy_helper.send_notification(notification)

        # validate no timeout is relaxed on any connection yet
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(NODE_PORT_1, 0),
                ConnectionStateExpectation(NODE_PORT_2, 0),
                ConnectionStateExpectation(NODE_PORT_3, 0),
            ],
        )

        # execute a command that will receive the notification
        res = await self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        # SMIGRATING is handled at the connection level (synchronously while reading
        # the response) - no background task draining needed here.
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(NODE_PORT_2, 0),
                ConnectionStateExpectation(NODE_PORT_3, 0),
            ],
        )

    async def test_receive_smigrating_with_disabled_relaxed_timeout(self):
        """SMIGRATING with relaxed_timeout disabled does not change any connection."""
        disabled_config = MaintNotificationsConfig(
            enabled="auto",
            relaxed_timeout=-1,  # disabled
        )
        cluster = await self._create_cluster_client(maint_config=disabled_config)
        try:
            await self._warm_up_connection_pools(cluster, created_connections_count=3)

            notification = RespTranslator.oss_maint_notification_to_resp(
                "SMIGRATING 12 123,456,5000-7000"
            )
            self.proxy_helper.send_notification(notification)

            await cluster.set("anyprefix:{3}:k", "VAL")

            self._validate_connections_states(
                cluster,
                [
                    ConnectionStateExpectation(NODE_PORT_1, 0),
                    ConnectionStateExpectation(NODE_PORT_2, 0),
                    ConnectionStateExpectation(NODE_PORT_3, 0),
                ],
            )
        finally:
            await cluster.aclose()

    async def test_receive_smigrated_notification(self):
        """Receiving an SMIGRATED notification updates the cluster topology."""
        await self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange(NODE_IP_PROXY, NODE_PORT_NEW, 0, 5460),
                SlotsRange(NODE_IP_PROXY, NODE_PORT_2, 5461, 10922),
                SlotsRange(NODE_IP_PROXY, NODE_PORT_3, 10923, 16383),
            ],
        )
        notification = RespTranslator.oss_maint_notification_to_resp(
            f"SMIGRATED 12 {NODE_IP_PROXY}:{NODE_PORT_1} "
            f"{NODE_IP_PROXY}:{NODE_PORT_2} 123,456,5000-7000"
        )
        self.proxy_helper.send_notification(notification)

        # execute a command that will receive the notification
        res = await self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        # SMIGRATED is handled in a background task in the async client
        await self._drain_maint_notification_tasks(self.cluster)

        new_node = self.cluster.nodes_manager.get_node(
            host=NODE_IP_PROXY, port=NODE_PORT_NEW
        )
        assert new_node is not None

    async def test_receive_smigrated_notification_with_two_nodes(self):
        """SMIGRATED affecting two destination nodes updates the topology."""
        await self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange(NODE_IP_PROXY, NODE_PORT_NEW, 0, 5460),
                SlotsRange(NODE_IP_PROXY, NODE_PORT_2, 5461, 10922),
                SlotsRange(NODE_IP_PROXY, NODE_PORT_3, 10923, 16383),
            ],
        )
        notification = RespTranslator.oss_maint_notification_to_resp(
            f"SMIGRATED 12 {NODE_IP_PROXY}:{NODE_PORT_1} "
            f"{NODE_IP_PROXY}:{NODE_PORT_2} 123,456,5000-7000 "
            f"{NODE_IP_PROXY}:{NODE_PORT_1} {NODE_IP_PROXY}:{NODE_PORT_NEW} 110-120"
        )
        self.proxy_helper.send_notification(notification)

        res = await self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        await self._drain_maint_notification_tasks(self.cluster)

        new_node = self.cluster.nodes_manager.get_node(
            host=NODE_IP_PROXY, port=NODE_PORT_NEW
        )
        assert new_node is not None

    async def test_smigrating_smigrated_on_the_same_node_two_slot_ranges(self):
        """Second in-progress migration must not unrelax the timeouts early."""
        await self._warm_up_connection_pools(self.cluster, created_connections_count=1)

        smigrating_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 1000-2000,2500-3000"
        )
        self.proxy_helper.send_notification(smigrating_node_1)
        await self.cluster.set("anyprefix:{3}:k", "VAL")
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )

        smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
            f"SMIGRATED 14 {NODE_IP_PROXY}:{NODE_PORT_1} "
            f"{NODE_IP_PROXY}:{NODE_PORT_2} 1000-2000 "
            f"{NODE_IP_PROXY}:{NODE_PORT_1} {NODE_IP_PROXY}:{NODE_PORT_3} 2500-3000"
        )
        self.proxy_helper.send_notification(smigrated_node_1)
        await self.cluster.set("anyprefix:{3}:k", "VAL")
        await self._drain_maint_notification_tasks(self.cluster)

        # second migration still in progress -> timeout remains relaxed
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(NODE_PORT_1),
            ],
        )

        smigrated_node_1_2 = RespTranslator.oss_maint_notification_to_resp(
            f"SMIGRATED 15 {NODE_IP_PROXY}:{NODE_PORT_1} "
            f"{NODE_IP_PROXY}:{NODE_PORT_3} 3000-4000"
        )
        self.proxy_helper.send_notification(smigrated_node_1_2)
        await self.cluster.set("anyprefix:{3}:k", "VAL")
        await self._drain_maint_notification_tasks(self.cluster)
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    NODE_PORT_1,
                    changed_connections_count=0,
                ),
            ],
        )

    async def test_smigrated_node_replacement_resets_topology_connection_configs(self):
        """End-to-end node replacement over the proxy with three warmed-up nodes.

        Start with three nodes, each with 3 connections created and used. An
        SMIGRATING notification relaxes a connection on NODE_PORT_1, then an
        SMIGRATED notification migrates all of NODE_PORT_1's slots to
        NODE_PORT_NEW - so NODE_PORT_1 leaves the topology and NODE_PORT_NEW joins.

        After the async handler drains, validate the end result:
          * the topology was updated (NODE_PORT_NEW in, NODE_PORT_1 out);
          * every node remaining in the topology has its connections back at the
            default (non-relaxed) config - i.e. no connection is left in
            MAINTENANCE / with a relaxed timeout;
          * the replaced node's connections were cleaned up (each is either
            disconnected or marked for reconnect, so none can serve a command
            with the stale relaxed timeout).
        """
        await self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        # Grab the node that will be replaced before it leaves the topology.
        removed_node = self.cluster.nodes_manager.get_node(
            host=NODE_IP_PROXY, port=NODE_PORT_1
        )
        assert removed_node is not None
        assert len(_node_all_connections(removed_node)) == 3

        # 1) SMIGRATING relaxes a connection on NODE_PORT_1 (the migrating source).
        self.proxy_helper.send_notification(
            RespTranslator.oss_maint_notification_to_resp(
                "SMIGRATING 12 123,456,5000-7000"
            )
        )
        assert await self.cluster.set("anyprefix:{3}:k", "VAL") is True
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(NODE_PORT_2, 0),
                ConnectionStateExpectation(NODE_PORT_3, 0),
            ],
        )

        # 2) SMIGRATED migrates all of NODE_PORT_1's slots to NODE_PORT_NEW, so
        # NODE_PORT_1 is dropped from the topology and NODE_PORT_NEW is added.
        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange(NODE_IP_PROXY, NODE_PORT_NEW, 0, 5460),
                SlotsRange(NODE_IP_PROXY, NODE_PORT_2, 5461, 10922),
                SlotsRange(NODE_IP_PROXY, NODE_PORT_3, 10923, 16383),
            ],
        )
        self.proxy_helper.send_notification(
            RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATED 13 {NODE_IP_PROXY}:{NODE_PORT_1} "
                f"{NODE_IP_PROXY}:{NODE_PORT_NEW} 0-5460"
            )
        )
        assert await self.cluster.set("anyprefix:{3}:k", "VAL") is True
        await self._drain_maint_notification_tasks(self.cluster)

        # Topology updated: new node joined, replaced node gone.
        assert (
            self.cluster.nodes_manager.get_node(host=NODE_IP_PROXY, port=NODE_PORT_NEW)
            is not None
        )
        assert (
            self.cluster.nodes_manager.get_node(host=NODE_IP_PROXY, port=NODE_PORT_1)
            is None
        )

        # Every node remaining in the topology is back at the default config -
        # no connection left relaxed / in MAINTENANCE.
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(NODE_PORT_NEW, 0),
                ConnectionStateExpectation(NODE_PORT_2, 0),
                ConnectionStateExpectation(NODE_PORT_3, 0),
            ],
        )

        # The replaced node's connections were cleaned up: each is either
        # disconnected (free connections) or marked for reconnect (in-use), so
        # none can be reused with the stale relaxed timeout.
        for conn in _node_all_connections(removed_node):
            assert (not conn.is_connected) or conn.should_reconnect()
