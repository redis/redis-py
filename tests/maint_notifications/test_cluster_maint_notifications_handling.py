from dataclasses import dataclass
from typing import List, Optional, cast

from redis import ConnectionPool, RedisCluster
from redis.cluster import ClusterNode
from redis.connection import (
    BlockingConnectionPool,
)
from redis.maint_notifications import MaintNotificationsConfig, MaintenanceState
from redis.cache import CacheConfig
from tests.maint_notifications.proxy_server_helpers import (
    ProxyInterceptorHelper,
    RespTranslator,
    SlotsRange,
)

NODE_PORT_1 = 15379
NODE_PORT_2 = 15380
NODE_PORT_3 = 15381

NODE_PORT_NEW = 15382

# Initial cluster node configuration for proxy-based tests
PROXY_CLUSTER_NODES = [
    ClusterNode("127.0.0.1", NODE_PORT_1),
    ClusterNode("127.0.0.1", NODE_PORT_2),
    ClusterNode("127.0.0.1", NODE_PORT_3),
]

CLUSTER_SLOTS_INTERCEPTOR_NAME = "test_topology"


class TestRespTranslatorHelper:
    def test_oss_maint_notification_to_resp(self):
        resp = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 123,456,5000-7000"
        )
        assert resp == ">3\r\n+SMIGRATING\r\n:12\r\n+123,456,5000-7000\r\n"

        resp = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 12 127.0.0.1:15380 123,456,5000-7000"
        )
        assert (
            resp
            == ">3\r\n+SMIGRATED\r\n:12\r\n*1\r\n*2\r\n+127.0.0.1:15380\r\n+123,456,5000-7000\r\n"
        )
        resp = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 12 127.0.0.1:15380 123,456,5000-7000 127.0.0.1:15381 7000-8000 127.0.0.1:15382 8000-9000"
        )

        assert (
            resp
            == ">3\r\n+SMIGRATED\r\n:12\r\n*3\r\n*2\r\n+127.0.0.1:15380\r\n+123,456,5000-7000\r\n*2\r\n+127.0.0.1:15381\r\n+7000-8000\r\n*2\r\n+127.0.0.1:15382\r\n+8000-9000\r\n"
        )


class TestClusterMaintNotificationsBase:
    """Base class for cluster maintenance notifications handling tests."""

    def _create_cluster_client(
        self,
        pool_class=ConnectionPool,
        enable_cache=False,
        max_connections=10,
        maint_config=None,
        protocol=3,
    ) -> RedisCluster:
        """Create a RedisCluster instance with mocked sockets."""
        if maint_config is None and hasattr(self, "config") and self.config is not None:
            maint_config = self.config

        kwargs = {}
        if enable_cache:
            kwargs = {"cache_config": CacheConfig()}

        test_redis_client = RedisCluster(
            protocol=protocol,
            startup_nodes=PROXY_CLUSTER_NODES,
            maint_notifications_config=maint_config,
            connection_pool_class=pool_class,
            max_connections=max_connections,
            **kwargs,
        )

        return test_redis_client


class TestClusterMaintNotificationsConfig(TestClusterMaintNotificationsBase):
    """Test the maint_notifications_config parameter of RedisCluster."""

    def _validate_maint_config_on_nodes_manager(
        self,
        cluster: RedisCluster,
        expected_enabled: bool,
        expected_proactive_reconnect: bool,
        expected_relaxed_timeout: int,
    ) -> None:
        """Validate maint_notifications_config on NodesManager."""
        assert cluster.nodes_manager.maint_notifications_config is not None
        assert (
            cluster.nodes_manager.maint_notifications_config.enabled == expected_enabled
        )
        assert (
            cluster.nodes_manager.maint_notifications_config.proactive_reconnect
            == expected_proactive_reconnect
        )
        assert (
            cluster.nodes_manager.maint_notifications_config.relaxed_timeout
            == expected_relaxed_timeout
        )

    def _validate_maint_config_on_nodes(
        self,
        cluster: RedisCluster,
        expected_enabled: bool,
        expected_proactive_reconnect: bool,
        expected_relaxed_timeout: int,
        should_have_handler: bool = True,
    ) -> None:
        """Validate maint_notifications_config on individual nodes."""
        nodes = list(cluster.nodes_manager.nodes_cache.values())
        assert len(nodes) > 0, "Cluster should have at least one node"

        for node in nodes:
            cluster_node = cast(ClusterNode, node)
            assert cluster_node.redis_connection is not None
            connection_pool = cluster_node.redis_connection.connection_pool
            assert connection_pool is not None

            if should_have_handler:
                if hasattr(connection_pool, "_maint_notifications_pool_handler"):
                    handler = connection_pool._maint_notifications_pool_handler
                    if handler is not None:
                        assert handler.config.enabled == expected_enabled
                        assert (
                            handler.config.proactive_reconnect
                            == expected_proactive_reconnect
                        )
                        assert (
                            handler.config.relaxed_timeout == expected_relaxed_timeout
                        )

    def test_maint_notifications_config(self):
        """
        Test that maint_notifications_config is passed to NodesManager and nodes.

        Creates a RedisCluster instance with 3 real startup nodes and validates
        that the maint_notifications_config is properly set on both the NodesManager
        and the individual nodes.
        """
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=True, relaxed_timeout=30
        )

        cluster = self._create_cluster_client(maint_config=maint_config)

        try:
            self._validate_maint_config_on_nodes_manager(cluster, False, True, 30)
            self._validate_maint_config_on_nodes(cluster, False, True, 30)

            # Verify we can execute commands without errors
            cluster.set("test", "VAL")
            res = cluster.get("test")
            assert res == b"VAL"
        finally:
            cluster.close()

    def test_config_propagation_to_new_nodes(self):
        """
        Test that when a new node is discovered/added to the cluster,
        it receives the same maint_notifications_config.
        """
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=True, relaxed_timeout=25
        )

        cluster = self._create_cluster_client(maint_config=maint_config)

        try:
            # Verify initial nodes have the config
            initial_node_count = len(cluster.nodes_manager.nodes_cache)
            self._validate_maint_config_on_nodes(cluster, False, True, 25)

            # Reinitialize to ensure all nodes are discovered
            cluster.nodes_manager.initialize()

            # Verify all nodes have the config
            new_node_count = len(cluster.nodes_manager.nodes_cache)
            assert new_node_count >= initial_node_count
            self._validate_maint_config_on_nodes(cluster, False, True, 25)
        finally:
            cluster.close()

    def test_config_with_blocking_connection_pool(self):
        """
        Test that maint_notifications_config works with BlockingConnectionPool.
        """
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=True, relaxed_timeout=20
        )

        cluster = self._create_cluster_client(
            maint_config=maint_config,
            pool_class=BlockingConnectionPool,
        )

        try:
            # Verify config is set on NodesManager
            self._validate_maint_config_on_nodes_manager(cluster, False, True, 20)

            # Verify config is set on nodes
            self._validate_maint_config_on_nodes(cluster, False, True, 20)

            # Verify we can execute commands without errors
            cluster.set("test", "VAL")
            res = cluster.get("test")
            assert res == b"VAL"
        finally:
            cluster.close()

    def test_config_with_cache_enabled(self):
        """
        Test that maint_notifications_config works with caching enabled.
        """
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=True, relaxed_timeout=15
        )

        cluster = self._create_cluster_client(
            maint_config=maint_config,
            enable_cache=True,
        )

        try:
            self._validate_maint_config_on_nodes_manager(cluster, False, True, 15)
            self._validate_maint_config_on_nodes(cluster, False, True, 15)

            # Verify we can execute commands without errors
            cluster.set("test", "VAL")
            res = cluster.get("test")
            assert res == b"VAL"
        finally:
            cluster.close()

    def test_none_config_default_behavior(self):
        """
        Test that when maint_notifications_config=None, it will be initialized with default values.
        """
        cluster = self._create_cluster_client(maint_config=None)

        try:
            # Verify cluster is created successfully
            assert cluster.nodes_manager is not None
            # for protocol 3, maint_notifications_config should be initialized with default values
            assert cluster.nodes_manager.maint_notifications_config is not None
            assert cluster.nodes_manager.maint_notifications_config.enabled == "auto"
            assert len(cluster.nodes_manager.nodes_cache) > 0
            # Verify we can execute commands without errors
            cluster.set("test", "VAL")
            res = cluster.get("test")
            assert res == b"VAL"
        finally:
            cluster.close()

    def test_none_config_default_behavior_for_protocol_2(self):
        """
        Test that when maint_notifications_config=None and protocol=2,
        it will not be initialized.
        """
        cluster = self._create_cluster_client(protocol=2)

        try:
            # Verify cluster is created successfully
            assert cluster.nodes_manager is not None
            # for protocol 2, maint_notifications_config should not be created
            assert cluster.nodes_manager.maint_notifications_config is None

            assert len(cluster.nodes_manager.nodes_cache) > 0
            # Verify we can execute commands without errors
            cluster.set("test", "VAL")
            res = cluster.get("test")
            assert res == b"VAL"
        finally:
            cluster.close()

    def test_config_with_enabled_false(self):
        """
        Test that when enabled=False, maint notifications handlers are not created/initialized.
        """
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=False, relaxed_timeout=-1
        )

        cluster = self._create_cluster_client(maint_config=maint_config)

        try:
            self._validate_maint_config_on_nodes_manager(cluster, False, False, -1)
            # When enabled=False, handlers should not be created
            self._validate_maint_config_on_nodes(
                cluster, False, False, -1, should_have_handler=False
            )

            # Verify we can execute commands without errors
            cluster.set("test", "VAL")
            res = cluster.get("test")
            assert res == b"VAL"
        finally:
            cluster.close()

    def test_config_with_pipeline_operations(self):
        """
        Test that maint_notifications_config works with pipelined commands.
        """
        maint_config = MaintNotificationsConfig(
            enabled=False, proactive_reconnect=True, relaxed_timeout=10
        )

        cluster = self._create_cluster_client(maint_config=maint_config)

        try:
            self._validate_maint_config_on_nodes_manager(cluster, False, True, 10)
            self._validate_maint_config_on_nodes(cluster, False, True, 10)

            # Verify pipeline operations work without errors
            pipe = cluster.pipeline()
            pipe.set("pipe_key1", "value1")
            pipe.set("pipe_key2", "value2")
            pipe.get("pipe_key1")
            pipe.get("pipe_key2")
            results = pipe.execute()

            # Verify pipeline results
            assert results[0] is True or results[0] == b"OK"  # SET returns True or OK
            assert results[1] is True or results[1] == b"OK"  # SET returns True or OK
            assert results[2] == b"value1"  # GET returns value
            assert results[3] == b"value2"  # GET returns value
        finally:
            cluster.close()


class TestClusterMaintNotificationsHandler(TestClusterMaintNotificationsBase):
    """Test OSSMaintNotificationsHandler propagation with RedisCluster."""

    def _validate_connection_handlers(
        self, conn, cluster_client, config, is_cache_conn=False
    ):
        """Helper method to validate connection handlers are properly set."""
        # Test that the oss cluster handler function is correctly set
        oss_cluster_parser_handler_set_for_con = (
            conn._parser.oss_cluster_maint_push_handler_func
        )
        assert oss_cluster_parser_handler_set_for_con is not None
        assert hasattr(oss_cluster_parser_handler_set_for_con, "__self__")
        assert hasattr(oss_cluster_parser_handler_set_for_con, "__func__")
        assert oss_cluster_parser_handler_set_for_con.__self__.connection is conn
        assert (
            oss_cluster_parser_handler_set_for_con.__self__.cluster_client
            is cluster_client
        )
        assert (
            oss_cluster_parser_handler_set_for_con.__self__._lock
            is cluster_client._oss_cluster_maint_notifications_handler._lock
        )
        assert (
            oss_cluster_parser_handler_set_for_con.__self__._processed_notifications
            is cluster_client._oss_cluster_maint_notifications_handler._processed_notifications
        )
        assert (
            oss_cluster_parser_handler_set_for_con.__func__
            is cluster_client._oss_cluster_maint_notifications_handler.handle_notification.__func__
        )

        # Test that the maintenance handler function is correctly set
        parser_maint_handler_set_for_con = conn._parser.maintenance_push_handler_func
        assert parser_maint_handler_set_for_con is not None
        assert hasattr(parser_maint_handler_set_for_con, "__self__")
        assert hasattr(parser_maint_handler_set_for_con, "__func__")
        # The maintenance handler should be bound to the connection's
        # maintenance notification connection handler
        assert (
            parser_maint_handler_set_for_con.__self__
            is conn._maint_notifications_connection_handler
        )
        assert (
            parser_maint_handler_set_for_con.__func__
            is conn._maint_notifications_connection_handler.handle_notification.__func__
        )

        # Validate that the connection's maintenance handler has the same config object
        assert conn._maint_notifications_connection_handler.config is config

    def test_oss_maint_handler_propagation(self):
        """Test that OSSMaintNotificationsHandler is propagated to all connections."""
        cluster = self._create_cluster_client()
        # Verify all nodes have the handler
        for node in cluster.nodes_manager.nodes_cache.values():
            assert node.redis_connection is not None
            assert node.redis_connection.connection_pool is not None
            for conn in (
                *node.redis_connection.connection_pool._get_in_use_connections(),
                *node.redis_connection.connection_pool._get_free_connections(),
            ):
                assert conn._oss_cluster_maint_notifications_handler is not None
                assert conn._oss_cluster_maint_notifications_handler.connection is conn
                self._validate_connection_handlers(
                    conn, cluster, cluster.maint_notifications_config
                )

    def test_oss_maint_handler_propagation_cache_enabled(self):
        """Test that OSSMaintNotificationsHandler is propagated to all connections."""
        cluster = self._create_cluster_client(enable_cache=True)
        # Verify all nodes have the handler
        for node in cluster.nodes_manager.nodes_cache.values():
            assert node.redis_connection is not None
            assert node.redis_connection.connection_pool is not None
            for conn in (
                *node.redis_connection.connection_pool._get_in_use_connections(),
                *node.redis_connection.connection_pool._get_free_connections(),
            ):
                assert conn._conn._oss_cluster_maint_notifications_handler is not None
                assert (
                    conn._conn._oss_cluster_maint_notifications_handler.connection
                    is conn._conn
                )
                self._validate_connection_handlers(
                    conn._conn, cluster, cluster.maint_notifications_config
                )


class TestClusterMaintNotificationsHandlingBase(TestClusterMaintNotificationsBase):
    """Base class for maintenance notifications handling tests."""

    def setup_method(self):
        """Set up test fixtures with mocked sockets."""
        self.proxy_helper = ProxyInterceptorHelper()
        self.proxy_helper.cleanup_interceptors(CLUSTER_SLOTS_INTERCEPTOR_NAME)

        # Create maintenance notifications config
        self.config = MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=True, relaxed_timeout=30
        )
        self.cluster = self._create_cluster_client(maint_config=self.config)

    def teardown_method(self):
        """Clean up test fixtures."""
        self.cluster.close()
        # interceptors that are changed during the tests are collected in the proxy helper
        self.proxy_helper.cleanup_interceptors()


@dataclass
class ConnectionStateExpectation:
    """Data class to hold connection state details for validation."""

    node_port: int
    changed_connections_count: int = 0
    state: MaintenanceState = MaintenanceState.NONE
    relaxed_timeout: Optional[int] = None


class TestClusterMaintNotificationsHandling(TestClusterMaintNotificationsHandlingBase):
    """Test maintenance notifications handling with RedisCluster."""

    def _warm_up_connection_pools(
        self, cluster: RedisCluster, created_connections_count: int = 3
    ):
        """Warm up connection pools by getting a connection from each pool."""
        for node in cluster.nodes_manager.nodes_cache.values():
            node_connections = []
            for _ in range(created_connections_count):
                node_connections.append(
                    node.redis_connection.connection_pool.get_connection()
                )
            for conn in node_connections:
                node.redis_connection.connection_pool.release(conn)

            node_connections.clear()

    def _get_expected_node_state(
        self, expectations_list: List[ConnectionStateExpectation], node_port: int
    ) -> Optional[ConnectionStateExpectation]:
        """Get the expected state for a node."""
        for expectation in expectations_list:
            if expectation.node_port == node_port:
                return expectation
        return None

    def _validate_connections_states(
        self,
        cluster: RedisCluster,
        expected_states: List[ConnectionStateExpectation],
    ):
        """Validate connections states."""
        default_maint_state = MaintenanceState.NONE
        default_timeout = None
        nodes = list(cluster.nodes_manager.nodes_cache.values())
        for node in nodes:
            cluster_node = cast(ClusterNode, node)
            assert cluster_node.redis_connection is not None
            connection_pool = cluster_node.redis_connection.connection_pool
            assert connection_pool is not None
            expected_state = self._get_expected_node_state(
                expected_states, cluster_node.port
            )
            if expected_state is None:
                # No expectation for this node
                continue
            changed_connections_count = 0
            for conn in (
                *connection_pool._get_in_use_connections(),
                *connection_pool._get_free_connections(),
            ):
                if (
                    conn.maintenance_state != default_maint_state
                    and conn.maintenance_state == expected_state.state
                ) or (
                    conn.socket_timeout != default_timeout
                    and conn.socket_timeout == expected_state.relaxed_timeout
                ):
                    changed_connections_count += 1
            assert changed_connections_count == expected_state.changed_connections_count

    def _validate_removed_node_connections(self, node):
        """Validate connections in a removed node."""
        assert node.redis_connection is not None
        connection_pool = node.redis_connection.connection_pool
        assert connection_pool is not None

        # validate all connections are disconnected or marked for reconnect
        for conn in connection_pool._get_free_connections():
            assert conn._sock is None
        for conn in connection_pool._get_in_use_connections():
            assert conn.should_reconnect()

    def test_receive_smigrating_notification(self):
        """Test receiving an OSS maintenance notification."""
        # warm up connection pools
        self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        # send a notification to node 1
        notification = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 123,456,5000-7000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, notification)

        # validate no timeout is relaxed on any connection
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1, changed_connections_count=0
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2, changed_connections_count=0
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_3, changed_connections_count=0
                ),
            ],
        )

        # execute a command that will receive the notification
        res = self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        # validate the timeout was relaxed on just one connection for the node
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2, changed_connections_count=0
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_3, changed_connections_count=0
                ),
            ],
        )

    def test_receive_smigrating_with_disabled_relaxed_timeout(self):
        """Test receiving an OSS maintenance notification with disabled relaxed timeout."""
        # Create config with disabled relaxed timeout
        disabled_config = MaintNotificationsConfig(
            enabled="auto",
            relaxed_timeout=-1,  # This means the relaxed timeout is Disabled
        )
        cluster = self._create_cluster_client(maint_config=disabled_config)

        # warm up connection pools
        self._warm_up_connection_pools(cluster, created_connections_count=3)

        # send a notification to node 1
        notification = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 123,456,5000-7000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, notification)

        # validate no timeout is relaxed on any connection
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1, changed_connections_count=0
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2, changed_connections_count=0
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_3, changed_connections_count=0
                ),
            ],
        )

    def test_receive_smigrated_notification(self):
        """Test receiving an OSS maintenance completed notification."""
        # create three connections in each node's connection pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", NODE_PORT_NEW, 0, 5460),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5461, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )
        # send a notification to node 1
        notification = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 12 127.0.0.1:15380 123,456,5000-7000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, notification)

        # execute a command that will receive the notification
        res = self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        # validate the cluster topology was updated
        new_node = self.cluster.nodes_manager.get_node(
            host="0.0.0.0", port=NODE_PORT_NEW
        )
        assert new_node is not None

    def test_receive_smigrated_notification_with_two_nodes(self):
        """Test receiving an OSS maintenance completed notification."""
        # create three connections in each node's connection pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", NODE_PORT_NEW, 0, 5460),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5461, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )
        # send a notification to node 1
        notification = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 12 127.0.0.1:15380 123,456,5000-7000 127.0.0.1:15382 110-120"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, notification)

        # execute a command that will receive the notification
        res = self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        # validate the cluster topology was updated
        new_node = self.cluster.nodes_manager.get_node(
            host="0.0.0.0", port=NODE_PORT_NEW
        )
        assert new_node is not None

    def test_smigrating_smigrated_on_two_nodes_without_node_replacement(self):
        """Test receiving an OSS maintenance notification on two nodes without node replacement."""
        # warm up connection pools - create several connections in each pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        node_1 = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_1)
        node_2 = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_2)

        smigrating_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 123,2000-3000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrating_node_1)
        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2, changed_connections_count=0
                ),
            ],
        )

        smigrating_node_2 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 13 8000-9000"
        )
        self.proxy_helper.send_notification(NODE_PORT_2, smigrating_node_2)

        # execute command with node 2 connection
        self.cluster.set("anyprefix:{1}:k", "VAL")

        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )
        smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 14 0.0.0.0:15381 123,2000-3000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrated_node_1)

        smigrated_node_2 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 15 0.0.0.0:15381 8000-9000"
        )
        self.proxy_helper.send_notification(NODE_PORT_2, smigrated_node_2)

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", NODE_PORT_1, 0, 122),
                SlotsRange("0.0.0.0", NODE_PORT_3, 123, 123),
                SlotsRange("0.0.0.0", NODE_PORT_1, 124, 2000),
                SlotsRange("0.0.0.0", NODE_PORT_3, 2001, 3000),
                SlotsRange("0.0.0.0", NODE_PORT_1, 3001, 5460),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5461, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )

        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")

        # validate the cluster topology was updated
        # validate old nodes are there
        assert node_1 in self.cluster.nodes_manager.nodes_cache.values()
        assert node_2 in self.cluster.nodes_manager.nodes_cache.values()
        # validate changed slot is assigned to node 3
        assert self.cluster.nodes_manager.get_node_from_slot(
            123
        ) == self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_3)
        # validate the connections are in the correct state
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=0,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", NODE_PORT_1, 0, 122),
                SlotsRange("0.0.0.0", NODE_PORT_3, 123, 123),
                SlotsRange("0.0.0.0", NODE_PORT_1, 124, 2000),
                SlotsRange("0.0.0.0", NODE_PORT_3, 2001, 3000),
                SlotsRange("0.0.0.0", NODE_PORT_1, 3001, 5460),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5461, 7000),
                SlotsRange("0.0.0.0", NODE_PORT_3, 7001, 8000),
                SlotsRange("0.0.0.0", NODE_PORT_2, 8001, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )
        # execute command with node 2 connection
        self.cluster.set("anyprefix:{1}:k", "VAL")

        # validate old nodes are there
        assert node_1 in self.cluster.nodes_manager.nodes_cache.values()
        assert node_2 in self.cluster.nodes_manager.nodes_cache.values()
        # validate slot changes are reflected
        assert self.cluster.nodes_manager.get_node_from_slot(
            8000
        ) == self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_3)

        # validate the connections are in the correct state
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=0,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2,
                    changed_connections_count=0,
                ),
            ],
        )

    def test_smigrating_smigrated_on_two_nodes_with_node_replacements(self):
        """Test receiving an OSS maintenance notification on two nodes with node replacement."""
        # warm up connection pools - create several connections in each pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=3)

        node_1 = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_1)
        node_2 = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_2)
        node_3 = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_3)

        smigrating_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 0-5460"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrating_node_1)
        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2, changed_connections_count=0
                ),
            ],
        )

        smigrating_node_2 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 13 5461-10922"
        )
        self.proxy_helper.send_notification(NODE_PORT_2, smigrating_node_2)

        # execute command with node 2 connection
        self.cluster.set("anyprefix:{1}:k", "VAL")

        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )

        smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 14 0.0.0.0:15382 0-5460"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrated_node_1)

        smigrated_node_2 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 15 0.0.0.0:15382 5461-10922"
        )
        self.proxy_helper.send_notification(NODE_PORT_2, smigrated_node_2)
        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", 15382, 0, 5460),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5461, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )

        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")

        # validate node 1 is removed
        assert node_1 not in self.cluster.nodes_manager.nodes_cache.values()
        # validate node 2 is still there
        assert node_2 in self.cluster.nodes_manager.nodes_cache.values()
        # validate new node is added
        new_node = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=15382)
        assert new_node is not None
        assert new_node.redis_connection is not None
        # validate a slot from the changed range is assigned to the new node
        assert self.cluster.nodes_manager.get_node_from_slot(
            123
        ) == self.cluster.nodes_manager.get_node(host="0.0.0.0", port=15382)

        # validate the connections are in the correct state
        self._validate_removed_node_connections(node_1)

        # validate the connections are in the correct state
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_2,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", 15382, 0, 5460),
                SlotsRange("0.0.0.0", 15383, 5461, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )
        # execute command with node 2 connection
        self.cluster.set("anyprefix:{1}:k", "VAL")

        # validate node 2 is removed
        assert node_2 not in self.cluster.nodes_manager.nodes_cache.values()
        # validate node 3 is still there
        assert node_3 in self.cluster.nodes_manager.nodes_cache.values()
        # validate new node is added
        new_node = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=15383)
        assert new_node is not None
        assert new_node.redis_connection is not None
        # validate a slot from the changed range is assigned to the new node
        assert self.cluster.nodes_manager.get_node_from_slot(
            8000
        ) == self.cluster.nodes_manager.get_node(host="0.0.0.0", port=15383)

        # validate the connections in removed node are in the correct state
        self._validate_removed_node_connections(node_2)

    def test_smigrating_smigrated_on_the_same_node_two_slot_ranges(
        self,
    ):
        """
        Test receiving an OSS maintenance notification on the same node twice.
        The focus here is to validate that the timeouts are not unrelaxed if a second
        migration is in progress
        """
        # warm up connection pools - create several connections in each pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=1)

        smigrating_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 1000-2000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrating_node_1)
        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )

        smigrating_node_1_2 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 13 3000-4000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrating_node_1_2)
        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )
        smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 14 0.0.0.0:15380 1000-2000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrated_node_1)
        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")
        # this functionality is part of CAE-1038 and will be added later
        # validate the timeout is still relaxed
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=1,
                    state=MaintenanceState.MAINTENANCE,
                    relaxed_timeout=self.config.relaxed_timeout,
                ),
            ],
        )
        smigrated_node_1_2 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 15 0.0.0.0:15381 3000-4000"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrated_node_1_2)
        # execute command with node 1 connection
        self.cluster.set("anyprefix:{3}:k", "VAL")
        self._validate_connections_states(
            self.cluster,
            [
                ConnectionStateExpectation(
                    node_port=NODE_PORT_1,
                    changed_connections_count=0,
                ),
            ],
        )

    def test_smigrating_smigrated_with_sharded_pubsub(
        self,
    ):
        """
        Test handling of sharded pubsub connections when SMIGRATING and SMIGRATED
        notifications are received.
        """
        # warm up connection pools - create several connections in each pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=5)

        node_1 = self.cluster.nodes_manager.get_node(host="0.0.0.0", port=NODE_PORT_1)

        pubsub = self.cluster.pubsub()

        # subscribe to a channel on node1
        pubsub.ssubscribe("anyprefix:{7}:k")

        msg = pubsub.get_sharded_message(
            ignore_subscribe_messages=False, timeout=10, target_node=node_1
        )
        # subscribe msg
        assert msg is not None and msg["type"] == "ssubscribe"

        smigrating_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 5200-5460"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrating_node_1)

        # get message with node 1 connection to consume the notification
        # timeout is 1 second
        msg = pubsub.get_sharded_message(ignore_subscribe_messages=False, timeout=5000)
        # smigrating handled
        assert msg is None

        assert pubsub.node_pubsub_mapping[node_1.name].connection._sock is not None
        assert pubsub.node_pubsub_mapping[node_1.name].connection._socket_timeout == 30
        assert (
            pubsub.node_pubsub_mapping[node_1.name].connection._socket_connect_timeout
            == 30
        )

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", NODE_PORT_1, 0, 5200),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5201, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )

        smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 14 0.0.0.0:15380 5200-5460"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrated_node_1)
        # execute command with node 1 connection
        # this will first consume the SMIGRATING notification for the connection
        # this should update the cluster topology and move the slot range to the new node
        # and should set the pubsub connection for reconnect
        res = self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        assert pubsub.node_pubsub_mapping[node_1.name].connection._should_reconnect
        assert pubsub.node_pubsub_mapping[node_1.name].connection._sock is not None
        assert (
            pubsub.node_pubsub_mapping[node_1.name].connection._socket_timeout is None
        )
        assert (
            pubsub.node_pubsub_mapping[node_1.name].connection._socket_connect_timeout
            is None
        )

        # first message will be SMIGRATED notification handling
        # during this read connection will be reconnected and will resubscribe to channels
        msg = pubsub.get_sharded_message(ignore_subscribe_messages=True, timeout=10)
        assert msg is None

        assert not pubsub.node_pubsub_mapping[node_1.name].connection._should_reconnect
        assert pubsub.node_pubsub_mapping[node_1.name].connection._sock is not None
        assert (
            pubsub.node_pubsub_mapping[node_1.name].connection._socket_timeout is None
        )
        assert (
            pubsub.node_pubsub_mapping[node_1.name].connection._socket_connect_timeout
            is None
        )
        assert (
            pubsub.node_pubsub_mapping[node_1.name].connection.maintenance_state
            == MaintenanceState.NONE
        )
        # validate resubscribed
        assert pubsub.node_pubsub_mapping[node_1.name].subscribed

    def test_smigrating_smigrated_with_std_pubsub(
        self,
    ):
        """
        Test handling of standard pubsub connections when SMIGRATING and SMIGRATED
        notifications are received.
        """
        # warm up connection pools - create several connections in each pool
        self._warm_up_connection_pools(self.cluster, created_connections_count=5)

        pubsub = self.cluster.pubsub()

        # subscribe to a channel on node1
        pubsub.subscribe("anyprefix:{7}:k")

        msg = pubsub.get_message(ignore_subscribe_messages=False, timeout=10)
        # subscribe msg
        assert msg is not None and msg["type"] == "subscribe"

        smigrating_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATING 12 5200-5460"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrating_node_1)

        # get message with node 1 connection to consume the notification
        # timeout is 1 second
        msg = pubsub.get_message(ignore_subscribe_messages=False, timeout=5000)
        # smigrating handled
        assert msg is None

        assert pubsub.connection._sock is not None
        assert pubsub.connection._socket_timeout == 30
        assert pubsub.connection._socket_connect_timeout == 30

        self.proxy_helper.set_cluster_slots(
            CLUSTER_SLOTS_INTERCEPTOR_NAME,
            [
                SlotsRange("0.0.0.0", NODE_PORT_1, 0, 5200),
                SlotsRange("0.0.0.0", NODE_PORT_2, 5201, 10922),
                SlotsRange("0.0.0.0", NODE_PORT_3, 10923, 16383),
            ],
        )

        smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
            "SMIGRATED 14 0.0.0.0:15380 5200-5460"
        )
        self.proxy_helper.send_notification(NODE_PORT_1, smigrated_node_1)
        # execute command with node 1 connection
        # this will first consume the SMIGRATING notification for the connection
        # this should update the cluster topology and move the slot range to the new node
        # and should set the pubsub connection for reconnect
        res = self.cluster.set("anyprefix:{3}:k", "VAL")
        assert res is True

        assert res is True

        assert pubsub.connection._should_reconnect
        assert pubsub.connection._sock is not None
        assert pubsub.connection._socket_timeout is None
        assert pubsub.connection._socket_connect_timeout is None

        # first message will be SMIGRATED notification handling
        # during this read connection will be reconnected and will resubscribe to channels
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
        assert msg is None

        assert not pubsub.connection._should_reconnect
        assert pubsub.connection._sock is not None
        assert pubsub.connection._socket_timeout is None
        assert pubsub.connection._socket_connect_timeout is None
        assert pubsub.connection.maintenance_state == MaintenanceState.NONE
        # validate resubscribed
        assert pubsub.subscribed
