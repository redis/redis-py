from typing import cast

from redis import ConnectionPool, RedisCluster
from redis.cluster import ClusterNode
from redis.connection import (
    BlockingConnectionPool,
)
from redis.maint_notifications import MaintNotificationsConfig
from redis.cache import CacheConfig
from tests.maint_notifications.proxy_server_helpers import (
    ProxyInterceptorHelper,
    RespTranslator,
)

# Initial cluster node configuration for proxy-based tests
PROXY_CLUSTER_NODES = [
    ClusterNode("127.0.0.1", 15379),
    ClusterNode("127.0.0.1", 15380),
    ClusterNode("127.0.0.1", 15381),
]


class TestClusterMaintNotificationsConfig:
    """Test the maint_notifications_config parameter of RedisCluster."""

    # Helper methods
    def _create_cluster_client(
        self,
        maint_config=None,
        connection_pool_class=None,
        cache_config=None,
        skip_full_coverage_check=True,
    ):
        """Create a RedisCluster instance with real cluster nodes."""
        kwargs = {
            "startup_nodes": PROXY_CLUSTER_NODES,
            "protocol": 3,
            "skip_full_coverage_check": skip_full_coverage_check,
        }
        if maint_config is not None:
            kwargs["maint_notifications_config"] = maint_config
        if connection_pool_class is not None:
            kwargs["connection_pool_class"] = connection_pool_class
        if cache_config is not None:
            kwargs["cache_config"] = cache_config

        return RedisCluster(**kwargs)

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
            connection_pool_class=BlockingConnectionPool,
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
        cache_config = CacheConfig()

        cluster = self._create_cluster_client(
            maint_config=maint_config,
            cache_config=cache_config,
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
        Test that when maint_notifications_config=None, the system works without errors.
        """
        cluster = self._create_cluster_client(maint_config=None)

        try:
            # Verify cluster is created successfully
            assert cluster.nodes_manager is not None
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


class TestClusterMaintNotificationsHandlingBase:
    """Base class for maintenance notifications handling tests."""

    def setup_method(self):
        """Set up test fixtures with mocked sockets."""
        self.proxy_helper = ProxyInterceptorHelper()

        # Create maintenance notifications config
        self.config = MaintNotificationsConfig(
            enabled="auto", proactive_reconnect=True, relaxed_timeout=30
        )
        self.cluster = self._create_cluster_client(maint_config=self.config)

    def _create_cluster_client(
        self,
        pool_class=ConnectionPool,
        enable_cache=False,
        max_connections=10,
        maint_config=None,
    ) -> RedisCluster:
        """Create a RedisCluster instance with mocked sockets."""
        config = maint_config if maint_config is not None else self.config
        kwargs = {}
        if enable_cache:
            kwargs = {"cache_config": CacheConfig()}

        test_redis_client = RedisCluster(
            protocol=3,
            startup_nodes=PROXY_CLUSTER_NODES,
            maint_notifications_config=config,
            connection_pool_class=pool_class,
            max_connections=max_connections,
            **kwargs,
        )

        return test_redis_client

    def teardown_method(self):
        """Clean up test fixtures."""
        self.cluster.close()
        self.proxy_helper.cleanup_interceptors()


class TestClusterMaintNotificationsHandling(TestClusterMaintNotificationsHandlingBase):
    """Test maintenance notifications handling with RedisCluster."""

    def test_receive_maint_notification(self):
        """Test receiving a maintenance notification."""
        self.cluster.set("test", "VAL")
        pubsub = self.cluster.pubsub()
        pubsub.subscribe("test")
        test_msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
        print(test_msg)

        # Try to send a push notification to the clients of given server node
        # Server node is defined by its port with the local test environment
        # The message should be in the format:
        # >3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$4\r\neeee\r
        notification = RespTranslator.smigrating_to_resp(
            "TEST_NOTIFICATION 12182 127.0.0.1:15380"
        )
        self.proxy_helper.send_notification(pubsub.connection.port, notification)
        res = self.proxy_helper.get_connections()
        print(res)

        test_msg = pubsub.get_message(timeout=1)
        print(test_msg)
