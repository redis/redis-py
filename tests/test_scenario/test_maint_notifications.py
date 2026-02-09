"""Tests for Redis Enterprise moving push notifications with real cluster operations."""

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import random
from queue import Queue
from threading import Thread
import threading
import time
from typing import Any, Dict, List, Literal, Optional, Union

import pytest

from redis import Redis
from redis.connection import ConnectionInterface
from redis.maint_notifications import (
    EndpointType,
    MaintNotificationsConfig,
    MaintenanceState,
)
from tests.test_scenario.conftest import (
    CLIENT_TIMEOUT,
    RELAXED_TIMEOUT,
    _FAULT_INJECTOR_CLIENT_OSS_API,
    _get_client_maint_notifications,
    get_cluster_client_maint_notifications,
    use_mock_proxy,
)
from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
    NodeInfo,
    ProxyServerFaultInjector,
    SlotMigrateEffects,
)
from tests.test_scenario.maint_notifications_helpers import (
    ClientValidations,
    ClusterOperations,
    KeyGenerationHelpers,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S:%f",
)

BIND_TIMEOUT = 60
MIGRATE_TIMEOUT = 60
FAILOVER_TIMEOUT = 15
SMIGRATING_TIMEOUT = 20
SMIGRATED_TIMEOUT = 40

SLOT_SHUFFLE_TIMEOUT = 120

DEFAULT_BIND_TTL = 15
DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT = 1
DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT = 1


class TestPushNotificationsBase:
    """
    Test Redis Enterprise maintenance push notifications with real cluster
    operations.
    """

    def _trigger_effect(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        effect_name: SlotMigrateEffects,
        trigger_name: Optional[str] = None,
        target_node: Optional[str] = None,
        empty_node: Optional[str] = None,
        skip_end_notification: bool = False,
        timeout: int = SLOT_SHUFFLE_TIMEOUT,
    ):
        trigger_effect_action_id = ClusterOperations.trigger_effect(
            fault_injector=fault_injector_client,
            endpoint_config=endpoints_config,
            effect_name=effect_name,
            trigger_name=trigger_name,
            source_node=target_node,
            target_node=empty_node,
            skip_end_notification=skip_end_notification,
        )

        trigger_effect_result = fault_injector_client.get_operation_result(
            trigger_effect_action_id,
            timeout=timeout,
        )
        logging.debug(f"Action execution result: {trigger_effect_result}")

    def _execute_failover(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        failover_result = ClusterOperations.execute_failover(
            fault_injector_client, endpoints_config
        )

        logging.debug("Marking failover as executed")
        self._failover_executed = True

        logging.debug(f"Failover result: {failover_result}")

    def _execute_migration(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
        skip_end_notification: bool = False,
    ):
        migrate_action_id = ClusterOperations.execute_migrate(
            fault_injector=fault_injector_client,
            endpoint_config=endpoints_config,
            target_node=target_node,
            empty_node=empty_node,
            skip_end_notification=skip_end_notification,
        )

        migrate_result = fault_injector_client.get_operation_result(
            migrate_action_id, timeout=MIGRATE_TIMEOUT
        )
        logging.debug(f"Migration result: {migrate_result}")

    def _execute_bind(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        endpoint_id: str,
    ):
        bind_action_id = ClusterOperations.execute_rebind(
            fault_injector_client, endpoints_config, endpoint_id
        )

        bind_result = fault_injector_client.get_operation_result(
            bind_action_id, timeout=BIND_TIMEOUT
        )
        logging.debug(f"Bind result: {bind_result}")

    def _execute_migrate_bind_flow(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
        endpoint_id: str,
    ):
        self._execute_migration(
            fault_injector_client=fault_injector_client,
            endpoints_config=endpoints_config,
            target_node=target_node,
            empty_node=empty_node,
            skip_end_notification=True,
        )
        self._execute_bind(
            fault_injector_client=fault_injector_client,
            endpoints_config=endpoints_config,
            endpoint_id=endpoint_id,
        )

    def _get_all_connections_in_pool(self, client: Redis) -> List[ConnectionInterface]:
        connections = []
        with client.connection_pool._lock:
            for conn in client.connection_pool._get_free_connections():
                connections.append(conn)
            for conn in client.connection_pool._get_in_use_connections():
                connections.append(conn)
        return connections

    def _validate_maintenance_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_conns_count = 0
        connections = self._get_all_connections_in_pool(client)

        for conn in connections:
            if (
                conn._sock is not None
                and conn._sock.gettimeout() == RELAXED_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MAINTENANCE
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

    def _validate_moving_state(
        self,
        client: Redis,
        configured_endpoint_type: EndpointType,
        expected_matching_connected_conns_count: int,
        expected_matching_disconnected_conns_count: int,
        fault_injector_client: FaultInjectorClient,
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_connected_conns_count = 0
        matching_disconnected_conns_count = 0
        with client.connection_pool._lock:
            connections = self._get_all_connections_in_pool(client)
            for conn in connections:
                endpoint_configured_correctly = bool(
                    (
                        configured_endpoint_type == EndpointType.NONE
                        and conn.host == conn.orig_host_address
                    )
                    or (
                        configured_endpoint_type != EndpointType.NONE
                        and conn.host != conn.orig_host_address
                        and (
                            configured_endpoint_type
                            == MaintNotificationsConfig().get_endpoint_type(
                                conn.host, conn
                            )
                        )
                    )
                    or isinstance(
                        fault_injector_client, ProxyServerFaultInjector
                    )  # we should not validate the endpoint type when using proxy server
                )

                if (
                    conn._sock is not None
                    and conn._sock.gettimeout() == RELAXED_TIMEOUT
                    and conn.maintenance_state == MaintenanceState.MOVING
                    and endpoint_configured_correctly
                ):
                    matching_connected_conns_count += 1
                elif (
                    conn._sock is None
                    and conn.maintenance_state == MaintenanceState.MOVING
                    and conn.socket_timeout == RELAXED_TIMEOUT
                    and endpoint_configured_correctly
                ):
                    matching_disconnected_conns_count += 1
                else:
                    pass
        assert matching_connected_conns_count == expected_matching_connected_conns_count
        assert (
            matching_disconnected_conns_count
            == expected_matching_disconnected_conns_count
        )

    def _validate_default_state(
        self,
        client: Redis,
        expected_matching_conns_count: Union[int, Literal["all"]],
        configured_timeout: float = CLIENT_TIMEOUT,
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_conns_count = 0
        connections = self._get_all_connections_in_pool(client)
        logging.info(f"Validating {len(connections)} connections")
        logging.info(f"Expected matching conns count: {expected_matching_conns_count}")

        for conn in connections:
            if conn._sock is None:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == configured_timeout
                    and conn.host == conn.orig_host_address
                ):
                    matching_conns_count += 1
                else:
                    logging.debug(
                        f"Connection not matching default state: "
                        f"maintenance_state={conn.maintenance_state}, "
                        f"socket_timeout={conn.socket_timeout}, "
                        f"host={conn.host}, "
                        f"orig_host_address={conn.orig_host_address}"
                    )
            elif (
                conn._sock.gettimeout() == configured_timeout
                and conn.maintenance_state == MaintenanceState.NONE
                and conn.host == conn.orig_host_address
            ):
                matching_conns_count += 1
            else:
                logging.debug(
                    f"Connection not matching default state: "
                    f"maintenance_state={conn.maintenance_state}, "
                    f"socket_timeout={conn.socket_timeout}, "
                    f"host={conn.host}, "
                    f"orig_host_address={conn.orig_host_address}"
                )

        # Get client configuration details for error message
        conn_kwargs = client.connection_pool.connection_kwargs
        client_host = conn_kwargs.get("host", "unknown")
        client_port = conn_kwargs.get("port", "unknown")

        if expected_matching_conns_count == "all":
            expected_matching_conns_count = len(connections)

        assert matching_conns_count == expected_matching_conns_count, (
            f"Default state validation failed. "
            f"Client: host={client_host}, port={client_port}, "
            f"configured_timeout={configured_timeout}. "
            f"Expected {expected_matching_conns_count} matching connections, "
            f"but found {matching_conns_count} out of {len(connections)} total connections."
        )

    def _validate_default_notif_disabled_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_conns_count = 0
        connections = self._get_all_connections_in_pool(client)

        for conn in connections:
            if conn._sock is None:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == CLIENT_TIMEOUT
                    and not hasattr(conn, "orig_host_address")
                ):
                    matching_conns_count += 1
            elif (
                conn._sock.gettimeout() == CLIENT_TIMEOUT
                and conn.maintenance_state == MaintenanceState.NONE
                and not hasattr(conn, "orig_host_address")
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count


class TestStandaloneClientPushNotifications(TestPushNotificationsBase):
    @pytest.fixture(autouse=True)
    def setup_and_cleanup(
        self,
        client_maint_notifications: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        endpoint_name: str,
    ):
        # Initialize cleanup flags first to ensure they exist even if setup fails
        self._failover_executed = False
        self.endpoint_id = None

        try:
            target_node, empty_node = ClusterOperations.find_target_node_and_empty_node(
                fault_injector_client, endpoints_config
            )
            logging.info(f"Using target_node: {target_node}, empty_node: {empty_node}")
        except Exception as e:
            pytest.fail(f"Failed to find target and empty nodes: {e}")

        try:
            self.endpoint_id = ClusterOperations.find_endpoint_for_bind(
                fault_injector_client,
                endpoint_name,
                force_cluster_info_refresh=False,
            )
            logging.info(f"Using endpoint: {self.endpoint_id}")
        except Exception as e:
            pytest.fail(f"Failed to find endpoint for bind operation: {e}")

        # Ensure setup completed successfully
        if not target_node or not empty_node:
            pytest.fail("Setup failed: target_node or empty_node not available")
        if not self.endpoint_id:
            pytest.fail("Setup failed: endpoint_id not available")

        self.target_node: NodeInfo = target_node
        self.empty_node: NodeInfo = empty_node

        # Yield control to the test
        yield

        # Cleanup code - this will run even if the test fails
        logging.info("Starting cleanup...")
        try:
            client_maint_notifications.close()
        except Exception as e:
            logging.error(f"Failed to close client: {e}")

        # Only attempt cleanup if we have the necessary attributes and they were executed
        if (
            not isinstance(fault_injector_client, ProxyServerFaultInjector)
            and self._failover_executed
        ):
            try:
                self._execute_failover(fault_injector_client, endpoints_config)
                logging.info("Failover cleanup completed")
            except Exception as e:
                logging.error(f"Failed to revert failover: {e}")

        logging.info("Cleanup finished")

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    def test_receive_failing_over_and_failed_over_push_notification(
        self,
        client_maint_notifications: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        """
        Test the push notifications are received when executing cluster operations.

        """
        logging.info("Creating one connection in the pool.")
        conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing failover command...")
        failover_thread = Thread(
            target=self._execute_failover,
            name="failover_thread",
            args=(fault_injector_client, endpoints_config),
        )
        failover_thread.start()

        logging.info("Waiting for FAILING_OVER push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=FAILOVER_TIMEOUT, connection=conn
        )

        logging.info("Validating connection maintenance state...")
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn._sock.gettimeout() == RELAXED_TIMEOUT

        logging.info("Waiting for FAILED_OVER push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=FAILOVER_TIMEOUT, connection=conn
        )

        logging.info("Validating connection default states is restored...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn._sock.gettimeout() == CLIENT_TIMEOUT

        logging.info("Releasing connection back to the pool...")
        client_maint_notifications.connection_pool.release(conn)

        failover_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    def test_receive_migrating_and_moving_push_notification(
        self,
        client_maint_notifications: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        """
        Test the push notifications are received when executing cluster operations.

        """
        # create one connection and release it back to the pool
        conn = client_maint_notifications.connection_pool.get_connection()
        client_maint_notifications.connection_pool.release(conn)

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=MIGRATE_TIMEOUT
        )

        logging.info("Validating connection migrating state...")
        conn = client_maint_notifications.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn._sock.gettimeout() == RELAXED_TIMEOUT
        client_maint_notifications.connection_pool.release(conn)

        logging.info("Waiting for MIGRATED push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=MIGRATE_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = client_maint_notifications.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn._sock.gettimeout() == CLIENT_TIMEOUT
        client_maint_notifications.connection_pool.release(conn)

        migrate_thread.join()

        logging.info("Executing rladmin bind endpoint command...")

        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        logging.info("Waiting for MOVING push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=BIND_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = client_maint_notifications.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn._sock.gettimeout() == RELAXED_TIMEOUT

        logging.info("Waiting for moving ttl to expire")
        time.sleep(BIND_TIMEOUT)

        logging.info("Validating connection states...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == CLIENT_TIMEOUT
        assert conn._sock.gettimeout() == CLIENT_TIMEOUT
        client_maint_notifications.connection_pool.release(conn)
        bind_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout
    @pytest.mark.parametrize(
        "endpoint_type",
        [
            EndpointType.EXTERNAL_FQDN,
            EndpointType.EXTERNAL_IP,
            EndpointType.NONE,
        ],
    )
    def test_timeout_handling_during_migrating_and_moving(
        self,
        endpoint_type: EndpointType,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        """
        Test the push notifications are received when executing cluster operations.

        """
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client = _get_client_maint_notifications(
            endpoints_config=endpoints_config, endpoint_type=endpoint_type
        )

        # Create three connections in the pool
        logging.info("Creating three connections in the pool.")
        conns = []
        for _ in range(3):
            conns.append(client.connection_pool.get_connection())
        # Release the connections
        for conn in conns:
            client.connection_pool.release(conn)

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in one of the connections
        ClientValidations.wait_push_notification(client, timeout=MIGRATE_TIMEOUT)

        self._validate_maintenance_state(client, expected_matching_conns_count=1)
        self._validate_default_state(client, expected_matching_conns_count=2)

        logging.info("Waiting for MIGRATED push notifications...")
        ClientValidations.wait_push_notification(client, timeout=MIGRATE_TIMEOUT)

        logging.info("Validating connection states after MIGRATED ...")
        self._validate_default_state(client, expected_matching_conns_count=3)

        migrate_thread.join()

        logging.info("Executing rladmin bind endpoint command...")

        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        logging.info("Waiting for MOVING push notifications...")
        # this will consume the notification in one of the connections
        # and will handle the states of the rest
        # the consumed connection will be disconnected during
        # releasing it back to the pool and as a result we will have
        # 3 disconnected connections in the pool
        ClientValidations.wait_push_notification(client, timeout=BIND_TIMEOUT)

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            time.sleep(fault_injector_client.get_moving_ttl() / 2)

        logging.info("Validating connections states...")
        self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=0,
            expected_matching_disconnected_conns_count=3,
            fault_injector_client=fault_injector_client,
        )
        # during get_connection() the connection will be reconnected
        # either to the address provided in the moving notification or to the original address
        # depending on the configured endpoint type
        # with this call we test if we are able to connect to the new address
        conn = client.connection_pool.get_connection()
        self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=2,
            fault_injector_client=fault_injector_client,
        )
        client.connection_pool.release(conn)

        logging.info("Waiting for moving ttl to expire")
        time.sleep(BIND_TIMEOUT)

        logging.info("Validating connection states...")
        self._validate_default_state(client, expected_matching_conns_count=3)
        bind_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout
    @pytest.mark.parametrize(
        "endpoint_type",
        [
            EndpointType.EXTERNAL_FQDN,
            EndpointType.EXTERNAL_IP,
            EndpointType.NONE,
        ],
    )
    def test_connection_handling_during_moving(
        self,
        endpoint_type: EndpointType,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client = _get_client_maint_notifications(
            endpoints_config=endpoints_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = client.connection_pool.get_connection()

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        ClientValidations.wait_push_notification(
            client, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        client.connection_pool.release(first_conn)

        migrate_thread.join()

        logging.info("Executing rladmin bind endpoint command...")

        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        logging.info("Waiting for MOVING push notifications on random connection ...")
        # this will consume the notification in one of the connections
        # and will handle the states of the rest
        # the consumed connection will be disconnected during
        # releasing it back to the pool and as a result we will have
        # 3 disconnected connections in the pool
        ClientValidations.wait_push_notification(client, timeout=BIND_TIMEOUT)

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            time.sleep(fault_injector_client.get_moving_ttl() / 2)

        # validate that new connections will also receive the moving notification
        connections = []
        for _ in range(3):
            connections.append(client.connection_pool.get_connection())
        for conn in connections:
            logging.debug(f"Releasing connection {conn}. {conn.maintenance_state}")
            client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        # during get_connection() the existing connection will be reconnected
        # either to the address provided in the moving notification or to the original address
        # depending on the configured endpoint type
        # with this call we test if we are able to connect to the new address
        # new connection should also be marked as moving
        self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=3,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        logging.info("Waiting for moving ttl to expire")
        time.sleep(fault_injector_client.get_moving_ttl())

        logging.info("Validating connection states after MOVING has expired ...")
        self._validate_default_state(client, expected_matching_conns_count=3)
        bind_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout
    def test_old_connection_shutdown_during_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        # it is better to use ip for this test - enables validation that
        # the connection is disconnected from the original address
        # and connected to the new address
        endpoint_type = EndpointType.EXTERNAL_IP
        logging.info("Testing old connection shutdown during MOVING")
        client = _get_client_maint_notifications(
            endpoints_config=endpoints_config, endpoint_type=endpoint_type
        )

        # create one connection and release it back to the pool
        conn = client.connection_pool.get_connection()
        client.connection_pool.release(conn)

        logging.info("Starting migration ...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        ClientValidations.wait_push_notification(client, timeout=MIGRATE_TIMEOUT)
        self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        ClientValidations.wait_push_notification(client, timeout=MIGRATE_TIMEOUT)
        self._validate_default_state(client, expected_matching_conns_count=1)
        migrate_thread.join()

        moving_event = threading.Event()

        def execute_commands(moving_event: threading.Event, errors: Queue):
            while not moving_event.is_set():
                try:
                    client.set("key", "value")
                    client.get("key")
                except Exception as e:
                    errors.put(
                        f"Command failed in thread {threading.current_thread().name}: {e}"
                    )

        # get the connection here because in case of proxy server
        # new connections will not receive the notification and there is a chance
        # that the existing connections in the pool that are used in the multiple
        # threads might have already consumed the notification
        # even with re clusters we might end up with an existing connection that has been
        # freed up in the pool that will not receive the notification while we are waiting
        # for it because it has already received and processed it
        conn_to_check_moving = client.connection_pool.get_connection()

        logging.info("Starting rebind...")
        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        errors = Queue()
        threads_count = 10
        futures = []

        logging.info(f"Starting {threads_count} command execution threads...")
        # Start the worker pool and submit N identical worker tasks
        with ThreadPoolExecutor(
            max_workers=threads_count, thread_name_prefix="command_execution_thread"
        ) as executor:
            futures = [
                executor.submit(execute_commands, moving_event, errors)
                for _ in range(threads_count)
            ]

            logging.info("Waiting for MOVING push notification ...")
            # this will consume the notification in one of the connections
            # and will handle the states of the rest
            ClientValidations.wait_push_notification(
                client, timeout=BIND_TIMEOUT, connection=conn_to_check_moving
            )
            # set the event to stop the command execution threads
            logging.info("Setting moving event...")
            moving_event.set()
            # release the connection back to the pool so that it can be disconnected
            # as part of the flow
            client.connection_pool.release(conn_to_check_moving)

            # Wait for all workers to finish and propagate any exceptions
            for f in futures:
                f.result()

        logging.info(
            "All command execution threads finished. Validating connections states..."
        )
        # validate that all connections are either disconnected
        # or connected to the new address
        connections = self._get_all_connections_in_pool(client)
        for conn in connections:
            if conn._sock is not None:
                assert conn.get_resolved_ip() == conn.host
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn._sock.gettimeout() == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, ProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()
            else:
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, ProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()

        # validate no errors were raised in the command execution threads
        assert errors.empty(), f"Errors occurred in threads: {errors.queue}"

        logging.info("Waiting for moving ttl to expire")
        bind_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    def test_new_connections_receive_moving(
        self,
        client_maint_notifications: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=1
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        migrate_thread.join()

        logging.info("Executing rladmin bind endpoint command...")

        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        logging.info("Waiting for MOVING push notifications on random connection ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=BIND_TIMEOUT, connection=first_conn
        )

        old_address = first_conn._sock.getpeername()[0]
        logging.info(f"The node address before bind: {old_address}")
        logging.info(
            "Creating new client to connect to the same node - new connections to this node should receive the moving notification..."
        )

        endpoint_type = EndpointType.EXTERNAL_IP
        # create new client with new pool that should also receive the moving notification
        new_client = _get_client_maint_notifications(
            endpoints_config=endpoints_config,
            endpoint_type=endpoint_type,
            host_config=old_address,
        )

        # the moving notification will be consumed as
        # part of the client connection setup, so we don't need
        # to wait for it explicitly with wait_push_notification
        logging.info(
            "Creating one connection in the new pool that should receive the moving notification."
        )
        new_client_conn = new_client.connection_pool.get_connection()

        logging.info("Validating connections states during MOVING ...")
        self._validate_moving_state(
            new_client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        logging.info("Waiting for moving thread to be completed ...")
        bind_thread.join()

        new_client.connection_pool.release(new_client_conn)
        new_client.close()

        client_maint_notifications.connection_pool.release(first_conn)

    @pytest.mark.timeout(300)  # 5 minutes timeout
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    def test_new_connections_receive_migrating(
        self,
        client_maint_notifications: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_notifications.connection_pool.get_connection()

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=1
        )

        # validate that new connections will also receive the migrating notification
        # it should be received as part of the client connection setup flow
        logging.info(
            "Creating second connection that should receive the migrating notification as well."
        )
        second_connection = client_maint_notifications.connection_pool.get_connection()
        self._validate_maintenance_state(
            client_maint_notifications, expected_matching_conns_count=2
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client_maint_notifications, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )
        ClientValidations.wait_push_notification(
            client_maint_notifications,
            timeout=MIGRATE_TIMEOUT,
            connection=second_connection,
        )

        migrate_thread.join()
        logging.info("Executing rladmin bind endpoint command for cleanup...")

        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()
        bind_thread.join()
        client_maint_notifications.connection_pool.release(first_conn)
        client_maint_notifications.connection_pool.release(second_connection)

    @pytest.mark.timeout(300)
    def test_disabled_handling_during_migrating_and_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info("Creating client with disabled notifications.")
        client = _get_client_maint_notifications(
            endpoints_config=endpoints_config,
            enable_maintenance_notifications=False,
        )

        logging.info("Creating one connection in the pool.")
        first_conn = client.connection_pool.get_connection()

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection if it arrives
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )

        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=1
        )

        # validate that new connections will also receive the moving notification
        logging.info(
            "Creating second connection in the pool"
            " and expect it not to receive the migrating as well."
        )

        second_connection = client.connection_pool.get_connection()
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_connection
        )

        logging.info(
            "Validating connection states after MIGRATING for both connections ..."
        )
        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=2
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )
        ClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_connection
        )

        client.connection_pool.release(first_conn)
        client.connection_pool.release(second_connection)

        migrate_thread.join()

        logging.info("Executing rladmin bind endpoint command...")

        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        logging.info("Waiting for MOVING push notifications on random connection ...")
        # this will consume the notification if it arrives in one of the connections
        # and will handle the states of the rest
        # the consumed connection will be disconnected during
        # releasing it back to the pool and as a result we will have
        # 3 disconnected connections in the pool
        ClientValidations.wait_push_notification(
            client,
            timeout=10,
            fail_on_timeout=False,
        )

        # validate that new connections will also receive the moving notification
        connections = []
        for _ in range(3):
            connections.append(client.connection_pool.get_connection())
        for conn in connections:
            client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        logging.info("Waiting for moving ttl to expire")
        time.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states after MOVING has expired ...")
        self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )
        bind_thread.join()

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "endpoint_type",
        [
            EndpointType.EXTERNAL_FQDN,
            EndpointType.EXTERNAL_IP,
            EndpointType.NONE,
        ],
    )
    def test_command_execution_during_migrating_and_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        endpoint_type: EndpointType,
    ):
        """
        Test command execution during migrating and moving notifications.

        This test validates that:
        1. Commands can be executed during MIGRATING and MOVING notifications
        2. Commands are not blocked by the notifications
        3. Commands are executed successfully
        """
        errors = Queue()
        if isinstance(fault_injector_client, ProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 180

        socket_timeout = DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        client = _get_client_maint_notifications(
            endpoints_config=endpoints_config,
            endpoint_type=endpoint_type,
            disable_retries=True,
            socket_timeout=socket_timeout,
            enable_maintenance_notifications=True,
        )

        def execute_commands(duration: int, errors: Queue):
            start = time.time()
            while time.time() - start < duration:
                try:
                    client.set("key", "value")
                    client.get("key")
                except Exception as e:
                    logging.error(
                        f"Error in thread {threading.current_thread().name}: {e}"
                    )
                    errors.put(
                        f"Command failed in thread {threading.current_thread().name}: {e}"
                    )
            logging.debug(f"{threading.current_thread().name}: Thread ended")

        threads = []
        for i in range(10):
            thread = Thread(
                target=execute_commands,
                name=f"command_execution_thread_{i}",
                args=(
                    execution_duration,
                    errors,
                ),
            )
            thread.start()
            threads.append(thread)

        logging.info("Waiting for threads to start and have a few cycles executed ...")
        time.sleep(3)

        migrate_and_bind_thread = Thread(
            target=self._execute_migrate_bind_flow,
            name="migrate_and_bind_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node.node_id,
                self.empty_node.node_id,
                self.endpoint_id,
            ),
        )
        migrate_and_bind_thread.start()

        for thread in threads:
            thread.join()

        migrate_and_bind_thread.join()

        # validate connections settings
        self._validate_default_state(
            client, expected_matching_conns_count=10, configured_timeout=socket_timeout
        )

        assert errors.empty(), f"Errors occurred in threads: {errors.queue}"


# 5 minutes timeout for this test
# @pytest.mark.skipif(
#     use_mock_proxy(),
#     reason="Mock proxy doesn't support sending notifications to new connections.",
# )


def generate_params(
    fault_injector_client: FaultInjectorClient,
    effect_names: list[SlotMigrateEffects],
):
    # params should produce list of tuples: (effect_name, trigger_name, bdb_config, bdb_name)
    params = []
    try:
        logging.info(f"Extracting params for test with effect_names: {effect_names}")
        for effect_name in effect_names:
            triggers_data = ClusterOperations.get_slot_migrate_triggers(
                fault_injector_client, effect_name
            )

            for trigger_info in triggers_data["triggers"]:
                trigger = trigger_info["name"]
                if trigger == "maintenance_mode":
                    continue
                trigger_requirements = trigger_info["requirements"]
                for requirement in trigger_requirements:
                    dbconfig = requirement["dbconfig"]
                    ip_type = requirement["oss_cluster_api"]["ip_type"]
                    if ip_type == "internal":
                        continue
                    db_name_pattern = dbconfig.get("name").rsplit("-", 1)[0]
                    dbconfig["name"] = (
                        db_name_pattern  # this will ensure dbs will be deleted
                    )

                    params.append((effect_name, trigger, dbconfig, db_name_pattern))
    except Exception as e:
        logging.error(f"Failed to extract params for test: {e}")

    return params


class TestClusterClientPushNotificationsWithEffectTriggerBase(
    TestPushNotificationsBase
):
    def delete_prev_db(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        db_name: str,
    ):
        try:
            logging.info(f"Deleting database if exists: {db_name}")
            existing_db_id = None
            existing_db_id = ClusterOperations.find_database_id_by_name(
                fault_injector_client_oss_api, db_name
            )

            if existing_db_id:
                fault_injector_client_oss_api.delete_database(existing_db_id)
                logging.info(f"Deleted database: {db_name}")
            else:
                logging.info(f"Database {db_name} does not exist.")
        except Exception as e:
            logging.error(f"Failed to delete database {db_name}: {e}")

    def create_db(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        bdb_config: Dict[str, Any],
    ):
        try:
            logging.info(f"Creating database: \n{json.dumps(bdb_config, indent=2)}")
            cluster_endpoint_config = fault_injector_client_oss_api.create_database(
                bdb_config
            )
            return cluster_endpoint_config
        except Exception as e:
            pytest.fail(f"Failed to create database: {e}")

    def setup_env(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        db_config: Dict[str, Any],
    ):
        self.delete_prev_db(fault_injector_client_oss_api, db_config["name"])

        cluster_endpoint_config = self.create_db(
            fault_injector_client_oss_api, db_config
        )

        self._bdb_name = db_config["name"]
        socket_timeout = DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT

        cluster_client_maint_notifications = get_cluster_client_maint_notifications(
            endpoints_config=cluster_endpoint_config,
            disable_retries=True,
            socket_timeout=socket_timeout,
            enable_maintenance_notifications=True,
        )
        return cluster_client_maint_notifications, cluster_endpoint_config

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(
        self,
    ):
        self.maintenance_ops_threads = []
        self._bdb_name = None

        # Yield control to the test
        yield

        # Cleanup code - this will run even if the test fails
        logging.info("Starting cleanup...")
        if self._bdb_name:
            self.delete_prev_db(_FAULT_INJECTOR_CLIENT_OSS_API, self._bdb_name)

        logging.info("Waiting for maintenance operations threads to finish...")
        for thread in self.maintenance_ops_threads:
            thread.join()

        logging.info("Cleanup finished")


class TestClusterClientPushNotificationsHandlingWithEffectTrigger(
    TestClusterClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API, [SlotMigrateEffects.SLOT_SHUFFLE]
        ),
    )
    def test_notification_handling_during_node_shuffle_no_node_replacement(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        The test validates the behavior when during the operations the slots are moved
        between the nodes, but no new nodes are appearing and no nodes are disappearing

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=int(SLOT_SHUFFLE_TIMEOUT / 2),
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )

        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Validating connection state after SMIGRATED ...")

        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 0
        assert len(initial_cluster_nodes) == len(updated_cluster_nodes)

        marked_conns_for_reconnect = 0
        for conn in in_use_connections.values():
            if conn.should_reconnect():
                marked_conns_for_reconnect += 1
        # only one connection should be marked for reconnect
        # onle the one that belongs to the node that was from
        # the src address of the maintenance
        assert marked_conns_for_reconnect == 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.REMOVE_ADD,
            ],
        ),
    )
    def test_notification_handling_with_node_replace(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        The test validates the behavior when during the operations the slots are moved
        between the nodes, and as a result a node is removed and a new node is added to the cluster

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")

        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=SMIGRATING_TIMEOUT,
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )

        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Validating connection state after SMIGRATED ...")

        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 1
        removed_node = removed_nodes.pop()
        assert removed_node is not None

        added_nodes = set(updated_cluster_nodes.values()) - set(
            initial_cluster_nodes.values()
        )
        assert len(added_nodes) == 1

        conn = in_use_connections.get(removed_node)
        # connection will be dropped, but it is marked
        # to be disconnected before released to the pool
        # we don't waste time to update the timeouts and state
        # so it is pointless to check those configs
        assert conn is not None
        assert conn.should_reconnect() is True

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.REMOVE,
            ],
        ),
    )
    def test_notification_handling_with_node_remove(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        The test validates the behavior when during the operations the slots are moved
        between the nodes, and as a result a node is removed.

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")

        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = (
                node.redis_connection.connection_pool.get_connection()
            )

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conn in in_use_connections.values():
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=int(SLOT_SHUFFLE_TIMEOUT / 2),
                connection=conn,
            )

        logging.info("Validating connection maintenance state...")
        for conn in in_use_connections.values():
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        assert len(initial_cluster_nodes) == len(
            cluster_client_maint_notifications.nodes_manager.nodes_cache
        )

        for node_key in initial_cluster_nodes.keys():
            assert (
                node_key in cluster_client_maint_notifications.nodes_manager.nodes_cache
            )

        logging.info("Waiting for SMIGRATED push notifications...")
        con_to_read_smigrated = random.choice(list(in_use_connections.values()))
        ClientValidations.wait_push_notification(
            cluster_client_maint_notifications,
            timeout=SMIGRATED_TIMEOUT,
            connection=con_to_read_smigrated,
        )

        logging.info("Validating connection state after SMIGRATED ...")

        updated_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )

        removed_nodes = set(initial_cluster_nodes.values()) - set(
            updated_cluster_nodes.values()
        )
        assert len(removed_nodes) == 1
        removed_node = removed_nodes.pop()
        assert removed_node is not None

        assert len(initial_cluster_nodes) == len(updated_cluster_nodes) + 1

        conn = in_use_connections.get(removed_node)
        # connection will be dropped, but it is marked
        # to be disconnected before released to the pool
        # we don't waste time to update the timeouts and state
        # so it is pointless to check those configs
        assert conn is not None
        assert conn.should_reconnect() is True

        # validate no other connections are marked for reconnect
        marked_conns_for_reconnect = 0
        for conn in in_use_connections.values():
            if conn.should_reconnect():
                marked_conns_for_reconnect += 1
        # only one connection should be marked for reconnect
        # onle the one that belongs to the node that was from
        # the src address of the maintenance
        assert marked_conns_for_reconnect == 1

        logging.info("Releasing connections back to the pool...")
        for node, conn in in_use_connections.items():
            if node.redis_connection is None:
                continue
            node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.SLOT_SHUFFLE,
                SlotMigrateEffects.REMOVE_ADD,
                SlotMigrateEffects.REMOVE,
                SlotMigrateEffects.ADD,
            ],
        ),
    )
    def test_new_connections_receive_last_notification_with_migrating(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are sent to the newly created connections.

        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        logging.info("Creating one connection in each node's pool.")
        initial_cluster_nodes = (
            cluster_client_maint_notifications.nodes_manager.nodes_cache.copy()
        )
        in_use_connections = {}
        for node in initial_cluster_nodes.values():
            in_use_connections[node] = [
                node.redis_connection.connection_pool.get_connection()
            ]

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
        )

        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        logging.info("Waiting for SMIGRATING push notifications in all connections...")
        for conns_per_node in in_use_connections.values():
            for conn in conns_per_node:
                ClientValidations.wait_push_notification(
                    cluster_client_maint_notifications,
                    timeout=int(SLOT_SHUFFLE_TIMEOUT / 2),
                    connection=conn,
                )
                logging.info(
                    f"Validating connection MAINTENANCE state and RELAXED timeout for conn: {conn}..."
                )
                assert conn.maintenance_state == MaintenanceState.MAINTENANCE
                assert conn._sock.gettimeout() == RELAXED_TIMEOUT
                assert conn.should_reconnect() is False

        logging.info(
            "Validating newly created connections will receive the SMIGRATING notification..."
        )
        for node in initial_cluster_nodes.values():
            conn = node.redis_connection.connection_pool.get_connection()
            in_use_connections[node].append(conn)
            ClientValidations.wait_push_notification(
                cluster_client_maint_notifications,
                timeout=1,
                connection=conn,
                fail_on_timeout=False,  # it might get read during handshake
            )
            logging.info(
                f"Validating new connection MAINTENANCE state and RELAXED timeout for conn: {conn}..."
            )
            assert conn.maintenance_state == MaintenanceState.MAINTENANCE
            assert conn._sock.gettimeout() == RELAXED_TIMEOUT
            assert conn.should_reconnect() is False

        logging.info(
            "Waiting for SMIGRATED push notifications in ALL EXISTING connections..."
        )
        marked_conns_for_reconnect = 0
        for conns_per_node in in_use_connections.values():
            for conn in conns_per_node:
                ClientValidations.wait_push_notification(
                    cluster_client_maint_notifications,
                    timeout=SMIGRATED_TIMEOUT,
                    connection=conn,
                )
                logging.info(
                    f"Validating connection state after SMIGRATED for conn: {conn}, "
                    f"local socket port: {conn._sock.getsockname()[1] if conn._sock else None}..."
                )
                if conn.should_reconnect():
                    logging.info(f"Connection marked for reconnect: {conn}")
                    marked_conns_for_reconnect += 1
                assert conn.maintenance_state == MaintenanceState.NONE
                assert conn.socket_timeout == DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT
                assert (
                    conn.socket_connect_timeout == DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT
                )
        assert (
            marked_conns_for_reconnect >= 1
        )  # at least one should be marked for reconnect

        logging.info("Releasing connections back to the pool...")
        for node, conns in in_use_connections.items():
            if node.redis_connection is None:
                continue
            for conn in conns:
                node.redis_connection.connection_pool.release(conn)

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)


class TestClusterClientCommandsExecutionWithPushNotificationsWithEffectTrigger(
    TestClusterClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_CLIENT_OSS_API,
            [
                SlotMigrateEffects.SLOT_SHUFFLE,
                SlotMigrateEffects.REMOVE,
                SlotMigrateEffects.ADD,
                SlotMigrateEffects.SLOT_SHUFFLE,
            ],
        ),
    )
    def test_command_execution_during_slot_shuffle_no_node_replacement(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        """
        Test the push notifications are received when executing re cluster operations.
        """
        logging.info(f"DB name: {db_name}")

        cluster_client_maint_notifications, cluster_endpoint_config = self.setup_env(
            fault_injector_client_oss_api, db_config
        )

        shards_count = db_config["shards_count"]
        logging.info(f"Shards count: {shards_count}")

        errors = Queue()
        if isinstance(fault_injector_client_oss_api, ProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 40

        def execute_commands(duration: int, errors: Queue):
            start = time.time()
            executed_commands_count = 0
            keys_for_all_shards = KeyGenerationHelpers.generate_keys_for_all_shards(
                shards_count,
                prefix=f"{threading.current_thread().name}_{effect_name}_{trigger}_key",
            )

            logging.info("Starting commands execution...")
            while time.time() - start < duration:
                for key in keys_for_all_shards:
                    try:
                        # the slot is covered by the first shard - this one will have slots migrated
                        cluster_client_maint_notifications.set(key, "value")
                        cluster_client_maint_notifications.get(key)
                        executed_commands_count += 2
                    except Exception as e:
                        logging.error(
                            f"Error in thread {threading.current_thread().name}: {e}"
                        )
                        errors.put(
                            f"Command failed in thread {threading.current_thread().name}: {e}"
                        )
                if executed_commands_count % 500 == 0:
                    logging.debug(
                        f"Executed {executed_commands_count} commands in {threading.current_thread().name}"
                    )
            logging.debug(f"{threading.current_thread().name}: Thread ended")

        threads = []
        for i in range(10):
            thread = Thread(
                target=execute_commands,
                name=f"cmd_execution_{i}",
                args=(
                    execution_duration,
                    errors,
                ),
            )
            thread.start()
            threads.append(thread)

        logging.info("Waiting for threads to start and have a few cycles executed ...")
        time.sleep(3)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_effect_thread = Thread(
            target=self._trigger_effect,
            name="trigger_effect_thread",
            args=(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
                effect_name,
                trigger,
            ),
        )
        self.maintenance_ops_threads.append(trigger_effect_thread)
        trigger_effect_thread.start()

        for thread in threads:
            thread.join()

        trigger_effect_thread.join()
        self.maintenance_ops_threads.remove(trigger_effect_thread)

        # go through all nodes and all their connections and consume the buffers - to validate no
        # notifications were left unconsumed
        logging.info(
            "Consuming all buffers to validate no notifications were left unconsumed..."
        )
        for (
            node
        ) in cluster_client_maint_notifications.nodes_manager.nodes_cache.values():
            if node.redis_connection is None:
                continue
            for conn in self._get_all_connections_in_pool(node.redis_connection):
                if conn._sock:
                    while conn.can_read(timeout=0.2):
                        conn.read_response(push_request=True)
            logging.info(f"Consumed all buffers for node: {node.name}")
        logging.info("All buffers consumed.")

        for (
            node
        ) in cluster_client_maint_notifications.nodes_manager.nodes_cache.values():
            # validate connections settings
            self._validate_default_state(
                node.redis_connection,
                expected_matching_conns_count="all",
                configured_timeout=DEFAULT_OSS_API_CLIENT_SOCKET_TIMEOUT,
            )
            logging.info(
                f"Node successfully validated: {node.name}, "
                f"connections: {len(self._get_all_connections_in_pool(node.redis_connection))}"
            )

        # validate no errors were raised in the command execution threads
        assert errors.empty(), f"Errors occurred in threads: {errors.queue}"
