"""Tests for Redis Enterprise moving push notifications with real cluster operations."""

import logging
from queue import Queue
from threading import Thread
import threading
import time
from typing import Any, Dict, List

import pytest

from redis import Redis
from redis.connection import ConnectionInterface
from redis.maintenance_events import (
    EndpointType,
    MaintenanceEventsConfig,
    MaintenanceState,
)
from tests.test_scenario.conftest import (
    CLIENT_TIMEOUT,
    RELAX_TIMEOUT,
    _get_client_maint_events,
)
from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
)
from tests.test_scenario.hitless_upgrade_helpers import (
    ClientValidations,
    ClusterOperations,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

BIND_TIMEOUT = 30
MIGRATE_TIMEOUT = 60
FAILOVER_TIMEOUT = 15

DEFAULT_BIND_TTL = 15


class TestPushNotifications:
    """
    Test Redis Enterprise maintenance push notifications with real cluster
    operations.
    """

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(
        self,
        client_maint_events: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        endpoint_name: str,
    ):
        # Initialize cleanup flags first to ensure they exist even if setup fails
        self._failover_executed = False
        self._migration_executed = False
        self._bind_executed = False
        self.target_node = None
        self.empty_node = None
        self.endpoint_id = None

        try:
            self.target_node, self.empty_node = (
                ClusterOperations.find_target_node_and_empty_node(
                    fault_injector_client, endpoints_config
                )
            )
            logging.info(
                f"Using target_node: {self.target_node}, empty_node: {self.empty_node}"
            )
        except Exception as e:
            pytest.fail(f"Failed to find target and empty nodes: {e}")

        try:
            self.endpoint_id = ClusterOperations.find_endpoint_for_bind(
                fault_injector_client, endpoints_config, endpoint_name
            )
            logging.info(f"Using endpoint: {self.endpoint_id}")
        except Exception as e:
            pytest.fail(f"Failed to find endpoint for bind operation: {e}")

        # Ensure setup completed successfully
        if not self.target_node or not self.empty_node:
            pytest.fail("Setup failed: target_node or empty_node not available")
        if not self.endpoint_id:
            pytest.fail("Setup failed: endpoint_id not available")

        # Yield control to the test
        yield

        # Cleanup code - this will run even if the test fails
        logging.info("Starting cleanup...")
        try:
            client_maint_events.close()
        except Exception as e:
            logging.error(f"Failed to close client: {e}")

        # Only attempt cleanup if we have the necessary attributes and they were executed
        if self._failover_executed:
            try:
                self._execute_failover(fault_injector_client, endpoints_config)
                logging.info("Failover cleanup completed")
            except Exception as e:
                logging.error(f"Failed to revert failover: {e}")

        if self._migration_executed:
            try:
                if self.target_node and self.empty_node:
                    self._execute_migration(
                        fault_injector_client=fault_injector_client,
                        endpoints_config=endpoints_config,
                        target_node=self.empty_node,
                        empty_node=self.target_node,
                    )
                    logging.info("Migration cleanup completed")
            except Exception as e:
                logging.error(f"Failed to revert migration: {e}")

        if self._bind_executed:
            try:
                if self.endpoint_id:
                    self._execute_bind(
                        fault_injector_client, endpoints_config, self.endpoint_id
                    )
                    logging.info("Bind cleanup completed")
            except Exception as e:
                logging.error(f"Failed to revert bind endpoint: {e}")

        logging.info("Cleanup finished")

    def _execute_failover(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        failover_result = ClusterOperations.execute_failover(
            fault_injector_client, endpoints_config
        )
        self._failover_executed = True

        logging.debug(f"Failover result: {failover_result}")

    def _execute_migration(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
    ):
        migrate_action_id = ClusterOperations.execute_rladmin_migrate(
            fault_injector=fault_injector_client,
            endpoint_config=endpoints_config,
            target_node=target_node,
            empty_node=empty_node,
        )

        self._migration_executed = True

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
        bind_action_id = ClusterOperations.execute_rladmin_bind_endpoint(
            fault_injector_client, endpoints_config, endpoint_id
        )

        self._bind_executed = True

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
        )
        self._execute_bind(
            fault_injector_client=fault_injector_client,
            endpoints_config=endpoints_config,
            endpoint_id=endpoint_id,
        )

    def _get_all_connections_in_pool(self, client: Redis) -> List[ConnectionInterface]:
        connections = []
        if hasattr(client.connection_pool, "_available_connections"):
            for conn in client.connection_pool._available_connections:
                connections.append(conn)
        if hasattr(client.connection_pool, "_in_use_connections"):
            for conn in client.connection_pool._in_use_connections:
                connections.append(conn)
        if hasattr(client.connection_pool, "_connections"):
            # This is the case for BlockingConnectionPool
            for conn in client.connection_pool._connections:
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
                and conn._sock.gettimeout() == RELAX_TIMEOUT
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
    ):
        """Validate the client connections are in the expected state after migration."""
        matching_connected_conns_count = 0
        matching_disconnected_conns_count = 0
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
                        == MaintenanceEventsConfig().get_endpoint_type(conn.host, conn)
                    )
                )
            )
            if (
                conn._sock is not None
                and conn._sock.gettimeout() == RELAX_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MOVING
                and endpoint_configured_correctly
            ):
                matching_connected_conns_count += 1
            elif (
                conn._sock is None
                and conn.maintenance_state == MaintenanceState.MOVING
                and conn.socket_timeout == RELAX_TIMEOUT
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
                    and conn.host == conn.orig_host_address
                ):
                    matching_conns_count += 1
            elif (
                conn._sock.gettimeout() == CLIENT_TIMEOUT
                and conn.maintenance_state == MaintenanceState.NONE
                and conn.host == conn.orig_host_address
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

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

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    def test_receive_failing_over_and_failed_over_push_notification(
        self,
        client_maint_events: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        """
        Test the push notifications are received when executing cluster operations.

        """
        logging.info("Creating one connection in the pool.")
        conn = client_maint_events.connection_pool.get_connection()

        logging.info("Executing failover command...")
        failover_thread = Thread(
            target=self._execute_failover,
            name="failover_thread",
            args=(fault_injector_client, endpoints_config),
        )
        failover_thread.start()

        logging.info("Waiting for FAILING_OVER push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=FAILOVER_TIMEOUT, connection=conn
        )

        logging.info("Validating connection maintenance state...")
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn._sock.gettimeout() == RELAX_TIMEOUT

        logging.info("Waiting for FAILED_OVER push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=FAILOVER_TIMEOUT, connection=conn
        )

        logging.info("Validating connection default states is restored...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn._sock.gettimeout() == CLIENT_TIMEOUT

        logging.info("Releasing connection back to the pool...")
        client_maint_events.connection_pool.release(conn)

        failover_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout for this test
    def test_receive_migrating_and_moving_push_notification(
        self,
        client_maint_events: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        """
        Test the push notifications are received when executing cluster operations.

        """

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node,
                self.empty_node,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT
        )

        logging.info("Validating connection migrating state...")
        conn = client_maint_events.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn._sock.gettimeout() == RELAX_TIMEOUT
        client_maint_events.connection_pool.release(conn)

        logging.info("Waiting for MIGRATED push notifications...")
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = client_maint_events.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn._sock.gettimeout() == CLIENT_TIMEOUT
        client_maint_events.connection_pool.release(conn)

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
            client_maint_events, timeout=BIND_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = client_maint_events.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn._sock.gettimeout() == RELAX_TIMEOUT

        logging.info("Waiting for moving ttl to expire")
        time.sleep(BIND_TIMEOUT)

        logging.info("Validating connection states...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == CLIENT_TIMEOUT
        assert conn._sock.gettimeout() == CLIENT_TIMEOUT
        client_maint_events.connection_pool.release(conn)
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
        client = _get_client_maint_events(
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
                self.target_node,
                self.empty_node,
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
            time.sleep(8)

        logging.info("Validating connections states...")
        self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=0,
            expected_matching_disconnected_conns_count=3,
        )
        # during get_connection() the connection will be reconnected
        # either to the address provided in the moving event or to the original address
        # depending on the configured endpoint type
        # with this call we test if we are able to connect to the new address
        conn = client.connection_pool.get_connection()
        self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=2,
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
        client = _get_client_maint_events(
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
                self.target_node,
                self.empty_node,
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
            time.sleep(8)

        # validate that new connections will also receive the moving event
        connections = []
        for _ in range(3):
            connections.append(client.connection_pool.get_connection())
        for conn in connections:
            client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        # during get_connection() the existing connection will be reconnected
        # either to the address provided in the moving event or to the original address
        # depending on the configured endpoint type
        # with this call we test if we are able to connect to the new address
        # new connection should also be marked as moving
        self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=3,
            expected_matching_disconnected_conns_count=0,
        )

        logging.info("Waiting for moving ttl to expire")
        time.sleep(BIND_TIMEOUT)

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
        client = _get_client_maint_events(
            endpoints_config=endpoints_config, endpoint_type=endpoint_type
        )

        logging.info("Starting migration ...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node,
                self.empty_node,
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

        logging.info("Starting rebind...")
        bind_thread = Thread(
            target=self._execute_bind,
            name="bind_thread",
            args=(fault_injector_client, endpoints_config, self.endpoint_id),
        )
        bind_thread.start()

        errors = Queue()
        commands_threads = []
        threads_count = 10
        logging.info(f"Starting {threads_count} command execution threads...")
        for _ in range(threads_count):
            thread = Thread(
                target=execute_commands,
                name="command_execution_thread",
                args=(
                    moving_event,
                    errors,
                ),
            )
            thread.start()
            commands_threads.append(thread)

        logging.info("Waiting for MOVING push notification ...")
        # this will consume the notification in one of the connections
        # and will handle the states of the rest
        ClientValidations.wait_push_notification(client, timeout=BIND_TIMEOUT)
        # set the event to stop the command execution threads
        moving_event.set()

        # wait for all command execution threads to stop
        for thread in commands_threads:
            thread.join()

        # validate that all connections are either disconnected
        # or connected to the new address
        connections = self._get_all_connections_in_pool(client)
        for conn in connections:
            if conn._sock is not None:
                assert conn.get_resolved_ip() == conn.host
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn._sock.gettimeout() == RELAX_TIMEOUT
                assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()
            else:
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAX_TIMEOUT
                assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()

        # validate no errors were raised in the command execution threads
        assert errors.empty(), f"Errors occurred in threads: {errors.queue}"

        logging.info("Waiting for moving ttl to expire")
        time.sleep(DEFAULT_BIND_TTL)
        bind_thread.join()

    @pytest.mark.timeout(300)  # 5 minutes timeout
    def test_new_connections_receive_moving(
        self,
        client_maint_events: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_events.connection_pool.get_connection()

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node,
                self.empty_node,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        self._validate_maintenance_state(
            client_maint_events, expected_matching_conns_count=1
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT, connection=first_conn
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
            client_maint_events, timeout=BIND_TIMEOUT, connection=first_conn
        )

        old_address = first_conn._sock.getpeername()[0]
        logging.info(f"The node address before bind: {old_address}")
        logging.info(
            "Creating new client to connect to the same node - new connections to this node should receive the moving event..."
        )

        endpoint_type = EndpointType.EXTERNAL_IP
        # create new client with new pool that should also receive the moving event
        new_client = _get_client_maint_events(
            endpoints_config=endpoints_config,
            endpoint_type=endpoint_type,
            host_config=old_address,
        )

        # the moving notification will be consumed as
        # part of the client connection setup, so we don't need
        # to wait for it explicitly with wait_push_notification
        logging.info(
            "Creating one connection in the new pool that should receive the moving event."
        )
        new_client_conn = new_client.connection_pool.get_connection()

        logging.info("Validating connections states during MOVING ...")
        self._validate_moving_state(
            new_client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=0,
        )

        logging.info("Waiting for moving thread to be completed ...")
        bind_thread.join()

        new_client.connection_pool.release(new_client_conn)
        new_client.close()

        client_maint_events.connection_pool.release(first_conn)

    @pytest.mark.timeout(300)  # 5 minutes timeout
    def test_new_connections_receive_migrating(
        self,
        client_maint_events: Redis,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info("Creating one connection in the pool.")
        first_conn = client_maint_events.connection_pool.get_connection()

        logging.info("Executing rladmin migrate command...")
        migrate_thread = Thread(
            target=self._execute_migration,
            name="migrate_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node,
                self.empty_node,
            ),
        )
        migrate_thread.start()

        logging.info("Waiting for MIGRATING push notifications...")
        # this will consume the notification in the provided connection
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )

        self._validate_maintenance_state(
            client_maint_events, expected_matching_conns_count=1
        )

        # validate that new connections will also receive the migrating event
        # it should be received as part of the client connection setup flow
        logging.info(
            "Creating second connection that should receive the migrating event as well."
        )
        second_connection = client_maint_events.connection_pool.get_connection()
        self._validate_maintenance_state(
            client_maint_events, expected_matching_conns_count=2
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT, connection=first_conn
        )
        ClientValidations.wait_push_notification(
            client_maint_events, timeout=MIGRATE_TIMEOUT, connection=second_connection
        )

        migrate_thread.join()

        client_maint_events.connection_pool.release(first_conn)
        client_maint_events.connection_pool.release(second_connection)

    @pytest.mark.timeout(300)
    def test_disabled_handling_during_migrating_and_moving(
        self,
        fault_injector_client: FaultInjectorClient,
        endpoints_config: Dict[str, Any],
    ):
        logging.info("Creating client with disabled notifications.")
        client = _get_client_maint_events(
            endpoints_config=endpoints_config,
            enable_maintenance_events=False,
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
                self.target_node,
                self.empty_node,
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

        # validate that new connections will not receive the migrating event
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

        # validate that new connections will also receive the moving event
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
        Test command execution during migrating and moving events.

        This test validates that:
        1. Commands can be executed during MIGRATING and MOVING events
        2. Commands are not blocked by the events
        3. Commands are executed successfully
        """
        errors = Queue()
        execution_duration = 180
        socket_timeout = 0.5

        client = _get_client_maint_events(
            endpoints_config=endpoints_config,
            endpoint_type=endpoint_type,
            disable_retries=True,
            socket_timeout=socket_timeout,
            enable_maintenance_events=True,
        )

        migrate_and_bind_thread = Thread(
            target=self._execute_migrate_bind_flow,
            name="migrate_and_bind_thread",
            args=(
                fault_injector_client,
                endpoints_config,
                self.target_node,
                self.empty_node,
                self.endpoint_id,
            ),
        )
        migrate_and_bind_thread.start()

        def execute_commands(duration: int, errors: Queue):
            start = time.time()
            while time.time() - start < duration:
                try:
                    client.set("key", "value")
                    client.get("key")
                except Exception as e:
                    errors.put(
                        f"Command failed in thread {threading.current_thread().name}: {e}"
                    )

        threads = []
        for _ in range(10):
            thread = Thread(
                target=execute_commands,
                name="command_execution_thread",
                args=(
                    execution_duration,
                    errors,
                ),
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        migrate_and_bind_thread.join()

        assert errors.empty(), f"Errors occurred in threads: {errors.queue}"
