"""Tests for Redis Enterprise maintenance push notifications — async standalone client."""

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Literal, Optional, Union

import pytest
import pytest_asyncio

from redis.asyncio import Redis
from redis.maint_notifications import (
    EndpointType,
    MaintenanceState,
)
from tests.test_asyncio.test_scenario.conftest import (
    _get_async_client_maint_notifications,
    get_async_standalone_client_maint_notifications,
)
from tests.test_asyncio.test_scenario.maint_notifications_helpers import (
    AsyncClientValidations,
)
from tests.test_scenario.conftest import (
    CLIENT_TIMEOUT,
    RELAXED_TIMEOUT,
    _FAULT_INJECTOR_STANDALONE_CLIENT,
    use_mock_proxy,
)
from tests.test_asyncio.test_scenario.async_fault_injector_client import (
    AsyncFaultInjectorClient,
    AsyncProxyServerFaultInjector,
)
from tests.test_scenario.fault_injector_client import (
    SlotMigrateEffects,
    TopologyChangeStandaloneEffects,
)
from tests.test_scenario.maint_notifications_helpers import (
    generate_params,
    is_endpoint_configured_correctly,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S:%f",
)

logging.getLogger("redis.asyncio.maint_notifications").setLevel(logging.DEBUG)

STANDALONE_MAINT_TIMEOUT = 60
SLOT_SHUFFLE_TIMEOUT = 120

DEFAULT_BIND_TTL = 15
DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT = 1


class TestAsyncPushNotificationsBase:
    async def _get_all_connections_in_pool(self, client: Redis) -> List:
        connections = []
        async with client.connection_pool._get_pool_lock():
            for conn in client.connection_pool._get_free_connections():
                connections.append(conn)
            for conn in client.connection_pool._get_in_use_connections():
                connections.append(conn)
        return connections

    async def _validate_maintenance_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        matching_conns_count = 0
        connections = await self._get_all_connections_in_pool(client)

        for conn in connections:
            if (
                conn.is_connected
                and conn.socket_timeout == RELAXED_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MAINTENANCE
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

    async def _validate_moving_state(
        self,
        client: Redis,
        configured_endpoint_type: EndpointType,
        expected_matching_connected_conns_count: int,
        expected_matching_disconnected_conns_count: int,
        fault_injector_client: AsyncFaultInjectorClient,
    ):
        matching_connected_conns_count = 0
        matching_disconnected_conns_count = 0
        # No outer lock here: asyncio.Lock is non-reentrant and
        # _get_all_connections_in_pool already holds the lock for its snapshot.
        # Single-threaded event loop guarantees no interleaving after the await.
        connections = await self._get_all_connections_in_pool(client)
        for conn in connections:
            endpoint_configured_correctly = is_endpoint_configured_correctly(
                conn, configured_endpoint_type, fault_injector_client
            )

            if (
                conn.is_connected
                and conn.socket_timeout == RELAXED_TIMEOUT
                and conn.maintenance_state == MaintenanceState.MOVING
                and endpoint_configured_correctly
            ):
                matching_connected_conns_count += 1
            elif (
                not conn.is_connected
                and conn.maintenance_state == MaintenanceState.MOVING
                and conn.socket_timeout == RELAXED_TIMEOUT
                and endpoint_configured_correctly
            ):
                matching_disconnected_conns_count += 1

        assert matching_connected_conns_count == expected_matching_connected_conns_count
        assert (
            matching_disconnected_conns_count
            == expected_matching_disconnected_conns_count
        )

    async def _validate_default_state(
        self,
        client: Redis,
        expected_matching_conns_count: Union[int, Literal["all"]],
        configured_timeout: float = CLIENT_TIMEOUT,
    ):
        matching_conns_count = 0
        connections = await self._get_all_connections_in_pool(client)
        logging.info(f"Validating {len(connections)} connections")

        for conn in connections:
            if not conn.is_connected:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == configured_timeout
                    and conn.host == getattr(conn, "orig_host_address", conn.host)
                ):
                    matching_conns_count += 1
                else:
                    logging.debug(
                        f"Connection not matching default state: "
                        f"maintenance_state={conn.maintenance_state}, "
                        f"socket_timeout={conn.socket_timeout}, "
                        f"host={conn.host}, "
                        f"orig_host_address={getattr(conn, 'orig_host_address', None)}"
                    )
            elif (
                conn.socket_timeout == configured_timeout
                and conn.maintenance_state == MaintenanceState.NONE
                and conn.host == getattr(conn, "orig_host_address", conn.host)
            ):
                matching_conns_count += 1
            else:
                logging.debug(
                    f"Connection not matching default state: "
                    f"maintenance_state={conn.maintenance_state}, "
                    f"socket_timeout={conn.socket_timeout}, "
                    f"host={conn.host}, "
                    f"orig_host_address={getattr(conn, 'orig_host_address', None)}"
                )

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

    async def _validate_default_notif_disabled_state(
        self, client: Redis, expected_matching_conns_count: int
    ):
        matching_conns_count = 0
        connections = await self._get_all_connections_in_pool(client)

        for conn in connections:
            if not conn.is_connected:
                if (
                    conn.maintenance_state == MaintenanceState.NONE
                    and conn.socket_timeout == CLIENT_TIMEOUT
                    and not hasattr(conn, "orig_host_address")
                ):
                    matching_conns_count += 1
            elif (
                conn.socket_timeout == CLIENT_TIMEOUT
                and conn.maintenance_state == MaintenanceState.NONE
                and not hasattr(conn, "orig_host_address")
            ):
                matching_conns_count += 1
        assert matching_conns_count == expected_matching_conns_count

    async def delete_prev_db(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        db_name: str,
    ):
        try:
            logging.info(f"Deleting database if exists: {db_name}")
            existing_db_id = await fault_injector_client.find_database_id_by_name(
                db_name
            )
            if existing_db_id:
                await fault_injector_client.delete_database(existing_db_id)
                logging.info(f"Deleted database: {db_name}")
            else:
                logging.info(f"Database {db_name} does not exist.")
        except Exception as e:
            logging.error(f"Failed to delete database {db_name}: {e}")

    async def create_db(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        bdb_config: Dict[str, Any],
    ):
        try:
            logging.info(f"Creating database: \n{json.dumps(bdb_config, indent=2)}")
            cluster_endpoint_config = await fault_injector_client.create_database(
                bdb_config
            )
            return cluster_endpoint_config
        except Exception as e:
            pytest.fail(f"Failed to create database: {e}")

    async def _trigger_effect(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        endpoints_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        target_node: Optional[str] = None,
        empty_node: Optional[str] = None,
        skip_end_notification: bool = False,
        timeout: int = SLOT_SHUFFLE_TIMEOUT,
    ):
        action_id = await fault_injector_client.trigger_effect(
            endpoint_config=endpoints_config,
            effect_name=effect_name,
            trigger_name=trigger_name,
            source_node=target_node,
            target_node=empty_node,
            skip_end_notification=skip_end_notification,
        )
        result = await fault_injector_client.get_operation_result(
            action_id,
            timeout=timeout,
        )
        logging.debug(f"Action execution result: {result}")


class TestAsyncStandaloneClientPushNotificationsWithEffectTriggerBase(
    TestAsyncPushNotificationsBase
):
    async def setup_env(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        db_config: Dict[str, Any],
        endpoint_type: Optional[EndpointType] = None,
    ):
        self._fault_injector = fault_injector_client
        await self.delete_prev_db(fault_injector_client, db_config["name"])

        db_endpoint_config = await self.create_db(fault_injector_client, db_config)
        self._bdb_name = db_config["name"]

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )
        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        self._client = get_async_standalone_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            disable_retries=True,
            socket_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
            enable_maintenance_notifications=True,
            endpoint_type=endpoint_type,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )
        return self._client, db_endpoint_config

    @pytest_asyncio.fixture(autouse=True)
    async def setup_and_cleanup(self):
        self.maintenance_ops_tasks = []
        self._bdb_name = None
        self._fault_injector = None
        self._client: Redis | None = None

        yield

        logging.info("Starting cleanup...")

        logging.info("Waiting for maintenance operation tasks to finish...")
        if self.maintenance_ops_tasks:
            await asyncio.gather(*self.maintenance_ops_tasks, return_exceptions=True)

        if self._client is not None:
            await self._client.aclose()

        if self._bdb_name and self._fault_injector:
            await self.delete_prev_db(self._fault_injector, self._bdb_name)

        logging.info("Cleanup finished")


class TestAsyncStandaloneClientPushNotificationsHandlingWithEffectTrigger(
    TestAsyncStandaloneClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP],
        ),
    )
    async def test_notification_handling_during_data_movements_no_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config
        )

        logging.info("Creating one connection in the pool.")
        conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for opening push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn,
        )

        logging.info("Validating connection maintenance state...")
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn.socket_timeout == RELAXED_TIMEOUT

        logging.info("Waiting for closing push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn,
        )

        logging.info("Validating connection default state is restored...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        logging.info("Releasing connection back to the pool...")
        await client.connection_pool.release(conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    async def test_notification_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config
        )

        conn = await client.connection_pool.get_connection()
        await client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection migrating state...")
        conn = await client.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MAINTENANCE
        assert conn.socket_timeout == RELAXED_TIMEOUT
        await client.connection_pool.release(conn)

        logging.info("Waiting for MIGRATED push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection states...")
        conn = await client.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT
        await client.connection_pool.release(conn)

        logging.info("Waiting for MOVING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        logging.info("Validating connection states...")
        conn = await client.connection_pool.get_connection()
        assert conn.maintenance_state == MaintenanceState.MOVING
        assert conn.socket_timeout == RELAXED_TIMEOUT

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states...")
        assert conn.maintenance_state == MaintenanceState.NONE
        assert conn.socket_timeout == DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT

        await client.connection_pool.release(conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name, endpoint_type",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
            endpoint_types=[
                EndpointType.EXTERNAL_FQDN,
                EndpointType.EXTERNAL_IP,
                EndpointType.NONE,
            ],
        ),
    )
    async def test_timeout_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating three connections in the pool.")
        conns = []
        for _ in range(3):
            conns.append(await client.connection_pool.get_connection())
        for conn in conns:
            await client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        await self._validate_maintenance_state(client, expected_matching_conns_count=1)
        await self._validate_default_state(
            client,
            expected_matching_conns_count=2,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        logging.info("Waiting for MIGRATED push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )

        logging.info("Validating connection states after MIGRATED ...")
        await self._validate_default_state(
            client,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        logging.info("Waiting for MOVING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            await asyncio.sleep(fault_injector_client.get_moving_ttl() / 2 + 1)

        logging.info("Validating connections states...")
        await self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=0,
            expected_matching_disconnected_conns_count=3,
            fault_injector_client=fault_injector_client,
        )
        conn = await client.connection_pool.get_connection()
        await self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=2,
            fault_injector_client=fault_injector_client,
        )
        await client.connection_pool.release(conn)

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states...")
        await self._validate_default_state(
            client,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name, endpoint_type",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
            endpoint_types=[
                EndpointType.EXTERNAL_FQDN,
                EndpointType.EXTERNAL_IP,
                EndpointType.NONE,
            ],
        ),
    )
    async def test_connection_handling_during_data_movements_with_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing with endpoint type: {endpoint_type}")
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        await client.connection_pool.release(first_conn)

        logging.info("Waiting for MOVING push notifications on random connection ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            expected_state=MaintenanceState.MOVING,
        )

        if endpoint_type == EndpointType.NONE:
            logging.info(
                "Waiting for moving ttl/2 to expire to validate proactive reconnection"
            )
            await asyncio.sleep(fault_injector_client.get_moving_ttl() / 2 + 1)

        connections = []
        for _ in range(3):
            connections.append(await client.connection_pool.get_connection())
        for conn in connections:
            logging.debug(f"Releasing connection {conn}. {conn.maintenance_state}")
            await client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        await self._validate_moving_state(
            client,
            endpoint_type,
            expected_matching_connected_conns_count=3,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(fault_injector_client.get_moving_ttl())

        logging.info("Validating connection states after MOVING has expired ...")
        await self._validate_default_state(
            client,
            expected_matching_conns_count=3,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP],
        ),
    )
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    async def test_new_connections_receive_notifications_no_conn_drop(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for opening push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )
        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info(
            "Creating second connection that should receive the opening notification as well."
        )
        second_conn = await client.connection_pool.get_connection()
        await self._validate_maintenance_state(client, expected_matching_conns_count=2)

        logging.info("Waiting for closing push notifications on both connections ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=second_conn,
        )

        await self._validate_default_state(
            client,
            expected_matching_conns_count=2,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        await client.connection_pool.release(first_conn)
        await client.connection_pool.release(second_conn)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    async def test_old_connection_shutdown_during_moving(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        endpoint_type = EndpointType.EXTERNAL_IP
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        conn = await client.connection_pool.get_connection()
        await client.connection_pool.release(conn)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )
        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notification ...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=STANDALONE_MAINT_TIMEOUT
        )
        await self._validate_default_state(
            client,
            expected_matching_conns_count=1,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        moving_event = asyncio.Event()
        errors = asyncio.Queue()

        async def execute_commands():
            while not moving_event.is_set():
                try:
                    await client.set("key", "value")
                    await client.get("key")
                except Exception as e:
                    await errors.put(
                        f"Command failed in task {asyncio.current_task().get_name()}: {e}"
                    )
                await asyncio.sleep(0.001)

        conn_to_check_moving = await client.connection_pool.get_connection()

        threads_count = 10
        tasks = [
            asyncio.create_task(execute_commands(), name=f"cmd_task_{i}")
            for i in range(threads_count)
        ]

        logging.info("Waiting for MOVING push notification ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=conn_to_check_moving,
            expected_state=MaintenanceState.MOVING,
        )

        logging.info("Setting moving event...")
        moving_event.set()
        await client.connection_pool.release(conn_to_check_moving)

        await asyncio.gather(*tasks)

        logging.info("All command tasks finished. Validating connections states...")
        connections = await self._get_all_connections_in_pool(client)
        for conn in connections:
            if conn.is_connected:
                assert conn.get_resolved_ip() == conn.host
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, AsyncProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()
            else:
                assert conn.maintenance_state == MaintenanceState.MOVING
                assert conn.socket_timeout == RELAXED_TIMEOUT
                if not isinstance(fault_injector_client, AsyncProxyServerFaultInjector):
                    assert conn.host != conn.orig_host_address
                assert not conn.should_reconnect()

        error_items = []
        while not errors.empty():
            error_items.append(errors.get_nowait())
        assert not error_items, f"Errors occurred in tasks: {error_items}"

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    @pytest.mark.skipif(
        use_mock_proxy(),
        reason="Mock proxy doesn't support sending notifications to new connections.",
    )
    async def test_new_connections_receive_moving(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        endpoint_type = EndpointType.EXTERNAL_IP
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        await self._validate_maintenance_state(client, expected_matching_conns_count=1)

        logging.info("Waiting for MIGRATED push notifications ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
        )

        logging.info("Waiting for MOVING push notifications on random connection ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=STANDALONE_MAINT_TIMEOUT,
            connection=first_conn,
            expected_state=MaintenanceState.MOVING,
        )

        assert first_conn.is_connected, (
            "Connection must be active after receiving MOVING notification"
        )
        old_address = first_conn.getpeername()
        logging.info(f"The node address before bind: {old_address}")

        auth_ssl_client_certs_config_info = db_config.get(
            "authentication_ssl_client_certs", None
        )
        auth_ssl_client_certs = (
            True
            if auth_ssl_client_certs_config_info
            and auth_ssl_client_certs_config_info[0].get("client_cert") is not None
            else False
        )

        logging.info(
            "Creating new client pointing at old address — "
            "new connections should receive the moving notification..."
        )
        new_client = _get_async_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            endpoint_type=endpoint_type,
            host_config=old_address,
            socket_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
            auth_ssl_client_certs=auth_ssl_client_certs,
        )

        logging.info(
            "Creating one connection in the new pool that should receive the moving notification."
        )
        new_client_conn = await new_client.connection_pool.get_connection()

        logging.info("Validating connections states during MOVING ...")
        await self._validate_moving_state(
            new_client,
            endpoint_type,
            expected_matching_connected_conns_count=1,
            expected_matching_disconnected_conns_count=0,
            fault_injector_client=fault_injector_client,
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        await new_client.connection_pool.release(new_client_conn)
        await new_client.aclose()

        await client.connection_pool.release(first_conn)

    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP],
        ),
    )
    async def test_disabled_handling_during_migrating_and_moving(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
    ):
        logging.info(f"DB name: {db_name}")

        self._fault_injector = fault_injector_client
        await self.delete_prev_db(fault_injector_client, db_config["name"])
        db_endpoint_config = await self.create_db(fault_injector_client, db_config)
        self._bdb_name = db_config["name"]

        logging.info("Creating client with disabled notifications.")
        self._client = client = get_async_standalone_client_maint_notifications(
            endpoints_config=db_endpoint_config,
            disable_retries=True,
            enable_maintenance_notifications=False,
        )

        logging.info("Creating one connection in the pool.")
        first_conn = await client.connection_pool.get_connection()

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        logging.info("Waiting for MIGRATING push notifications...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )

        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=1
        )

        logging.info(
            "Creating second connection — expect it not to receive MIGRATING either."
        )
        second_conn = await client.connection_pool.get_connection()
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_conn
        )

        logging.info(
            "Validating connection states after MIGRATING for both connections ..."
        )
        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=2
        )

        logging.info("Waiting for MIGRATED push notifications on both connections ...")
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=first_conn
        )
        await AsyncClientValidations.wait_push_notification(
            client, timeout=5, fail_on_timeout=False, connection=second_conn
        )

        await client.connection_pool.release(first_conn)
        await client.connection_pool.release(second_conn)

        logging.info("Waiting for MOVING push notifications on random connection ...")
        await AsyncClientValidations.wait_push_notification(
            client,
            timeout=10,
            fail_on_timeout=False,
        )

        connections = []
        for _ in range(3):
            connections.append(await client.connection_pool.get_connection())
        for conn in connections:
            await client.connection_pool.release(conn)

        logging.info("Validating connections states during MOVING ...")
        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        logging.info("Waiting for moving ttl to expire")
        await asyncio.sleep(DEFAULT_BIND_TTL)

        logging.info("Validating connection states after MOVING has expired ...")
        await self._validate_default_notif_disabled_state(
            client, expected_matching_conns_count=3
        )

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)


class TestAsyncStandaloneClientCommandsExecutionWithPushNotificationsWithEffectTrigger(
    TestAsyncStandaloneClientPushNotificationsWithEffectTriggerBase
):
    @pytest.mark.timeout(300)
    @pytest.mark.parametrize(
        "effect_name, trigger, db_config, db_name, endpoint_type",
        generate_params(
            _FAULT_INJECTOR_STANDALONE_CLIENT,
            [
                TopologyChangeStandaloneEffects.DATA_MOVEMENT_NO_CONN_DROP,
                TopologyChangeStandaloneEffects.DATA_MOVEMENT_CONN_DROP,
                TopologyChangeStandaloneEffects.CONN_DROP,
                TopologyChangeStandaloneEffects.DNS_RESOLUTION_CHANGE,
            ],
            endpoint_types=[
                EndpointType.EXTERNAL_FQDN,
                EndpointType.EXTERNAL_IP,
                EndpointType.NONE,
            ],
        ),
    )
    async def test_command_execution_during_maintenance(
        self,
        fault_injector_client: AsyncFaultInjectorClient,
        effect_name: TopologyChangeStandaloneEffects,
        trigger: str,
        db_config: dict[str, Any],
        db_name: str,
        endpoint_type: EndpointType,
    ):
        errors = asyncio.Queue()
        if isinstance(fault_injector_client, AsyncProxyServerFaultInjector):
            execution_duration = 20
        else:
            execution_duration = 60

        logging.info(f"DB name: {db_name}")
        logging.info(f"Testing timeout handling for endpoint type: {endpoint_type}")
        client, db_endpoint_config = await self.setup_env(
            fault_injector_client, db_config, endpoint_type=endpoint_type
        )

        async def execute_commands(duration: int):
            start = time.time()
            while time.time() - start < duration:
                try:
                    await client.set("key", "value")
                    await client.get("key")
                except Exception as e:
                    logging.error(
                        f"Error in task {asyncio.current_task().get_name()}: {e}"
                    )
                    await errors.put(
                        f"Command failed in task "
                        f"{asyncio.current_task().get_name()}: {e}"
                    )
                await asyncio.sleep(0.001)
            logging.debug(f"{asyncio.current_task().get_name()}: task ended")

        tasks = [
            asyncio.create_task(
                execute_commands(execution_duration), name=f"cmd_task_{i}"
            )
            for i in range(10)
        ]

        logging.info("Waiting for tasks to start and have a few cycles executed ...")
        await asyncio.sleep(3)

        logging.info("Executing FI command that triggers the desired effect...")
        trigger_task = asyncio.create_task(
            self._trigger_effect(
                fault_injector_client,
                db_endpoint_config,
                effect_name,
                trigger,
            )
        )
        self.maintenance_ops_tasks.append(trigger_task)

        await asyncio.gather(*tasks)

        await trigger_task
        self.maintenance_ops_tasks.remove(trigger_task)

        await self._validate_default_state(
            client,
            expected_matching_conns_count=10,
            configured_timeout=DEFAULT_STANDALONE_CLIENT_SOCKET_TIMEOUT,
        )

        error_items = []
        while not errors.empty():
            error_items.append(errors.get_nowait())
        assert not error_items, f"Errors occurred in tasks: {error_items}"
