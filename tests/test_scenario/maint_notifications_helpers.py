import binascii
import logging
import time
from typing import Any, Dict, Optional, Tuple, Union
import pytest
from redis import RedisCluster

from redis.client import Redis
from redis.connection import Connection
from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
    NodeInfo,
    SlotMigrateEffects,
)


class ClientValidations:
    @staticmethod
    def get_default_connection(redis_client: Union[Redis, RedisCluster]) -> Connection:
        """Get a random connection from the pool."""
        if isinstance(redis_client, RedisCluster):
            return redis_client.get_default_node().redis_connection.connection_pool.get_connection()
        if isinstance(redis_client, Redis):
            return redis_client.connection_pool.get_connection()
        raise ValueError(f"Unsupported redis client type: {type(redis_client)}")

    @staticmethod
    def release_connection(
        redis_client: Union[Redis, RedisCluster], connection: Connection
    ):
        """Release a connection back to the pool."""
        if isinstance(redis_client, RedisCluster):
            node_address = connection.host + ":" + str(connection.port)
            node = redis_client.get_node(node_address)
            if node is None:
                raise ValueError(
                    f"Node not found in cluster for address: {node_address}"
                )
            node.redis_connection.connection_pool.release(connection)
        elif isinstance(redis_client, Redis):
            redis_client.connection_pool.release(connection)
        else:
            raise ValueError(f"Unsupported redis client type: {type(redis_client)}")

    @staticmethod
    def wait_push_notification(
        redis_client: Union[Redis, RedisCluster],
        timeout: int = 120,
        fail_on_timeout: bool = True,
        connection: Optional[Connection] = None,
    ):
        """Wait for a push notification to be received."""
        start_time = time.time()  # returns the time in seconds
        check_interval = 0.2  # Check more frequently during operations
        test_conn = (
            connection
            if connection
            else ClientValidations.get_default_connection(redis_client)
        )
        logging.info(f"Waiting for push notification on connection: {test_conn}")

        try:
            while time.time() - start_time < timeout:
                try:
                    if test_conn.can_read(timeout=0.2):
                        # reading is important, it triggers the push notification
                        push_response = test_conn.read_response(push_request=True)
                        logging.debug(
                            f"Push notification has been received. Response: {push_response}"
                        )
                        if test_conn.should_reconnect():
                            logging.debug("Connection is marked for reconnect")
                        return
                except Exception as e:
                    logging.error(f"Error reading push notification: {e}")
                    break
                time.sleep(check_interval)
            if fail_on_timeout:
                pytest.fail(
                    f"Timeout waiting for push notification: waiting > {time.time() - start_time} seconds."
                )
        finally:
            # Release the connection back to the pool
            try:
                if not connection:
                    ClientValidations.release_connection(redis_client, test_conn)
            except Exception as e:
                logging.error(f"Error releasing connection: {e}")


class ClusterOperations:
    @staticmethod
    def find_database_id_by_name(
        fault_injector: FaultInjectorClient,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        """Find the database ID by name."""
        return fault_injector.find_database_id_by_name(
            database_name, force_cluster_info_refresh
        )

    @staticmethod
    def find_target_node_and_empty_node(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        force_cluster_info_refresh: bool = True,
    ) -> Tuple[NodeInfo, NodeInfo]:
        """Find the node with master shards and the node with no shards.

        Returns:
            tuple: (target_node, empty_node) where target_node has master shards
                   and empty_node has no shards
        """
        return fault_injector.find_target_node_and_empty_node(
            endpoint_config, force_cluster_info_refresh
        )

    @staticmethod
    def find_endpoint_for_bind(
        fault_injector: FaultInjectorClient,
        endpoint_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> str:
        """Find the endpoint ID from cluster status.

        Returns:
            str: The endpoint ID (e.g., "1:1")
        """
        return fault_injector.find_endpoint_for_bind(
            endpoint_name, force_cluster_info_refresh
        )

    @staticmethod
    def execute_failover(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Execute failover command and wait for completion."""
        return fault_injector.execute_failover(endpoint_config, timeout)

    @staticmethod
    def execute_migrate(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
        skip_end_notification: bool = False,
    ) -> str:
        """Execute rladmin migrate command and wait for completion."""
        return fault_injector.execute_migrate(
            endpoint_config, target_node, empty_node, skip_end_notification
        )

    @staticmethod
    def execute_rebind(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        endpoint_id: str,
    ) -> str:
        """Execute rladmin bind endpoint command and wait for completion."""
        return fault_injector.execute_rebind(endpoint_config, endpoint_id)

    @staticmethod
    def trigger_effect(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        """Execute fault injector action that will trigger the desired effect.

        Args:
            fault_injector: The fault injector client to use
            endpoint_config: Endpoint configuration dictionary
            effect_name: The effect to trigger (e.g., SlotMigrateEffects enum value)
            trigger_name: Optional trigger/variant name
            source_node: Optional source node ID
            target_node: Optional target node ID
            skip_end_notification: Whether to skip end notification

        Returns:
            str: Action ID for tracking the operation
        """
        return fault_injector.trigger_effect(
            endpoint_config=endpoint_config,
            effect_name=effect_name,
            trigger_name=trigger_name,
            source_node=source_node,
            target_node=target_node,
            skip_end_notification=skip_end_notification,
        )


class KeyGenerationHelpers:
    TOTAL_SLOTS = 16384

    @staticmethod
    def redis_crc16(data: bytes) -> int:
        """
        Redis-compatible CRC16 (CRC-CCITT)
        """
        return binascii.crc_hqx(data, 0)

    @staticmethod
    def redis_slot(key: str) -> int:
        """
        Compute Redis Cluster hash slot for a key
        """
        start = key.find("{")
        if start != -1:
            end = key.find("}", start + 1)
            if end != -1 and end > start + 1:
                key = key[start + 1 : end]

        return (
            KeyGenerationHelpers.redis_crc16(key.encode("utf-8"))
            % KeyGenerationHelpers.TOTAL_SLOTS
        )

    @staticmethod
    def generate_key(slot_number: int, prefix: str = "key") -> str:
        """
        Generate a Redis key that hashes to the given slot
        """
        if not (0 <= slot_number < KeyGenerationHelpers.TOTAL_SLOTS):
            raise ValueError("slot_number must be between 0 and 16383")

        i = 0
        while True:
            hashtag = f"{slot_number}-{i}"
            candidate = f"{prefix}:{{{hashtag}}}"
            if KeyGenerationHelpers.redis_slot(candidate) == slot_number:
                return candidate
            i += 1

    @staticmethod
    def generate_keys_for_all_shards(
        shards_count: int, prefix: str = "key", keys_per_shard: int = 1
    ) -> list:
        """
        Generate keys for all shards based on slot ranges.

        Divides the total slots (16384) evenly across shards and generates
        keys for each shard range.

        Args:
            shards_count: Number of shards in the cluster
            prefix: Prefix for generated keys
            keys_per_shard: Number of keys to generate per shard

        Returns:
            List of generated keys distributed across all shards
        """
        keys = []
        slots_per_shard = KeyGenerationHelpers.TOTAL_SLOTS // shards_count

        for shard_index in range(shards_count):
            # Calculate slot range for this shard
            start_slot = shard_index * slots_per_shard

            # Last shard gets any remaining slots
            if shard_index == shards_count - 1:
                end_slot = KeyGenerationHelpers.TOTAL_SLOTS - 1
            else:
                end_slot = start_slot + slots_per_shard - 1

            # Generate keys for this shard's slot range
            for i in range(keys_per_shard):
                # Pick a slot within this shard's range
                slot_number = start_slot + (i % (end_slot - start_slot + 1))
                keys.append(KeyGenerationHelpers.generate_key(slot_number, prefix))

        return keys
