import binascii
import logging
from typing import Any, Dict, Optional, Tuple

from tests.test_scenario.fault_injector_client import (
    FaultInjectorClient,
    NodeInfo,
    SlotMigrateEffects,
)


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
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Execute rladmin migrate command and wait for completion."""
        migrate_action_id = fault_injector.execute_migrate(
            endpoint_config, target_node, empty_node, skip_end_notification
        )

        migrate_result = fault_injector.get_operation_result(
            migrate_action_id, timeout=timeout
        )
        logging.debug(f"Migration result: {migrate_result}")
        return migrate_result

    @staticmethod
    def execute_rebind(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        endpoint_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Execute rladmin bind endpoint command and wait for completion."""
        bind_action_id = fault_injector.execute_rebind(endpoint_config, endpoint_id)

        bind_result = fault_injector.get_operation_result(
            bind_action_id, timeout=timeout
        )
        logging.debug(f"Bind result: {bind_result}")
        return bind_result

    @staticmethod
    def get_slot_migrate_triggers(
        fault_injector: FaultInjectorClient,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        """Get available triggers(trigger name + db example config) for a slot migration effect."""
        return fault_injector.get_slot_migrate_triggers(effect_name)

    @staticmethod
    def trigger_effect(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
        timeout: int = 120,
    ) -> Dict[str, Any]:
        """Execute fault injector action that will trigger the desired effect.

        Args:
            fault_injector: The fault injector client to use
            endpoint_config: Endpoint configuration dictionary
            effect_name: The effect to trigger (e.g., SlotMigrateEffects enum value)
            trigger_name: Optional trigger/variant name
            source_node: Optional source node ID
            target_node: Optional target node ID
            skip_end_notification: Whether to skip end notification
            timeout: Operation completion timeout

        Returns:
            dict: Operation result
        """
        trigger_effect_action_id = fault_injector.trigger_effect(
            endpoint_config=endpoint_config,
            effect_name=effect_name,
            trigger_name=trigger_name,
            source_node=source_node,
            target_node=target_node,
            skip_end_notification=skip_end_notification,
        )

        trigger_effect_result = fault_injector.get_operation_result(
            trigger_effect_action_id,
            timeout=timeout,
        )
        logging.debug(f"Action execution result: {trigger_effect_result}")
        return trigger_effect_result


def delete_database_if_exists(
    fault_injector_client: FaultInjectorClient, database_name: str
):
    try:
        bdb_id = ClusterOperations.find_database_id_by_name(
            fault_injector_client, database_name
        )
    except Exception as exc:
        logging.info("Database %s not found during cleanup: %s", database_name, exc)
        return

    if bdb_id:
        fault_injector_client.delete_database(bdb_id)


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
