import logging
import time
from typing import Any, Dict, Optional, Tuple
import pytest

from redis.client import Redis
from redis.connection import Connection
from tests.test_scenario.fault_injector_client import (
    ActionRequest,
    ActionType,
    FaultInjectorClient,
)


class TaskStatuses:
    """Class to hold completed statuses constants."""

    FAILED = "failed"
    FINISHED = "finished"
    SUCCESS = "success"
    RUNNING = "running"

    COMPLETED_STATUSES = [FAILED, FINISHED, SUCCESS]


class ClientValidations:
    @staticmethod
    def wait_push_notification(
        redis_client: Redis,
        timeout: int = 120,
        connection: Optional[Connection] = None,
    ):
        """Wait for a push notification to be received."""
        start_time = time.time()
        check_interval = 1  # Check more frequently during operations
        test_conn = (
            connection if connection else redis_client.connection_pool.get_connection()
        )

        try:
            while time.time() - start_time < timeout:
                try:
                    if test_conn.can_read(timeout=0.5):
                        # reading is important, it triggers the push notification
                        push_response = test_conn.read_response(push_request=True)
                        logging.debug(
                            f"Push notification has been received. Response: {push_response}"
                        )
                        return
                except Exception as e:
                    logging.error(f"Error reading push notification: {e}")
                    break
                time.sleep(check_interval)
        finally:
            # Release the connection back to the pool
            try:
                if not connection:
                    redis_client.connection_pool.release(test_conn)
            except Exception as e:
                logging.error(f"Error releasing connection: {e}")


class ClusterOperations:
    @staticmethod
    def get_operation_result(
        fault_injector: FaultInjectorClient,
        action_id: str,
        timeout: int = 60,
    ) -> Tuple[str, dict]:
        """Get the result of a specific action"""
        start_time = time.time()
        check_interval = 3
        while time.time() - start_time < timeout:
            try:
                status_result = fault_injector.get_action_status(action_id)
                operation_status = status_result.get("status", "unknown")

                if operation_status in TaskStatuses.COMPLETED_STATUSES:
                    logging.debug(
                        f"Operation {action_id} completed with status: "
                        f"{operation_status}"
                    )
                    return operation_status, status_result

                time.sleep(check_interval)
            except Exception as e:
                logging.warning(f"Error checking operation status: {e}")
                time.sleep(check_interval)
        else:
            raise TimeoutError(f"Timeout waiting for operation {action_id}")

    @staticmethod
    def get_cluster_nodes_info(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Get cluster nodes information from Redis Enterprise."""
        try:
            # Use rladmin status to get node information
            bdb_id = endpoint_config.get("bdb_id")
            get_status_action = ActionRequest(
                action_type=ActionType.EXECUTE_RLADMIN_COMMAND,
                parameters={
                    "rladmin_command": "status",
                    "bdb_id": bdb_id,
                },
            )
            trigger_action_result = fault_injector.trigger_action(get_status_action)
            action_id = trigger_action_result.get("action_id")
            if not action_id:
                raise ValueError(
                    f"Failed to trigger get cluster status action for bdb_id {bdb_id}: {trigger_action_result}"
                )

            status, action_status_check_response = (
                ClusterOperations.get_operation_result(
                    fault_injector, action_id, timeout=timeout
                )
            )

            if status != TaskStatuses.SUCCESS:
                pytest.fail(
                    f"Failed to get cluster nodes info: {action_status_check_response}"
                )
            logging.info(
                f"Completed cluster nodes info reading: {action_status_check_response}"
            )
            return action_status_check_response

        except Exception as e:
            pytest.fail(f"Failed to get cluster nodes info: {e}")

    @staticmethod
    def find_target_node_and_empty_node(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
    ) -> Tuple[str, str]:
        """Find the node with master shards and the node with no shards.

        Returns:
            tuple: (target_node, empty_node) where target_node has master shards
                   and empty_node has no shards
        """
        cluster_info = ClusterOperations.get_cluster_nodes_info(
            fault_injector, endpoint_config
        )
        output = cluster_info.get("output", {}).get("output", "")

        if not output:
            raise ValueError("No cluster status output found")

        # Parse the sections to find nodes with master shards and nodes with no shards
        lines = output.split("\n")
        shards_section_started = False
        nodes_section_started = False

        # Get all node IDs from CLUSTER NODES section
        all_nodes = set()
        nodes_with_shards = set()
        master_nodes = set()

        for line in lines:
            line = line.strip()

            # Start of CLUSTER NODES section
            if line.startswith("CLUSTER NODES:"):
                nodes_section_started = True
                continue
            elif line.startswith("DATABASES:"):
                nodes_section_started = False
                continue
            elif nodes_section_started and line and not line.startswith("NODE:ID"):
                # Parse node line: node:1  master 10.0.101.206 ... (ignore the role)
                parts = line.split()
                if len(parts) >= 1:
                    node_id = parts[0].replace("*", "")  # Remove * prefix if present
                    all_nodes.add(node_id)

            # Start of SHARDS section - only care about shard roles here
            if line.startswith("SHARDS:"):
                shards_section_started = True
                continue
            elif shards_section_started and line.startswith("DB:ID"):
                continue
            elif shards_section_started and line and not line.startswith("ENDPOINTS:"):
                # Parse shard line: db:1  m-standard  redis:1  node:2  master  0-8191  1.4MB  OK
                parts = line.split()
                if len(parts) >= 5:
                    node_id = parts[3]  # node:2
                    shard_role = parts[4]  # master/slave - this is what matters

                    nodes_with_shards.add(node_id)
                    if shard_role == "master":
                        master_nodes.add(node_id)
            elif line.startswith("ENDPOINTS:") or not line:
                shards_section_started = False

        # Find empty node (node with no shards)
        empty_nodes = all_nodes - nodes_with_shards

        logging.debug(f"All nodes: {all_nodes}")
        logging.debug(f"Nodes with shards: {nodes_with_shards}")
        logging.debug(f"Master nodes: {master_nodes}")
        logging.debug(f"Empty nodes: {empty_nodes}")

        if not empty_nodes:
            raise ValueError("No empty nodes (nodes without shards) found")

        if not master_nodes:
            raise ValueError("No nodes with master shards found")

        # Return the first available empty node and master node (numeric part only)
        empty_node = next(iter(empty_nodes)).split(":")[1]  # node:1 -> 1
        target_node = next(iter(master_nodes)).split(":")[1]  # node:2 -> 2

        return target_node, empty_node

    @staticmethod
    def find_endpoint_for_bind(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        timeout: int = 60,
    ) -> str:
        """Find the endpoint ID from cluster status.

        Returns:
            str: The endpoint ID (e.g., "1:1")
        """
        cluster_info = ClusterOperations.get_cluster_nodes_info(
            fault_injector, endpoint_config, timeout
        )
        output = cluster_info.get("output", {}).get("output", "")

        if not output:
            raise ValueError("No cluster status output found")

        # Parse the ENDPOINTS section to find endpoint ID
        lines = output.split("\n")
        endpoints_section_started = False

        for line in lines:
            line = line.strip()

            # Start of ENDPOINTS section
            if line.startswith("ENDPOINTS:"):
                endpoints_section_started = True
                continue
            elif line.startswith("SHARDS:"):
                endpoints_section_started = False
                break
            elif endpoints_section_started and line and not line.startswith("DB:ID"):
                # Parse endpoint line: db:1  m-standard  endpoint:1:1  node:2  single  No
                parts = line.split()
                if len(parts) >= 3:
                    endpoint_full = parts[2]  # endpoint:1:1
                    if endpoint_full.startswith("endpoint:"):
                        endpoint_id = endpoint_full.replace("endpoint:", "")  # 1:1
                        return endpoint_id

        raise ValueError("No endpoint ID found in cluster status")

    @staticmethod
    def execute_rladmin_migrate(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
    ) -> str:
        """Execute rladmin migrate command and wait for completion."""
        command = f"migrate node {target_node} all_shards target_node {empty_node}"

        # Get bdb_id from endpoint configuration
        bdb_id = endpoint_config.get("bdb_id")

        try:
            # Correct parameter format for fault injector
            parameters = {
                "bdb_id": bdb_id,
                "rladmin_command": command,  # Just the command without "rladmin" prefix
            }

            logging.debug(f"Executing rladmin_command with parameter: {parameters}")

            action = ActionRequest(
                action_type=ActionType.EXECUTE_RLADMIN_COMMAND, parameters=parameters
            )
            result = fault_injector.trigger_action(action)

            logging.debug(f"Migrate command action result: {result}")

            action_id = result.get("action_id")

            if not action_id:
                raise Exception(f"Failed to trigger migrate action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute rladmin migrate: {e}")

    @staticmethod
    def execute_rladmin_bind_endpoint(
        fault_injector: FaultInjectorClient,
        endpoint_config: Dict[str, Any],
        endpoint_id: str,
    ) -> str:
        """Execute rladmin bind endpoint command and wait for completion."""
        command = f"bind endpoint {endpoint_id} policy single"

        bdb_id = endpoint_config.get("bdb_id")

        try:
            parameters = {
                "rladmin_command": command,  # Just the command without "rladmin" prefix
                "bdb_id": bdb_id,
            }

            logging.info(f"Executing rladmin_command with parameter: {parameters}")
            action = ActionRequest(
                action_type=ActionType.EXECUTE_RLADMIN_COMMAND, parameters=parameters
            )
            result = fault_injector.trigger_action(action)
            logging.info(
                f"Migrate command {command} with parameters {parameters} trigger result: {result}"
            )

            action_id = result.get("action_id")

            if not action_id:
                raise Exception(f"Failed to trigger bind endpoint action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute rladmin bind endpoint: {e}")
