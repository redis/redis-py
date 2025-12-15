from abc import abstractmethod
from dataclasses import dataclass
import json
import logging
import time
import urllib.request
import urllib.error
from typing import Dict, Any, Optional, Tuple, Union
from enum import Enum

import pytest

from redis.cluster import ClusterNode
from tests.maint_notifications.proxy_server_helpers import (
    ProxyInterceptorHelper,
    RespTranslator,
    SlotsRange,
)


class TaskStatuses:
    """Class to hold completed statuses constants."""

    FAILED = "failed"
    FINISHED = "finished"
    SUCCESS = "success"
    RUNNING = "running"

    COMPLETED_STATUSES = [FAILED, FINISHED, SUCCESS]


class ActionType(str, Enum):
    DMC_RESTART = "dmc_restart"
    FAILOVER = "failover"
    RESHARD = "reshard"
    SEQUENCE_OF_ACTIONS = "sequence_of_actions"
    NETWORK_FAILURE = "network_failure"
    EXECUTE_RLUTIL_COMMAND = "execute_rlutil_command"
    EXECUTE_RLADMIN_COMMAND = "execute_rladmin_command"


class RestartDmcParams:
    def __init__(self, bdb_id: str):
        self.bdb_id = bdb_id

    def to_dict(self) -> Dict[str, str]:
        return {"bdb_id": self.bdb_id}


class ActionRequest:
    def __init__(
        self,
        action_type: ActionType,
        parameters: Union[Dict[str, Any], RestartDmcParams],
    ):
        self.type = action_type
        self.parameters = parameters

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type.value,  # Use the string value of the enum
            "parameters": self.parameters.to_dict()
            if isinstance(self.parameters, RestartDmcParams)
            else self.parameters,
        }


@dataclass
class NodeInfo:
    node_id: str
    role: str
    internal_address: str
    external_address: str
    hostname: str
    port: int


class FaultInjectorClient:
    @abstractmethod
    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def find_target_node_and_empty_node(
        self,
        endpoint_config: Dict[str, Any],
    ) -> Tuple[NodeInfo, NodeInfo]:
        pass

    @abstractmethod
    def find_endpoint_for_bind(
        self,
        endpoint_config: Dict[str, Any],
        endpoint_name: str,
    ) -> str:
        pass

    @abstractmethod
    def get_cluster_nodes_info(
        self,
        endpoint_config: Dict[str, Any],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def execute_failover(
        self,
        endpoint_config: Dict[str, Any],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def execute_migrate(
        self,
        endpoint_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
    ) -> str:
        pass

    @abstractmethod
    def execute_rebind(
        self,
        endpoint_config: Dict[str, Any],
        endpoint_id: str,
    ) -> str:
        pass

    @abstractmethod
    def get_moving_ttl(self) -> int:
        pass


class REFaultInjector(FaultInjectorClient):
    """Fault injector client for Redis Enterprise cluster setup."""

    MOVING_TTL = 15

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _make_request(
        self, method: str, path: str, data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"} if data else {}

        request_data = json.dumps(data).encode("utf-8") if data else None

        request = urllib.request.Request(
            url, method=method, data=request_data, headers=headers
        )

        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 422:
                error_body = json.loads(e.read().decode("utf-8"))
                raise ValueError(f"Validation Error: {error_body}")
            raise

    def list_actions(self) -> Dict[str, Any]:
        """List all available actions"""
        return self._make_request("GET", "/action")

    def trigger_action(self, action_request: ActionRequest) -> Dict[str, Any]:
        """Trigger a new action"""
        request_data = action_request.to_dict()
        return self._make_request("POST", "/action", request_data)

    def get_action_status(self, action_id: str) -> Dict[str, Any]:
        """Get the status of a specific action"""
        return self._make_request("GET", f"/action/{action_id}")

    def execute_rladmin_command(
        self, command: str, bdb_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute rladmin command directly as string"""
        url = f"{self.base_url}/rladmin"

        # The fault injector expects the raw command string
        command_string = f"rladmin {command}"
        if bdb_id:
            command_string = f"rladmin -b {bdb_id} {command}"

        headers = {"Content-Type": "text/plain"}

        request = urllib.request.Request(
            url, method="POST", data=command_string.encode("utf-8"), headers=headers
        )

        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 422:
                error_body = json.loads(e.read().decode("utf-8"))
                raise ValueError(f"Validation Error: {error_body}")
            raise

    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Get the result of a specific action"""
        start_time = time.time()
        check_interval = 3
        while time.time() - start_time < timeout:
            try:
                status_result = self.get_action_status(action_id)
                operation_status = status_result.get("status", "unknown")

                if operation_status in TaskStatuses.COMPLETED_STATUSES:
                    logging.debug(
                        f"Operation {action_id} completed with status: "
                        f"{operation_status}"
                    )
                    if operation_status != TaskStatuses.SUCCESS:
                        pytest.fail(f"Operation {action_id} failed: {status_result}")
                    return status_result

                time.sleep(check_interval)
            except Exception as e:
                logging.warning(f"Error checking operation status: {e}")
                time.sleep(check_interval)
        else:
            pytest.fail(f"Timeout waiting for operation {action_id}")

    def get_cluster_nodes_info(
        self,
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
            trigger_action_result = self.trigger_action(get_status_action)
            action_id = trigger_action_result.get("action_id")
            if not action_id:
                raise ValueError(
                    f"Failed to trigger get cluster status action for bdb_id {bdb_id}: {trigger_action_result}"
                )

            action_status_check_response = self.get_operation_result(
                action_id, timeout=timeout
            )
            logging.info(
                f"Completed cluster nodes info reading: {action_status_check_response}"
            )
            return action_status_check_response

        except Exception as e:
            pytest.fail(f"Failed to get cluster nodes info: {e}")

    def find_target_node_and_empty_node(
        self,
        endpoint_config: Dict[str, Any],
    ) -> Tuple[NodeInfo, NodeInfo]:
        """Find the node with master shards and the node with no shards.

        Returns:
            tuple: (target_node, empty_node) where target_node has master shards
                and empty_node has no shards
        """
        db_port = int(endpoint_config.get("port", 0))
        cluster_info = self.get_cluster_nodes_info(endpoint_config)
        output = cluster_info.get("output", {}).get("output", "")

        if not output:
            raise ValueError("No cluster status output found")

        # Parse the sections to find nodes with master shards and nodes with no shards
        lines = output.split("\n")
        shards_section_started = False
        nodes_section_started = False

        # Get all node IDs from CLUSTER NODES section
        all_nodes = set()
        all_nodes_details = {}
        nodes_with_any_shards = set()  # Nodes with shards from ANY database
        nodes_with_target_db_shards = set()  # Nodes with shards from target database
        master_nodes = set()  # Master nodes for target database only

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
                    node_role = parts[1]
                    node_internal_address = parts[2]
                    node_external_address = parts[3]
                    node_hostname = parts[4]

                    node = NodeInfo(
                        node_id.split(":")[1],
                        node_role,
                        node_internal_address,
                        node_external_address,
                        node_hostname,
                        db_port,
                    )
                    all_nodes.add(node_id)
                    all_nodes_details[node_id.split(":")[1]] = node

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
                    db_id = parts[0]  # db:1, db:2, etc.
                    node_id = parts[3]  # node:2
                    shard_role = parts[4]  # master/slave - this is what matters

                    # Track ALL nodes with shards (for finding truly empty nodes)
                    nodes_with_any_shards.add(node_id)

                    # Only track master nodes for the specific database we're testing
                    bdb_id = endpoint_config.get("bdb_id")
                    if db_id == f"db:{bdb_id}":
                        nodes_with_target_db_shards.add(node_id)
                        if shard_role == "master":
                            master_nodes.add(node_id)
            elif line.startswith("ENDPOINTS:") or not line:
                shards_section_started = False

        # Find empty node (node with no shards from ANY database)
        nodes_with_no_shards_target_bdb = all_nodes - nodes_with_target_db_shards

        logging.debug(f"All nodes: {all_nodes}")
        logging.debug(f"Nodes with shards from any database: {nodes_with_any_shards}")
        logging.debug(
            f"Nodes with target database shards: {nodes_with_target_db_shards}"
        )
        logging.debug(f"Master nodes (target database only): {master_nodes}")
        logging.debug(
            f"Nodes with no shards from target database: {nodes_with_no_shards_target_bdb}"
        )

        if not nodes_with_no_shards_target_bdb:
            raise ValueError("All nodes have shards from target database")

        if not master_nodes:
            raise ValueError("No nodes with master shards from target database found")

        # Return the first available empty node and master node (numeric part only)
        empty_node = next(iter(nodes_with_no_shards_target_bdb)).split(":")[
            1
        ]  # node:1 -> 1
        target_node = next(iter(master_nodes)).split(":")[1]  # node:2 -> 2

        return all_nodes_details[target_node], all_nodes_details[empty_node]

    def find_endpoint_for_bind(
        self,
        endpoint_config: Dict[str, Any],
        endpoint_name: str,
        timeout: int = 60,
    ) -> str:
        """Find the endpoint ID from cluster status.

        Returns:
            str: The endpoint ID (e.g., "1:1")
        """
        cluster_info = self.get_cluster_nodes_info(endpoint_config, timeout)
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
                if len(parts) >= 3 and parts[1] == endpoint_name:
                    endpoint_full = parts[2]  # endpoint:1:1
                    if endpoint_full.startswith("endpoint:"):
                        endpoint_id = endpoint_full.replace("endpoint:", "")  # 1:1
                        return endpoint_id

        raise ValueError(f"No endpoint ID for {endpoint_name} found in cluster status")

    def execute_failover(
        self,
        endpoint_config: Dict[str, Any],
        timeout: int = 60,
    ) -> Dict[str, Any]:
        """Execute failover command and wait for completion."""

        try:
            bdb_id = endpoint_config.get("bdb_id")
            failover_action = ActionRequest(
                action_type=ActionType.FAILOVER,
                parameters={
                    "bdb_id": bdb_id,
                },
            )
            trigger_action_result = self.trigger_action(failover_action)
            action_id = trigger_action_result.get("action_id")
            if not action_id:
                raise ValueError(
                    f"Failed to trigger fail over action for bdb_id {bdb_id}: {trigger_action_result}"
                )

            action_status_check_response = self.get_operation_result(
                action_id, timeout=timeout
            )
            logging.info(
                f"Completed cluster nodes info reading: {action_status_check_response}"
            )
            return action_status_check_response

        except Exception as e:
            pytest.fail(f"Failed to get cluster nodes info: {e}")

    def execute_migrate(
        self,
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
            result = self.trigger_action(action)

            logging.debug(f"Migrate command action result: {result}")

            action_id = result.get("action_id")

            if not action_id:
                raise Exception(f"Failed to trigger migrate action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute rladmin migrate: {e}")

    def execute_rebind(
        self,
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
            result = self.trigger_action(action)
            logging.info(
                f"Migrate command {command} with parameters {parameters} trigger result: {result}"
            )

            action_id = result.get("action_id")

            if not action_id:
                raise Exception(f"Failed to trigger bind endpoint action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute rladmin bind endpoint: {e}")

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL


class ProxyServerFaultInjector(FaultInjectorClient):
    """Fault injector client for proxy server setup."""

    NODE_PORT_1 = 15379
    NODE_PORT_2 = 15380
    NODE_PORT_3 = 15381

    # Initial cluster node configuration for proxy-based tests
    PROXY_CLUSTER_NODES = [
        ClusterNode("127.0.0.1", NODE_PORT_1),
        ClusterNode("127.0.0.1", NODE_PORT_2),
    ]

    DEFAULT_CLUSTER_SLOTS = [
        SlotsRange("127.0.0.1", NODE_PORT_1, 0, 8191),
        SlotsRange("127.0.0.1", NODE_PORT_2, 8192, 16383),
    ]

    CLUSTER_SLOTS_INTERCEPTOR_NAME = "test_topology"

    SLEEP_TIME_BETWEEN_START_END_NOTIFICATIONS = 2
    MOVING_TTL = 4

    def __init__(self, oss_cluster: bool = False):
        self.oss_cluster = oss_cluster
        self.proxy_helper = ProxyInterceptorHelper()

        # set the initial state of the proxy server
        logging.info(
            f"Setting up initial cluster slots -> {self.DEFAULT_CLUSTER_SLOTS}"
        )
        self.proxy_helper.set_cluster_slots(
            self.CLUSTER_SLOTS_INTERCEPTOR_NAME, self.DEFAULT_CLUSTER_SLOTS
        )
        logging.info("Sleeping for 1 seconds to allow proxy to apply the changes...")
        time.sleep(2)

        self.seq_id = 0

    def _get_seq_id(self):
        self.seq_id += 1
        return self.seq_id

    def find_target_node_and_empty_node(
        self,
        endpoint_config: Dict[str, Any],
    ) -> Tuple[NodeInfo, NodeInfo]:
        target_node = NodeInfo(
            "1", "master", "0.0.0.0", "127.0.0.1", "localhost", self.NODE_PORT_1
        )
        empty_node = NodeInfo(
            "3", "master", "0.0.0.0", "127.0.0.1", "localhost", self.NODE_PORT_3
        )
        return target_node, empty_node

    def find_endpoint_for_bind(
        self,
        endpoint_config: Dict[str, Any],
        endpoint_name: str,
    ) -> str:
        return "1:1"

    def execute_failover(
        self, endpoint_config: Dict[str, Any], timeout: int = 60
    ) -> Dict[str, Any]:
        """
        Execute failover command and wait for completion.
        Run in separate thread so that it can simulate the actual failover process.
        This will run always for the same nodes - node 1 to node 3!
        Assuming that the initial state is the DEFAULT_CLUSTER_SLOTS - shard 1 on node 1 and shard 2 on node 2.
        In a real RE cluster we would have on some other node the replica - and we simulate that with node 3.
        """

        # send smigrating
        if self.oss_cluster:
            start_maint_notif = RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATING {self._get_seq_id()} 0-8191"
            )
        else:
            # send failing over
            start_maint_notif = RespTranslator.re_cluster_maint_notification_to_resp(
                f"FAILING_OVER {self._get_seq_id()} 2 [1]"
            )

        self.proxy_helper.send_notification(self.NODE_PORT_1, start_maint_notif)

        # sleep to allow the client to receive the notification
        time.sleep(self.SLEEP_TIME_BETWEEN_START_END_NOTIFICATIONS)

        if self.oss_cluster:
            # intercept cluster slots
            self.proxy_helper.set_cluster_slots(
                self.CLUSTER_SLOTS_INTERCEPTOR_NAME,
                [
                    SlotsRange("127.0.0.1", self.NODE_PORT_3, 0, 8191),
                    SlotsRange("127.0.0.1", self.NODE_PORT_2, 8192, 16383),
                ],
            )
            # send smigrated
            end_maint_notif = RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATED {self._get_seq_id()} 127.0.0.1:{self.NODE_PORT_3} 0-8191"
            )
        else:
            # send failed over
            end_maint_notif = RespTranslator.re_cluster_maint_notification_to_resp(
                f"FAILED_OVER {self._get_seq_id()} [1]"
            )
        self.proxy_helper.send_notification(self.NODE_PORT_1, end_maint_notif)

        return {"status": "done"}

    def execute_migrate(
        self, endpoint_config: Dict[str, Any], target_node: str, empty_node: str
    ) -> str:
        """
        Simulate migrate command execution.
        Run in separate thread so that it can simulate the actual migrate process.
        This will run always for the same nodes - node 1 to node 2!
        Assuming that the initial state is the DEFAULT_CLUSTER_SLOTS - shard 1 on node 1 and shard 2 on node 2.

        """

        if self.oss_cluster:
            # send smigrating
            start_maint_notif = RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATING {self._get_seq_id()} 0-200"
            )
        else:
            # send migrating
            start_maint_notif = RespTranslator.re_cluster_maint_notification_to_resp(
                f"MIGRATING {self._get_seq_id()} 2 [1]"
            )

        self.proxy_helper.send_notification(self.NODE_PORT_1, start_maint_notif)

        # sleep to allow the client to receive the notification
        time.sleep(self.SLEEP_TIME_BETWEEN_START_END_NOTIFICATIONS)

        if self.oss_cluster:
            # intercept cluster slots
            self.proxy_helper.set_cluster_slots(
                self.CLUSTER_SLOTS_INTERCEPTOR_NAME,
                [
                    SlotsRange("127.0.0.1", self.NODE_PORT_2, 0, 200),
                    SlotsRange("127.0.0.1", self.NODE_PORT_1, 201, 8191),
                    SlotsRange("127.0.0.1", self.NODE_PORT_2, 8192, 16383),
                ],
            )
            # send smigrated
            end_maint_notif = RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATED {self._get_seq_id()} 127.0.0.1:{self.NODE_PORT_2} 0-200"
            )
        else:
            # send migrated
            end_maint_notif = RespTranslator.re_cluster_maint_notification_to_resp(
                f"MIGRATED {self._get_seq_id()} [1]"
            )
        self.proxy_helper.send_notification(self.NODE_PORT_1, end_maint_notif)

        return "done"

    def execute_rebind(self, endpoint_config: Dict[str, Any], endpoint_id: str) -> str:
        """
        Execute rladmin bind endpoint command and wait for completion.
        Run in separate thread so that it can simulate the actual bind process.
        This will run always for the same nodes - node 1 to node 3!
        Assuming that the initial state is the DEFAULT_CLUSTER_SLOTS - shard 1 on node 1
        and shard 2 on node 2.

        """
        sleep_time = self.SLEEP_TIME_BETWEEN_START_END_NOTIFICATIONS
        if self.oss_cluster:
            # send smigrating
            maint_start_notif = RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATING {self._get_seq_id()} 0-8191"
            )
        else:
            # send moving
            sleep_time = self.MOVING_TTL
            maint_start_notif = RespTranslator.re_cluster_maint_notification_to_resp(
                f"MOVING {self._get_seq_id()} {sleep_time} 127.0.0.1:{self.NODE_PORT_3}"
            )
        self.proxy_helper.send_notification(self.NODE_PORT_1, maint_start_notif)

        # sleep to allow the client to receive the notification
        time.sleep(sleep_time)

        if self.oss_cluster:
            # intercept cluster slots
            self.proxy_helper.set_cluster_slots(
                self.CLUSTER_SLOTS_INTERCEPTOR_NAME,
                [
                    SlotsRange("127.0.0.1", self.NODE_PORT_3, 0, 8191),
                    SlotsRange("127.0.0.1", self.NODE_PORT_2, 8192, 16383),
                ],
            )
            # send smigrated
            smigrated_node_1 = RespTranslator.oss_maint_notification_to_resp(
                f"SMIGRATED {self._get_seq_id()} 127.0.0.1:{self.NODE_PORT_3} 0-8191"
            )
            self.proxy_helper.send_notification(self.NODE_PORT_1, smigrated_node_1)
        else:
            # TODO drop connections to node 1
            pass

        return "done"

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL
