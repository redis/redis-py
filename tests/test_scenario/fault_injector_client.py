from abc import ABC, abstractmethod
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

DEFAULT_BDB_ID = 1


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
    CREATE_DATABASE = "create_database"
    DELETE_DATABASE = "delete_database"
    SEQUENCE_OF_ACTIONS = "sequence_of_actions"
    NETWORK_FAILURE = "network_failure"
    EXECUTE_RLUTIL_COMMAND = "execute_rlutil_command"
    EXECUTE_RLADMIN_COMMAND = "execute_rladmin_command"
    SLOT_MIGRATE = "slot_migrate"


class SlotMigrateEffects(str, Enum):
    REMOVE_ADD = "remove-add"
    REMOVE = "remove"
    ADD = "add"
    SLOT_SHUFFLE = "slot-shuffle"


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


class FaultInjectorClient(ABC):
    @abstractmethod
    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        pass

    @abstractmethod
    def find_target_node_and_empty_node(
        self,
        endpoint_config: Dict[str, Any],
        force_cluster_info_refresh: bool = True,
    ) -> Tuple[NodeInfo, NodeInfo]:
        pass

    @abstractmethod
    def find_endpoint_for_bind(
        self,
        endpoint_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> str:
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
        skip_end_notification: bool = False,
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

    @abstractmethod
    def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        pass


class REFaultInjector(FaultInjectorClient):
    """Fault injector client for Redis Enterprise cluster setup."""

    MOVING_TTL = 15

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._cluster_nodes_info = None
        self._current_db_id = None

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
        start_time = time.time()  # returns the time in seconds
        check_interval = 0.3

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
            pytest.fail(
                f"Timeout waiting for operation {action_id}. Start time: {start_time}, current time: {time.time()}"
            )

    def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a new database."""
        # Please provide the config just for the db that will be created
        logging.debug(f"Creating database with config: {bdb_config}")
        params = {"database_config": bdb_config}
        create_db_action = ActionRequest(
            action_type=ActionType.CREATE_DATABASE,
            parameters=params,
        )
        result = self.trigger_action(create_db_action)
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger create database action: {result}")

        action_status_check_response = self.get_operation_result(action_id)
        logging.debug(f"Create database action result: {action_status_check_response}")

        if action_status_check_response.get("status") != TaskStatuses.SUCCESS:
            raise Exception(
                f"Create database action failed: {action_status_check_response}"
            )

        self._current_db_id = action_status_check_response["output"]["bdb_id"]
        return action_status_check_response["output"]

    def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        logging.debug(f"Deleting database with id: {bdb_id}")
        params = {"bdb_id": bdb_id}
        create_db_action = ActionRequest(
            action_type=ActionType.DELETE_DATABASE,
            parameters=params,
        )
        result = self.trigger_action(create_db_action)
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger delete database action: {result}")

        action_status_check_response = self.get_operation_result(action_id)

        if action_status_check_response.get("status") != TaskStatuses.SUCCESS:
            raise Exception(
                f"Delete database action failed: {action_status_check_response}"
            )
        logging.debug(f"Delete database action result: {action_status_check_response}")
        return action_status_check_response

    def get_cluster_nodes_info(self) -> None:
        """Get cluster nodes information from Redis Enterprise."""
        try:
            # Use rladmin status to get node information
            get_status_action = ActionRequest(
                action_type=ActionType.EXECUTE_RLADMIN_COMMAND,
                parameters={
                    "rladmin_command": "status",
                    "bdb_id": DEFAULT_BDB_ID
                    if self._current_db_id is None
                    else self._current_db_id,  # Any database id will do - it just only needs to exist
                },
            )
            trigger_action_result = self.trigger_action(get_status_action)
            action_id = trigger_action_result.get("action_id")
            if not action_id:
                raise ValueError(
                    f"Failed to trigger get cluster status action: {trigger_action_result}"
                )

            action_status_check_response = self.get_operation_result(action_id)

            if action_status_check_response.get("status") != TaskStatuses.SUCCESS:
                raise Exception(
                    f"Get cluster status action failed: {action_status_check_response}"
                )
            self._cluster_nodes_info = action_status_check_response.get(
                "output", {}
            ).get("output", "")

        except Exception as e:
            pytest.fail(f"Failed to get cluster nodes info: {e}")

    def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        """Find the database ID by name."""
        if self._cluster_nodes_info is None or force_cluster_info_refresh:
            self.get_cluster_nodes_info()

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status info found")

        # Parse the DATABASES section to find the database ID
        lines = self._cluster_nodes_info.split("\n")
        databases_section_started = False

        for line in lines:
            line = line.strip()

            # Start of DATABASES section
            if line.startswith("DATABASES:"):
                databases_section_started = True
                continue
            elif databases_section_started and line and not line.startswith("DB:ID"):
                # Parse database line: db:3 m-standard redis:5 node:3 master 8192-16383 1.79MB OK

                parts = line.split()
                if len(parts) >= 2 and parts[1] == database_name:
                    return int(parts[0].replace("db:", ""))

        raise ValueError(f"Database {database_name} not found")

    def find_target_node_and_empty_node(
        self,
        endpoint_config: Dict[str, Any],
        force_cluster_info_refresh: bool = True,
    ) -> Tuple[NodeInfo, NodeInfo]:
        """Find the node with master shards and the node with no shards.

        Returns:
            tuple: (target_node, empty_node) where target_node has master shards
                and empty_node has no shards
        """
        db_port = int(endpoint_config.get("port", 0))

        if self._cluster_nodes_info is None or force_cluster_info_refresh:
            self.get_cluster_nodes_info()

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status output found")

        # Parse the sections to find nodes with master shards and nodes with no shards
        lines = self._cluster_nodes_info.split("\n")
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
        endpoint_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> str:
        """Find the endpoint ID from cluster status.

        Returns:
            str: The endpoint ID (e.g., "1:1")
        """
        if self._cluster_nodes_info is None or force_cluster_info_refresh:
            self.get_cluster_nodes_info()

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status output found")

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status output found")

        # Parse the ENDPOINTS section to find endpoint ID
        lines = self._cluster_nodes_info.split("\n")
        endpoints_section_started = False

        for line in lines:
            line = line.strip()

            # Start of ENDPOINTS section
            if line.startswith("ENDPOINTS:"):
                endpoints_section_started = True
                continue
            elif line.startswith("SHARDS:"):
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
            # Refresh cluster info before getting the shard - we want to be sure
            # that we have the current state
            shard = self._get_first_master_shard(
                endpoint_config,
                force_cluster_info_refresh=True,
            )
            bdb_id = endpoint_config.get("bdb_id")
            command = f"failover db db:{bdb_id} shard {shard}"

            parameters = {
                "bdb_id": bdb_id,
                "rladmin_command": command,  # Just the command without "rladmin" prefix
            }
            logging.debug(f"Executing rladmin_command with parameter: {parameters}")

            failover_action = ActionRequest(
                action_type=ActionType.EXECUTE_RLADMIN_COMMAND,
                parameters=parameters,
            )
            result = self.trigger_action(failover_action)

            logging.debug(f"Failover command action result: {result}")

            action_id = result.get("action_id")
            if not action_id:
                raise Exception(f"Failed to trigger failover action: {result}")

            action_status_check_response = self.get_operation_result(
                action_id, timeout=timeout
            )
            logging.info(
                f"Completed failover execution: {action_status_check_response}"
            )
            return action_status_check_response

        except Exception as e:
            pytest.fail(f"Failed to execute failover: {e}")

    def execute_migrate(
        self,
        endpoint_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
        skip_end_notification: bool = False,
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

        endpoint_policy = endpoint_config["raw_endpoints"][0]["proxy_policy"]
        logging.info(
            f"Executing rladmin bind endpoint {endpoint_id} policy {endpoint_policy}"
        )
        command = f"bind endpoint {endpoint_id} policy {endpoint_policy}"

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

    def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects,
        trigger_name: str,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        """Execute FI action that will trigger the desired effect."""

        # Get bdb_id from endpoint configuration
        bdb_id = endpoint_config.get("bdb_id")
        cluster_index = 0

        try:
            # Correct parameter format for fault injector
            parameters = {
                "bdb_id": bdb_id,
                "cluster_index": cluster_index,
                "effect": effect_name,
                "variant": trigger_name,  # will be renamed to trigger
            }
            if source_node:
                parameters["source_node"] = source_node
            if target_node:
                parameters["target_node"] = target_node

            logging.debug(f"Executing slot migrate with parameters: {parameters}")

            action = ActionRequest(
                action_type=ActionType.SLOT_MIGRATE, parameters=parameters
            )
            result = self.trigger_action(action)

            logging.debug(f"Trigger effect action result: {result}")

            action_id = result.get("action_id")

            if not action_id:
                raise Exception(f"Failed to trigger slot migrate action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute slot migrate: {e}")

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL

    def _get_first_master_shard(
        self,
        endpoint_config: Dict[str, Any],
        force_cluster_info_refresh: bool = True,
    ) -> str:
        """Get the first master shard from the endpoint configuration."""
        bdb_id = endpoint_config.get("bdb_id")

        if self._cluster_nodes_info is None or force_cluster_info_refresh:
            self.get_cluster_nodes_info()

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status output found")

        # Parse the SHARDS section to find the shard id covering slot 0
        lines = self._cluster_nodes_info.split("\n")
        shards_section_started = False

        for line in lines:
            line = line.strip()

            # Start of SHARDS section
            if line.startswith("SHARDS:"):
                shards_section_started = True
                continue
            elif shards_section_started and line and not line.startswith("DB:ID"):
                # Parse shard line: db:3 m-standard redis:3 node:3 master 0-8191 1.79MB OK
                parts = line.split()
                if (
                    len(parts) >= 8
                    and parts[0] == f"db:{bdb_id}"
                    and parts[4] == "master"
                    and parts[5].startswith("0-")
                ):
                    return parts[2].replace("redis:", "")  # redis:3 --> 3

        raise ValueError("No master shard found")


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

        self.seq_id = 0

    def _get_seq_id(self):
        self.seq_id += 1
        return self.seq_id

    def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        return {"status": "done"}

    def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "bdb_id": 1,
            "username": "default",
            "password": "",
            "tls": False,
            "raw_endpoints": [
                {
                    "addr": ["127.0.0.1"],
                    "addr_type": "external",
                    "dns_name": "localhost",
                    "oss_cluster_api_preferred_endpoint_type": "ip",
                    "oss_cluster_api_preferred_ip_type": "internal",
                    "port": 15379,
                    "proxy_policy": "all-master-shards",
                    "uid": "1:1",
                }
            ],
            "endpoints": ["redis://127.0.0.1:15379"],
        }

    def delete_database(
        self,
        endpoint_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {}

    def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        return 1

    def find_target_node_and_empty_node(
        self,
        endpoint_config: Dict[str, Any],
        force_cluster_info_refresh: bool = True,
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
        endpoint_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> str:
        return "1:1"

    def execute_failover(
        self, endpoint_config: Dict[str, Any], timeout: int = 60
    ) -> Dict[str, Any]:
        """
        Simulates a failover operation and waits for completion.
        This method does not create or manage threads; if asynchronous execution is required,
        it should be called from a separate thread by the caller.
        This will always run for the same nodes - node 1 to node 3!
        Assumes that the initial state is the DEFAULT_CLUSTER_SLOTS - shard 1 on node 1 and shard 2 on node 2.
        In a real RE cluster, a replica would exist on another node, which is simulated here with node 3.
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
        self,
        endpoint_config: Dict[str, Any],
        target_node: str,
        empty_node: str,
        skip_end_notification: bool = False,
    ) -> str:
        """
        Simulate migrate command execution.
        This method does not create or manage threads; it simulates the migration process synchronously.
        If asynchronous execution is desired, the caller should run this method in a separate thread.
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
            if not skip_end_notification:
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
                self.proxy_helper.send_notification(self.NODE_PORT_1, end_maint_notif)
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
        This method simulates the actual bind process. It does not create or manage threads;
        if you wish to run it in a separate thread, you must do so from the caller.
        This will run always for the same nodes - node 1 to node 3!
        Assuming that the initial state is the DEFAULT_CLUSTER_SLOTS - shard 1 on node 1
        and shard 2 on node 2.

        """
        sleep_time = self.SLEEP_TIME_BETWEEN_START_END_NOTIFICATIONS
        if self.oss_cluster:
            # smigrating should be sent as part of the migrate flow
            pass
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
            # TODO drop connections to node 1 to simulate that the node is removed
            pass

        return "done"

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL

    def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        """
        Trigger the desired effect. For the proxy server,
        this will need to be implemented in next iterations.
        """
        raise NotImplementedError("Not implemented for proxy server")
