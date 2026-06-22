import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import aiohttp
import pytest

from tests.maint_notifications.proxy_server_helpers import (
    ProxyInterceptorHelper,
    SlotsRange,
)
from tests.test_scenario.fault_injector_client import (
    ActionRequest,
    ActionType,
    DEFAULT_BDB_ID,
    MockProxyFaultInjector,
    SlotMigrateEffects,
    TaskStatuses,
    TopologyChangeStandaloneEffects,
)


class AsyncFaultInjectorClient(ABC):
    """Async counterpart to FaultInjectorClient — all I/O methods are coroutines."""

    @abstractmethod
    async def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        pass

    @abstractmethod
    def get_moving_ttl(self) -> int:
        pass

    @abstractmethod
    async def get_slot_migrate_triggers(
        self,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def get_topology_change_standalone_triggers(
        self,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        pass

    @abstractmethod
    async def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        pass


class AsyncREFaultInjector(AsyncFaultInjectorClient):
    """Async fault injector client for Redis Enterprise cluster setup."""

    MOVING_TTL = 15

    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self._cluster_nodes_info: Optional[str] = None
        self._current_db_id: Optional[int] = None

    async def _make_request(
        self, method: str, path: str, data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"} if data else {}
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method, url, json=data, headers=headers
            ) as response:
                if response.status == 422:
                    error_body = await response.json()
                    raise ValueError(f"Validation Error: {error_body}")
                response.raise_for_status()
                return await response.json()

    async def _trigger_action(self, action_request: ActionRequest) -> Dict[str, Any]:
        return await self._make_request("POST", "/action", action_request.to_dict())

    async def _get_action_status(self, action_id: str) -> Dict[str, Any]:
        return await self._make_request("GET", f"/action/{action_id}")

    async def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        check_interval = 0.3

        while loop.time() - start_time < timeout:
            try:
                status_result = await self._get_action_status(action_id)
                operation_status = status_result.get("status", "unknown")

                if operation_status in TaskStatuses.COMPLETED_STATUSES:
                    logging.debug(
                        f"Operation {action_id} completed with status: "
                        f"{operation_status}"
                    )
                    if operation_status != TaskStatuses.SUCCESS:
                        pytest.fail(f"Operation {action_id} failed: {status_result}")
                    return status_result

                await asyncio.sleep(check_interval)
            except Exception as e:
                logging.warning(f"Error checking operation status: {e}")
                await asyncio.sleep(check_interval)

        pytest.fail(f"Timeout waiting for operation {action_id} after {timeout}s")

    async def create_database(
        self,
        bdb_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        logging.debug(f"Creating database with config: {bdb_config}")
        result = await self._trigger_action(
            ActionRequest(ActionType.CREATE_DATABASE, {"database_config": bdb_config})
        )
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger create database action: {result}")

        response = await self.get_operation_result(action_id)
        logging.debug(f"Create database action result: {response}")

        if response.get("status") != TaskStatuses.SUCCESS:
            raise Exception(f"Create database action failed: {response}")

        self._current_db_id = response["output"]["bdb_id"]
        return response["output"]

    async def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        logging.debug(f"Deleting database with id: {bdb_id}")
        result = await self._trigger_action(
            ActionRequest(ActionType.DELETE_DATABASE, {"bdb_id": bdb_id})
        )
        action_id = result.get("action_id")
        if not action_id:
            raise Exception(f"Failed to trigger delete database action: {result}")

        response = await self.get_operation_result(action_id)
        self._current_db_id = None

        if response.get("status") != TaskStatuses.SUCCESS:
            raise Exception(f"Delete database action failed: {response}")
        logging.debug(f"Delete database action result: {response}")
        return response

    async def _get_cluster_nodes_info(self) -> None:
        try:
            action = ActionRequest(
                ActionType.EXECUTE_RLADMIN_COMMAND,
                {
                    "rladmin_command": "status",
                    "bdb_id": DEFAULT_BDB_ID
                    if self._current_db_id is None
                    else self._current_db_id,
                },
            )
            trigger_result = await self._trigger_action(action)
            action_id = trigger_result.get("action_id")
            if not action_id:
                raise ValueError(
                    f"Failed to trigger get cluster status action: {trigger_result}"
                )

            response = await self.get_operation_result(action_id)

            if response.get("status") != TaskStatuses.SUCCESS:
                raise Exception(f"Get cluster status action failed: {response}")
            self._cluster_nodes_info = response.get("output", {}).get("output", "")
        except Exception as e:
            pytest.fail(f"Failed to get cluster nodes info: {e}")

    async def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        if self._cluster_nodes_info is None or force_cluster_info_refresh:
            await self._get_cluster_nodes_info()

        if not self._cluster_nodes_info:
            raise ValueError("No cluster status info found")

        lines = self._cluster_nodes_info.split("\n")
        databases_section_started = False

        for line in lines:
            line = line.strip()
            if line.startswith("DATABASES:"):
                databases_section_started = True
                continue
            elif databases_section_started and line and not line.startswith("DB:ID"):
                parts = line.split()
                if len(parts) >= 2 and parts[1] == database_name:
                    return int(parts[0].replace("db:", ""))

        raise ValueError(f"Database {database_name} not found")

    async def get_slot_migrate_triggers(
        self,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        return await self._make_request(
            "GET", f"/slot-migrate?effect={effect_name.value}&cluster_index=0"
        )

    async def get_topology_change_standalone_triggers(
        self,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        return await self._make_request(
            "GET",
            f"/topology-change-standalone?effect={effect_name.value}&cluster_index=0",
        )

    async def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: str,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        bdb_id = endpoint_config.get("bdb_id")
        parameters: Dict[str, Any] = {
            "bdb_id": bdb_id,
            "cluster_index": 0,
            "effect": effect_name,
            "trigger": trigger_name,
        }
        if source_node:
            parameters["source_node"] = source_node
        if target_node:
            parameters["target_node"] = target_node

        action_type = (
            ActionType.SLOT_MIGRATE
            if isinstance(effect_name, SlotMigrateEffects)
            else ActionType.TOPOLOGY_CHANGE_STANDALONE
        )
        logging.debug(f"Executing {action_type.value} with parameters: {parameters}")

        try:
            result = await self._trigger_action(ActionRequest(action_type, parameters))
            logging.debug(f"Trigger effect action result: {result}")
            action_id = result.get("action_id")
            if not action_id:
                raise Exception(f"Failed to trigger slot migrate action: {result}")
            return action_id
        except Exception as e:
            raise Exception(f"Failed to execute slot migrate: {e}")

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL


class AsyncProxyServerFaultInjector(MockProxyFaultInjector, AsyncFaultInjectorClient):
    """Async fault injector client for proxy server setup."""

    NODE_PORT_1 = 15379
    NODE_PORT_2 = 15380
    NODE_PORT_3 = 15381

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
        logging.info(
            f"Setting up initial cluster slots -> {self.DEFAULT_CLUSTER_SLOTS}"
        )
        self.proxy_helper.set_cluster_slots(
            self.CLUSTER_SLOTS_INTERCEPTOR_NAME, self.DEFAULT_CLUSTER_SLOTS
        )

    async def get_operation_result(
        self,
        action_id: str,
        timeout: int = 60,
    ) -> Dict[str, Any]:
        return {"status": "done"}

    async def create_database(
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

    async def delete_database(
        self,
        bdb_id: int,
    ) -> Dict[str, Any]:
        return {}

    async def find_database_id_by_name(
        self,
        database_name: str,
        force_cluster_info_refresh: bool = True,
    ) -> Optional[int]:
        return 1

    def get_moving_ttl(self) -> int:
        return self.MOVING_TTL

    async def get_slot_migrate_triggers(
        self,
        effect_name: SlotMigrateEffects,
    ) -> Dict[str, Any]:
        raise NotImplementedError("Not implemented for proxy server")

    async def get_topology_change_standalone_triggers(
        self,
        effect_name: TopologyChangeStandaloneEffects,
    ) -> Dict[str, Any]:
        raise NotImplementedError("Not implemented for proxy server")

    async def trigger_effect(
        self,
        endpoint_config: Dict[str, Any],
        effect_name: SlotMigrateEffects | TopologyChangeStandaloneEffects,
        trigger_name: Optional[str] = None,
        source_node: Optional[str] = None,
        target_node: Optional[str] = None,
        skip_end_notification: bool = False,
    ) -> str:
        raise NotImplementedError("Not implemented for proxy server")
