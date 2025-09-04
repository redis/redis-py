import asyncio
import logging
from time import sleep

import pytest

from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)

async def trigger_network_failure_action(fault_injector_client, config, event: asyncio.Event = None):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config['bdb_id'], "delay": 2, "cluster_index": 0}
    )

    result = fault_injector_client.trigger_action(action_request)
    status_result = fault_injector_client.get_action_status(result['action_id'])

    while status_result['status'] != "success":
        await asyncio.sleep(0.1)
        status_result = fault_injector_client.get_action_status(result['action_id'])
        logger.info(f"Waiting for action to complete. Status: {status_result['status']}")

    if event:
        event.set()

    logger.info(f"Action completed. Status: {status_result['status']}")

class TestActiveActive:

    def teardown_method(self, method):
        # Timeout so the cluster could recover from network failure.
        sleep(5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [{"failure_threshold": 2}],
        indirect=True
    )
    @pytest.mark.timeout(50)
    async def test_multi_db_client_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = asyncio.Event()
        asyncio.create_task(trigger_network_failure_action(fault_injector_client,config,event))

        # Client initialized on the first command.
        await r_multi_db.set('key', 'value')

        # Execute commands before network failure
        while not event.is_set():
            assert await r_multi_db.get('key') == 'value'
            await asyncio.sleep(0.5)

        # Execute commands until database failover
        while not listener.is_changed_flag:
            assert await r_multi_db.get('key') == 'value'
            await asyncio.sleep(0.5)