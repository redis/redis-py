import logging
import threading
from time import sleep

import pytest

from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError
from redis.retry import Retry
from tests.scenario.conftest import get_endpoint_config
from tests.scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)

def trigger_network_failure_action(fault_injector_client, event: threading.Event = None):
    endpoint_config = get_endpoint_config('re-active-active')
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": endpoint_config['bdb_id'], "delay": 3, "cluster_index": 0}
    )

    result = fault_injector_client.trigger_action(action_request)
    status_result = fault_injector_client.get_action_status(result['action_id'])

    while status_result['status'] != "success":
        sleep(0.1)
        status_result = fault_injector_client.get_action_status(result['action_id'])
        logger.info(f"Waiting for action to complete. Status: {status_result['status']}")

    if event:
        event.set()

    logger.info(f"Action completed. Status: {status_result['status']}")

class TestActiveActiveStandalone:
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"failure_threshold": 2}
        ],
        indirect=True
    )
    def test_multi_db_client_failover_to_another_db(self, r_multi_db, fault_injector_client):
        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,event)
        )
        thread.start()

        r_multi_db.set('key', 'value')
        current_active_db = r_multi_db._command_executor.active_database

        # Execute commands before network failure
        while not event.is_set():
            assert r_multi_db.get('key') == 'value'
            sleep(0.1)

        # Active db has been changed.
        assert current_active_db != r_multi_db._command_executor.active_database

        # Execute commands after network failure
        for _ in range(3):
            assert r_multi_db.get('key') == 'value'
            sleep(0.1)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "failure_threshold": 15,
                "command_retry": Retry(NoBackoff(), retries=5),
                "health_check_interval": 100,
            }
        ],
        indirect=True
    )
    def test_multi_db_client_throws_error_on_retry_exceed(self, r_multi_db, fault_injector_client):
        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,event)
        )
        thread.start()

        with pytest.raises(ConnectionError):
            # Retries count > failure threshold, so a client gives up earlier.
            while not event.is_set():
                assert r_multi_db.get('key') == 'value'
                sleep(0.1)