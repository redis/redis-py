import logging
import threading
from time import sleep

import pytest

from redis.backoff import NoBackoff
from redis.client import Pipeline
from redis.exceptions import ConnectionError
from redis.retry import Retry
from tests.test_scenario.conftest import get_endpoint_config
from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

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

    def teardown_method(self, method):
        # Timeout so the cluster could recover from network failure.
        sleep(3)

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

        # Client initialized on the first command.
        r_multi_db.set('key', 'value')
        current_active_db = r_multi_db.command_executor.active_database
        thread.start()

        # Execute commands before network failure
        while not event.is_set():
            assert r_multi_db.get('key') == 'value'
            sleep(0.1)

        # Active db has been changed.
        assert current_active_db != r_multi_db.command_executor.active_database

        # Execute commands after network failure
        for _ in range(3):
            assert r_multi_db.get('key') == 'value'
            sleep(0.1)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"failure_threshold": 2}
        ],
        indirect=True
    )
    def test_multi_db_client_throws_error_on_retry_exceed(self, r_multi_db, fault_injector_client):
        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, event)
        )
        thread.start()

        with pytest.raises(ConnectionError):
            # Retries count > failure threshold, so a client gives up earlier.
            while not event.is_set():
                assert r_multi_db.get('key') == 'value'
                sleep(0.1)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"failure_threshold": 2}
        ],
        indirect=True
    )
    def test_context_manager_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,event)
        )

        # Client initialized on first pipe execution.
        with r_multi_db.pipeline() as pipe:
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')
            assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']

        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            with r_multi_db.pipeline() as pipe:
                pipe.set('{hash}key1', 'value1')
                pipe.set('{hash}key2', 'value2')
                pipe.set('{hash}key3', 'value3')
                pipe.get('{hash}key1')
                pipe.get('{hash}key2')
                pipe.get('{hash}key3')
                assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
            sleep(0.1)

        # Execute pipeline after network failure
        for _ in range(3):
            with r_multi_db.pipeline() as pipe:
                pipe.set('{hash}key1', 'value1')
                pipe.set('{hash}key2', 'value2')
                pipe.set('{hash}key3', 'value3')
                pipe.get('{hash}key1')
                pipe.get('{hash}key2')
                pipe.get('{hash}key3')
                assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
            sleep(0.1)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"failure_threshold": 2}
        ],
        indirect=True
    )
    def test_chaining_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,event)
        )

        # Client initialized on first pipe execution.
        pipe = r_multi_db.pipeline()
        pipe.set('{hash}key1', 'value1')
        pipe.set('{hash}key2', 'value2')
        pipe.set('{hash}key3', 'value3')
        pipe.get('{hash}key1')
        pipe.get('{hash}key2')
        pipe.get('{hash}key3')
        assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']

        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')
            assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
        sleep(0.1)

        # Execute pipeline after network failure
        for _ in range(3):
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')
            assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
        sleep(0.1)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"failure_threshold": 2}
        ],
        indirect=True
    )
    def test_transaction_failover_to_another_db(self, r_multi_db, fault_injector_client):
        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,event)
        )

        def callback(pipe: Pipeline):
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')

        # Client initialized on first transaction execution.
        r_multi_db.transaction(callback)
        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            r_multi_db.transaction(callback)
            sleep(0.1)

        # Execute pipeline after network failure
        for _ in range(3):
            r_multi_db.transaction(callback)
            sleep(0.1)