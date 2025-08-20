import json
import logging
import threading
from time import sleep

import pytest

from redis import Redis, RedisCluster
from redis.client import Pipeline
from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)

def trigger_network_failure_action(fault_injector_client, config, event: threading.Event = None):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config['bdb_id'], "delay": 2, "cluster_index": 0}
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

class TestActiveActive:

    def teardown_method(self, method):
        # Timeout so the cluster could recover from network failure.
        sleep(5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    def test_multi_db_client_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,config,event)
        )

        # Client initialized on the first command.
        r_multi_db.set('key', 'value')
        thread.start()

        # Execute commands before network failure
        while not event.is_set():
            assert r_multi_db.get('key') == 'value'
            sleep(0.5)

        # Execute commands until database failover
        while not listener.is_changed_flag:
            assert r_multi_db.get('key') == 'value'
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    def test_context_manager_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,config,event)
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
            sleep(0.5)

        # Execute pipeline until database failover
        for _ in range(5):
            with r_multi_db.pipeline() as pipe:
                pipe.set('{hash}key1', 'value1')
                pipe.set('{hash}key2', 'value2')
                pipe.set('{hash}key3', 'value3')
                pipe.get('{hash}key1')
                pipe.get('{hash}key2')
                pipe.get('{hash}key3')
                assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    def test_chaining_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,config,event)
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
            pipe = r_multi_db.pipeline()
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')
            assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
        sleep(0.5)

        # Execute pipeline until database failover
        for _ in range(5):
            pipe = r_multi_db.pipeline()
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')
            assert pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
        sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    def test_transaction_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,config,event)
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
            sleep(0.5)

        # Execute pipeline until database failover
        while not listener.is_changed_flag:
            r_multi_db.transaction(callback)
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    def test_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,config,event)
        )
        data = json.dumps({'message': 'test'})
        messages_count = 0

        def handler(message):
            nonlocal messages_count
            messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        pubsub.subscribe(**{'test-channel': handler})
        pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True)
        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            r_multi_db.publish('test-channel', data)
            sleep(0.5)

        # Execute pipeline until database failover
        while not listener.is_changed_flag:
            r_multi_db.publish('test-channel', data)
            sleep(0.5)

        pubsub_thread.stop()
        assert messages_count > 5

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    def test_sharded_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client,config,event)
        )
        data = json.dumps({'message': 'test'})
        messages_count = 0

        def handler(message):
            nonlocal messages_count
            messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        pubsub.ssubscribe(**{'test-channel': handler})
        pubsub_thread = pubsub.run_in_thread(
            sleep_time=0.1,
            daemon=True,
            sharded_pubsub=True
        )
        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            r_multi_db.spublish('test-channel', data)
            sleep(0.5)

        # Execute pipeline until database failover
        while not listener.is_changed_flag:
            r_multi_db.spublish('test-channel', data)
            sleep(0.5)

        pubsub_thread.stop()
        assert messages_count > 5