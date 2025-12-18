from queue import Queue
import json
import logging
import os
import threading
from time import sleep
import time
from typing import Optional

import pytest

from redis import Redis, RedisCluster
from redis.backoff import ConstantBackoff
from redis.client import Pipeline
from redis.multidb.exception import TemporaryUnavailableException
from redis.multidb.failover import DEFAULT_FAILOVER_ATTEMPTS, DEFAULT_FAILOVER_DELAY
from redis.multidb.healthcheck import LagAwareHealthCheck
from redis.retry import Retry
from redis.utils import dummy_fail
from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)
WAIT_FOR_FAILOVER_TIMEOUT = 3  # 3 seconds
SLEEP_BETWEEN_CHECKS = 0.1
SLEEP_BETWEEN_MESSAGES = 0.5
SLEEP_BETWEEN_COMMANDS = 0.5


def trigger_network_failure_action(
    fault_injector_client,
    config,
    event: Optional[threading.Event] = None,
    fault_injection_errors: Optional[Queue] = None,
):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config["bdb_id"], "delay": 3, "cluster_index": 0},
    )

    result = fault_injector_client.trigger_action(action_request)
    status_result = fault_injector_client.get_action_status(result["action_id"])

    waiting_iteration = 0
    while status_result["status"] != "success":
        sleep(SLEEP_BETWEEN_CHECKS)
        status_result = fault_injector_client.get_action_status(result["action_id"])
        if status_result["status"] == "failed":
            if fault_injection_errors:
                fault_injection_errors.put(status_result)
            if event:
                event.set()
                logger.info(
                    "Fault injection failed. Event is set so the test can continue"
                )
            logger.error(f"Action failed: {status_result}")
            raise Exception(f"Action failed: {status_result}")
        if waiting_iteration % 10 == 0:
            logger.info(
                f"Waiting for action to complete. Status: {status_result['status']}"
            )
        waiting_iteration += 1

    if event:
        event.set()
        logger.info("Fault injection completed. Event is set")

    logger.info(f"Action completed. Status: {status_result['status']}")


def should_continue(start_time, timeout):
    # timeout is in seconds
    return time.time() - start_time < timeout * 1000


class TestActiveActive:
    def teardown_method(self, method):
        # Timeout so the cluster could recover from network failure.
        sleep(10)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2},
            {"client_class": RedisCluster, "min_num_failures": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_multi_db_client_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        fault_injection_errors = Queue()
        trigger_network_failure_thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, fault_injection_errors),
        )

        # Client initialized on the first command.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )

        trigger_network_failure_thread.start()

        # Execute commands before network failure
        tmp_commands_executions = 0
        while not event.is_set():
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            tmp_commands_executions += 1
            logger.info(
                f"Executed {tmp_commands_executions} commands before network failure triggered event is set"
            )
            sleep(SLEEP_BETWEEN_COMMANDS)
        else:
            logger.info("Commands before network failure are completed!")

        # Execute commands until database failover
        tmp_commands_executions = 0
        start_time = time.time()
        while not listener.is_changed_flag:
            if should_continue(start_time, WAIT_FOR_FAILOVER_TIMEOUT):
                break
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            tmp_commands_executions += 1
            logger.info(
                f"Executed {tmp_commands_executions} commands after network failure triggered event is set"
            )
            sleep(SLEEP_BETWEEN_COMMANDS)
        else:
            logger.info("Multi db client failover completed!")

        trigger_network_failure_thread.join()
        assert fault_injection_errors.empty()

    @pytest.mark.skip(reason="Skip while validating credentials are configured in CI")
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2, "health_check_interval": 20},
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": 20,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_multi_db_client_uses_lag_aware_health_check(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event),
        )

        env0_username = os.getenv("ENV0_USERNAME")
        env0_password = os.getenv("ENV0_PASSWORD")

        # Adding additional health check to the client.
        r_multi_db.add_health_check(
            LagAwareHealthCheck(
                verify_tls=False,
                auth_basic=(env0_username, env0_password),
                lag_aware_tolerance=10000,
            )
        )

        # Client initialized on the first command.
        retry.call_with_retry(
            lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail()
        )
        thread.start()

        # Execute commands before network failure
        while not event.is_set():
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

        # Execute commands after network failure
        while not listener.is_changed_flag:
            assert (
                retry.call_with_retry(
                    lambda: r_multi_db.get("key"), lambda _: dummy_fail()
                )
                == "value"
            )
            sleep(0.5)

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2},
            {"client_class": RedisCluster, "min_num_failures": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_context_manager_pipeline_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        fault_injection_errors = Queue()
        trigger_network_failure_thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, fault_injection_errors),
        )

        def callback():
            with r_multi_db.pipeline() as pipe:
                pipe.set("{hash}key1", "value1")
                pipe.set("{hash}key2", "value2")
                pipe.set("{hash}key3", "value3")
                pipe.get("{hash}key1")
                pipe.get("{hash}key2")
                pipe.get("{hash}key3")
                assert pipe.execute() == [
                    True,
                    True,
                    True,
                    "value1",
                    "value2",
                    "value3",
                ]

        # Client initialized on first pipe execution.
        retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())

        trigger_network_failure_thread.start()

        # Execute pipeline before network failure
        tmp_pipelines_executions = 0
        while not event.is_set():
            try:
                retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
                tmp_pipelines_executions += 1
                logger.info(
                    f"Executed {tmp_pipelines_executions} pipelines before network failure triggered event is set"
                )
                sleep(SLEEP_BETWEEN_COMMANDS)
            except Exception as e:
                logger.error(f"Error executing pipeline: {e}")
        else:
            logger.info("Pipelines before network failure are completed!")

        # Execute pipeline until database failover
        tmp_pipelines_executions = 0
        start_time = time.time()
        while not listener.is_changed_flag:
            if should_continue(start_time, WAIT_FOR_FAILOVER_TIMEOUT):
                break
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            tmp_pipelines_executions += 1
            logger.info(
                f"Executed {tmp_pipelines_executions} pipelines after network failure triggered event is set"
            )
            sleep(SLEEP_BETWEEN_COMMANDS)
        else:
            logger.info("Multi db client failover completed!")

        trigger_network_failure_thread.join()
        assert fault_injection_errors.empty()

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2},
            {"client_class": RedisCluster, "min_num_failures": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_chaining_pipeline_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        fault_injection_errors = Queue()
        trigger_network_failure_thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, fault_injection_errors),
        )

        def callback():
            pipe = r_multi_db.pipeline()
            pipe.set("{hash}key1", "value1")
            pipe.set("{hash}key2", "value2")
            pipe.set("{hash}key3", "value3")
            pipe.get("{hash}key1")
            pipe.get("{hash}key2")
            pipe.get("{hash}key3")
            assert pipe.execute() == [True, True, True, "value1", "value2", "value3"]

        # Client initialized on first pipe execution.
        retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())

        trigger_network_failure_thread.start()

        # Execute pipeline before network failure
        tmp_pipelines_executions = 0
        while not event.is_set():
            try:
                retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
                tmp_pipelines_executions += 1
                logger.info(
                    f"Executed {tmp_pipelines_executions} pipelines before network failure triggered event is set"
                )
                sleep(SLEEP_BETWEEN_COMMANDS)
            except Exception as e:
                logger.error(f"Error executing pipeline: {e}")
        else:
            logger.info("Pipelines before network failure are completed!")

        # Execute pipeline until database failover
        tmp_pipelines_executions = 0
        start_time = time.time()
        while not listener.is_changed_flag:
            if should_continue(start_time, WAIT_FOR_FAILOVER_TIMEOUT):
                break
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            tmp_pipelines_executions += 1
            logger.info(
                f"Executed {tmp_pipelines_executions} pipelines after network failure triggered event is set"
            )
            sleep(SLEEP_BETWEEN_COMMANDS)
        else:
            logger.info("Multi db client failover completed!")

        trigger_network_failure_thread.join()
        assert fault_injection_errors.empty()

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2},
            {"client_class": RedisCluster, "min_num_failures": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_transaction_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        fault_injection_errors = Queue()
        trigger_network_failure_thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, fault_injection_errors),
        )

        def callback(pipe: Pipeline):
            pipe.set("{hash}key1", "value1")
            pipe.set("{hash}key2", "value2")
            pipe.set("{hash}key3", "value3")
            pipe.get("{hash}key1")
            pipe.get("{hash}key2")
            pipe.get("{hash}key3")

        # Client initialized on first transaction execution.
        retry.call_with_retry(
            lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
        )
        trigger_network_failure_thread.start()

        # Execute transaction before network failure
        tmp_transactions_executions = 0
        while not event.is_set():
            try:
                retry.call_with_retry(
                    lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
                )
                tmp_transactions_executions += 1
                logger.info(
                    f"Executed {tmp_transactions_executions} transactions before network failure triggered event is set"
                )
                sleep(SLEEP_BETWEEN_COMMANDS)
            except Exception as e:
                logger.error(f"Error executing transaction: {e}")
        else:
            logger.info("Transactions before network failure are completed!")

        # Execute transaction until database failover
        tmp_transactions_executions = 0
        start_time = time.time()
        logger.info("Waiting for multi db client failover to complete")

        while not listener.is_changed_flag:
            if should_continue(start_time, WAIT_FOR_FAILOVER_TIMEOUT):
                break
            retry.call_with_retry(
                lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
            )
            tmp_transactions_executions += 1
            logger.info(
                f"Executed {tmp_transactions_executions} transactions after network failure triggered event is set"
            )
            sleep(SLEEP_BETWEEN_COMMANDS)
        else:
            logger.info("Multi db client failover completed!")

        trigger_network_failure_thread.join()
        assert fault_injection_errors.empty()

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2},
            {"client_class": RedisCluster, "min_num_failures": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        fault_injection_errors = Queue()
        fault_injection_thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, fault_injection_errors),
        )
        data = json.dumps({"message": "test"})
        handled_messages_count = 0

        def handler(message):
            nonlocal handled_messages_count
            handled_messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        retry.call_with_retry(
            lambda: pubsub.subscribe(**{"test-channel": handler}),
            lambda _: dummy_fail(),
        )
        pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True)

        fault_injection_thread.start()

        # Execute publish before network failure
        tmp_messages_count = 0
        while not event.is_set():
            try:
                retry.call_with_retry(
                    lambda: r_multi_db.publish("test-channel", data),
                    lambda _: dummy_fail(),
                )
                tmp_messages_count += 1
                logger.info(
                    f"Published {tmp_messages_count} messages before network failure triggered event is set"
                )
                sleep(SLEEP_BETWEEN_MESSAGES)
            except Exception as e:
                logger.error(f"Error publishing message: {e}")
        else:
            logger.info("Publishing before network failure is completed!")

        # Execute publish until database failover
        tmp_messages_count = 0
        start_time = time.time()
        logger.info("Waiting for multi db client failover to complete")
        while not listener.is_changed_flag:
            if should_continue(start_time, WAIT_FOR_FAILOVER_TIMEOUT):
                break
            retry.call_with_retry(
                lambda: r_multi_db.publish("test-channel", data), lambda _: dummy_fail()
            )
            tmp_messages_count += 1
            logger.info(
                f"Published {tmp_messages_count} messages after network failure triggered event is set"
            )
            sleep(SLEEP_BETWEEN_MESSAGES)
        else:
            logger.info("Multi db client failover completed!")

        fault_injection_thread.join()
        pubsub_thread.stop()
        assert fault_injection_errors.empty()
        assert handled_messages_count > 2

    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "min_num_failures": 2},
            {"client_class": RedisCluster, "min_num_failures": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(100)
    def test_sharded_pubsub_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        r_multi_db, listener, config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        event = threading.Event()
        fault_injection_errors = Queue()
        trigger_network_failure_thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event, fault_injection_errors),
        )
        data = json.dumps({"message": "test"})
        handled_message_count = 0

        def handler(message):
            nonlocal handled_message_count
            handled_message_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        retry.call_with_retry(
            lambda: pubsub.ssubscribe(**{"test-channel": handler}),
            lambda _: dummy_fail(),
        )
        pubsub_thread = pubsub.run_in_thread(
            sleep_time=SLEEP_BETWEEN_CHECKS, daemon=True, sharded_pubsub=True
        )
        trigger_network_failure_thread.start()

        # Execute publish before network failure
        tmp_messages_count = 0
        while not event.is_set():
            try:
                retry.call_with_retry(
                    lambda: r_multi_db.spublish("test-channel", data),
                    lambda _: dummy_fail(),
                )
                tmp_messages_count += 1
                logger.info(
                    f"Published {tmp_messages_count} messages before network failure triggered event is set"
                )
                sleep(SLEEP_BETWEEN_MESSAGES)
            except Exception as e:
                logger.error(f"Error publishing message: {e}")
        else:
            logger.info("Publishing before network failure is completed!")

        # Execute publish until database failover
        tmp_messages_count = 0
        start_time = time.time()
        logger.info("Waiting for multi db client failover to complete")

        while not listener.is_changed_flag:
            if should_continue(start_time, WAIT_FOR_FAILOVER_TIMEOUT):
                break
            retry.call_with_retry(
                lambda: r_multi_db.spublish("test-channel", data),
                lambda _: dummy_fail(),
            )
            tmp_messages_count += 1
            logger.info(
                f"Published {tmp_messages_count} messages after network failure triggered event is set"
            )

            sleep(SLEEP_BETWEEN_MESSAGES)
        else:
            logger.info("Multi db client failover completed!")

        trigger_network_failure_thread.join()
        pubsub_thread.stop()

        assert fault_injection_errors.empty()
        assert handled_message_count > 2
