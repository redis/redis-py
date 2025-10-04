import json
import logging
import os
import threading
from time import sleep

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


def trigger_network_failure_action(
    fault_injector_client, config, event: threading.Event = None
):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config["bdb_id"], "delay": 3, "cluster_index": 0},
    )

    result = fault_injector_client.trigger_action(action_request)
    status_result = fault_injector_client.get_action_status(result["action_id"])

    while status_result["status"] != "success":
        sleep(0.1)
        status_result = fault_injector_client.get_action_status(result["action_id"])
        logger.info(
            f"Waiting for action to complete. Status: {status_result['status']}"
        )

    if event:
        event.set()

    logger.info(f"Action completed. Status: {status_result['status']}")


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
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event),
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

        # Execute commands until database failover
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
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event),
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
        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
            sleep(0.5)

        # Execute pipeline until database failover
        for _ in range(5):
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
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
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event),
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

        thread.start()

        # Execute pipeline before network failure
        while not event.is_set():
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
        sleep(0.5)

        # Execute pipeline until database failover
        for _ in range(5):
            retry.call_with_retry(lambda: callback(), lambda _: dummy_fail())
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
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event),
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
        thread.start()

        # Execute transaction before network failure
        while not event.is_set():
            retry.call_with_retry(
                lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
            )
            sleep(0.5)

        # Execute transaction until database failover
        while not listener.is_changed_flag:
            retry.call_with_retry(
                lambda: r_multi_db.transaction(callback), lambda _: dummy_fail()
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
    def test_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
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
        data = json.dumps({"message": "test"})
        messages_count = 0

        def handler(message):
            nonlocal messages_count
            messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        retry.call_with_retry(
            lambda: pubsub.subscribe(**{"test-channel": handler}),
            lambda _: dummy_fail(),
        )
        pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True)
        thread.start()

        # Execute publish before network failure
        while not event.is_set():
            retry.call_with_retry(
                lambda: r_multi_db.publish("test-channel", data), lambda _: dummy_fail()
            )
            sleep(0.5)

        # Execute publish until database failover
        while not listener.is_changed_flag:
            retry.call_with_retry(
                lambda: r_multi_db.publish("test-channel", data), lambda _: dummy_fail()
            )
            sleep(0.5)

        pubsub_thread.stop()
        assert messages_count > 2

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
        thread = threading.Thread(
            target=trigger_network_failure_action,
            daemon=True,
            args=(fault_injector_client, config, event),
        )
        data = json.dumps({"message": "test"})
        messages_count = 0

        def handler(message):
            nonlocal messages_count
            messages_count += 1

        pubsub = r_multi_db.pubsub()

        # Assign a handler and run in a separate thread.
        retry.call_with_retry(
            lambda: pubsub.ssubscribe(**{"test-channel": handler}),
            lambda _: dummy_fail(),
        )
        pubsub_thread = pubsub.run_in_thread(
            sleep_time=0.1, daemon=True, sharded_pubsub=True
        )
        thread.start()

        # Execute publish before network failure
        while not event.is_set():
            retry.call_with_retry(
                lambda: r_multi_db.spublish("test-channel", data),
                lambda _: dummy_fail(),
            )
            sleep(0.5)

        # Execute publish until database failover
        while not listener.is_changed_flag:
            retry.call_with_retry(
                lambda: r_multi_db.spublish("test-channel", data),
                lambda _: dummy_fail(),
            )
            sleep(0.5)

        pubsub_thread.stop()
        assert messages_count > 2
