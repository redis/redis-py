import json
import logging
import os
import threading
import time
from queue import Queue
from time import sleep
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
from tests.test_scenario.fault_injector_client import (
    ActionRequest,
    ActionType,
    SlotMigrateEffects,
)
from tests.test_scenario.maint_notifications_helpers import (
    ClusterOperations,
    KeyGenerationHelpers,
)

logger = logging.getLogger(__name__)


def trigger_network_failure_action(
    fault_injector_client, config, event: Optional[threading.Event] = None
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


@pytest.mark.skip(reason="Temporarily disabled")
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


NETWORK_LATENCY_DELAY_MS = 3000
NETWORK_LATENCY_DURATION = 60
SOCKET_TIMEOUT = 2
SLOT_SHUFFLE_TIMEOUT = 120


class TestActiveActiveWithHitless:
    """
    Two mirror tests proving hitless timeout relaxation is the decisive factor
    in preventing (or allowing) CB trips during network latency on an OSS
    cluster with Active-Active MultiDBClient.

    Both tests share the same setup:
      - socket_timeout = 2s
      - network_latency = 3s (exceeds socket_timeout)
      - min_num_failures = 2, failure_rate_threshold = 0.51

    The ONLY difference is relaxed_timeout:
      - Test 1: relaxed_timeout=10 -> timeout relaxed to 10s during SMIGRATING,
        commands succeed (3s < 10s), CB stays closed, NO AA failover.
      - Test 2: relaxed_timeout=-1 -> timeout stays at 2s,
        commands timeout (3s > 2s), CB trips, AA failover occurs.
    """

    def teardown_method(self, method):
        sleep(10)

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": 10,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.51,
            },
        ],
        ids=["hitless_relaxation_enabled"],
        indirect=True,
    )
    @pytest.mark.timeout(300)
    def test_hitless_relaxation_prevents_cb_trip_during_network_latency(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        """
        With relaxed_timeout=10, SMIGRATING relaxes socket timeout to 10s.
        Despite 3s network latency (> 2s socket_timeout), commands succeed
        because 3s < 10s relaxed timeout. CB stays closed, no AA failover.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        logging.info("Initializing MultiDBClient with warm-up commands")
        retry.call_with_retry(
            lambda: multi_db_client.set("warmup_key", "warmup_value"),
            lambda _: dummy_fail(),
        )

        bdb_id = endpoint_config.get("bdb_id")

        logging.info(
            "Triggering network latency (%dms, %ds duration) on active DB (bdb_id=%s)",
            NETWORK_LATENCY_DELAY_MS,
            NETWORK_LATENCY_DURATION,
            bdb_id,
        )
        latency_thread = threading.Thread(
            target=fault_injector_client_oss_api.trigger_network_latency,
            daemon=True,
            args=(bdb_id, NETWORK_LATENCY_DELAY_MS, NETWORK_LATENCY_DURATION),
            kwargs={"cluster_index": 0},
        )
        latency_thread.start()

        logging.info("Triggering SLOT_SHUFFLE on active DB")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle,
            daemon=True,
            args=(fault_injector_client_oss_api, endpoint_config),
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info("Executing commands during network latency + slot migration")
        cmd_thread = threading.Thread(
            target=self._execute_commands_for_duration,
            daemon=True,
            args=(multi_db_client, errors, NETWORK_LATENCY_DURATION),
        )
        cmd_thread.start()
        cmd_thread.join()

        trigger_effect_thread.join()
        latency_thread.join()

        logging.info(
            "Verifying CB did NOT trip (is_changed_flag=%s)", listener.is_changed_flag
        )
        assert not listener.is_changed_flag, (
            "AA failover should NOT have occurred -- hitless relaxation "
            "should have prevented the CB from tripping"
        )
        assert errors.empty(), (
            f"Commands should not have failed, but got errors: {list(errors.queue)}"
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": -1,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.51,
            },
        ],
        ids=["hitless_relaxation_disabled"],
        indirect=True,
    )
    @pytest.mark.timeout(300)
    def test_cb_trips_without_hitless_relaxation_during_network_latency(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        """
        With relaxed_timeout=-1, SMIGRATING is received but socket timeout
        stays at 2s. With 3s network latency, commands timeout. CB trips
        after 2 failures, triggering AA failover to the standby DB.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        logging.info("Initializing MultiDBClient with warm-up commands")
        retry.call_with_retry(
            lambda: multi_db_client.set("warmup_key", "warmup_value"),
            lambda _: dummy_fail(),
        )

        bdb_id = endpoint_config.get("bdb_id")

        logging.info(
            "Triggering network latency (%dms, %ds duration) on active DB (bdb_id=%s)",
            NETWORK_LATENCY_DELAY_MS,
            NETWORK_LATENCY_DURATION,
            bdb_id,
        )
        latency_thread = threading.Thread(
            target=fault_injector_client_oss_api.trigger_network_latency,
            daemon=True,
            args=(bdb_id, NETWORK_LATENCY_DELAY_MS, NETWORK_LATENCY_DURATION),
            kwargs={"cluster_index": 0},
        )
        latency_thread.start()

        logging.info("Triggering SLOT_SHUFFLE on active DB")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle,
            daemon=True,
            args=(fault_injector_client_oss_api, endpoint_config),
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info(
            "Executing commands -- expecting timeouts and CB trip"
        )
        cmd_thread = threading.Thread(
            target=self._execute_commands_until_failover_or_timeout,
            daemon=True,
            args=(multi_db_client, listener, errors, NETWORK_LATENCY_DURATION),
        )
        cmd_thread.start()
        cmd_thread.join()

        trigger_effect_thread.join()
        latency_thread.join()

        logging.info(
            "Verifying CB DID trip (is_changed_flag=%s)", listener.is_changed_flag
        )
        assert listener.is_changed_flag, (
            "AA failover SHOULD have occurred -- without hitless relaxation "
            "the CB should have tripped due to command timeouts"
        )
        assert not errors.empty(), (
            "Expected command failures before failover, but none were recorded"
        )

    @staticmethod
    def _trigger_slot_shuffle(fault_injector_client, endpoint_config):
        logging.info("Starting SLOT_SHUFFLE trigger effect")
        action_id = ClusterOperations.trigger_effect(
            fault_injector=fault_injector_client,
            endpoint_config=endpoint_config,
            effect_name=SlotMigrateEffects.SLOT_SHUFFLE,
        )
        fault_injector_client.get_operation_result(
            action_id, timeout=SLOT_SHUFFLE_TIMEOUT
        )
        logging.info("SLOT_SHUFFLE trigger effect completed")

    @staticmethod
    def _execute_commands_for_duration(
        multi_db_client, errors: Queue, duration: int
    ):
        start = time.time()
        executed = 0
        while time.time() - start < duration:
            key = f"aa_hitless_test_key_{executed}"
            try:
                multi_db_client.set(key, "value")
                result = multi_db_client.get(key)
                if result != "value":
                    errors.put(f"Unexpected GET result for {key}: {result}")
                executed += 2
            except Exception as e:
                logging.info("Command failed: %s", e)
                errors.put(str(e))
            if executed % 100 == 0:
                logging.info("Executed %d commands so far", executed)
            sleep(0.1)
        logging.info("Command execution finished. Total commands: %d", executed)

    @staticmethod
    def _execute_commands_until_failover_or_timeout(
        multi_db_client, listener, errors: Queue, max_duration: int
    ):
        start = time.time()
        executed = 0
        while time.time() - start < max_duration:
            key = f"aa_hitless_test_key_{executed}"
            try:
                multi_db_client.set(key, "value")
                multi_db_client.get(key)
                executed += 2
            except Exception as e:
                logging.info("Command failed (expected): %s", e)
                errors.put(str(e))
            if listener.is_changed_flag:
                logging.info(
                    "AA failover detected after %d commands", executed
                )
                return
            sleep(0.1)
        logging.info(
            "Max duration reached without failover. Total commands: %d",
            executed,
        )
