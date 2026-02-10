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
)
from tests.test_scenario.conftest import _wait_for_cluster_healthy_in_fixture

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


# TestActiveActiveWithHitless constants
NETWORK_LATENCY_DELAY_MS = 2000
NETWORK_LATENCY_DURATION = 60
SOCKET_TIMEOUT = 1
SLOT_SHUFFLE_TIMEOUT = 240
DATA_PRELOAD_KEY_COUNT = 100
DATA_PRELOAD_VALUE_SIZE = 102400
RECOVERY_LATENCY_DURATION = 60
RECOVERY_POLL_TIMEOUT = 120
SUCCESSIVE_LATENCY_DURATION = 120
DATA_INTEGRITY_PRELOAD_COUNT = 50
DATA_INTEGRITY_MIGRATION_WRITES = 50
CLUSTER_HEALTH_TIMEOUT = 90
TEARDOWN_LATENCY_WAIT = 70
LATENCY_BUFFER_SECONDS = 5
COMMAND_POLL_INTERVAL = 0.1
CIRCUIT_RECOVERY_POLL_INTERVAL = 2.0
NUMBERED_SET_INTERVAL = 0.05
BOUNDARY_LATENCY_MS = 3000
DEFAULT_PORT = 6379


class TestActiveActiveWithHitless:
    """
    Tests proving hitless timeout relaxation behaviour during network
    latency on an OSS cluster with Active-Active MultiDBClient.

    Common setup:
      - socket_timeout = 1s
      - network_latency = 2s (exceeds socket_timeout)
      - ~10 MB of data pre-loaded to slow down the shard migration
      - failure_rate_threshold = 0.90

    Tests:
      1. Hitless relaxation PREVENTS CB trip (relaxed_timeout=5, latency=2s).
      2. Hitless boundary: CB trips when latency EXCEEDS relaxed_timeout (relaxed_timeout=2, latency=3s).
      3. Control: CB trips WITHOUT hitless relaxation (relaxed_timeout=-1).
      4. Hitless with failover trigger (relaxed_timeout=5, trigger=failover).
      5. AA recovery after CB trip (circuit returns to CLOSED).
      6. Successive slot shuffles with hitless (migration storm).
      7. Data integrity during hitless migration.
    """

    @staticmethod
    def _ensure_cluster_healthy(endpoint_config, timeout=CLUSTER_HEALTH_TIMEOUT):
        from urllib.parse import urlparse

        endpoint_url = endpoint_config["endpoints"][0]
        parsed = urlparse(endpoint_url)
        host = parsed.hostname
        port = parsed.port or DEFAULT_PORT
        username = endpoint_config.get("username", "default")
        password = endpoint_config.get("password")
        use_ssl = parsed.scheme == "rediss"

        _wait_for_cluster_healthy_in_fixture(
            host, port, username, password, timeout=timeout, use_ssl=use_ssl
        )

    def teardown_method(self, method):
        sleep(TEARDOWN_LATENCY_WAIT)

    @staticmethod
    def _create_retry():
        return Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

    @staticmethod
    def _warmup_client(multi_db_client, retry):
        logging.info("Initializing MultiDBClient with warm-up commands")
        retry.call_with_retry(
            lambda: multi_db_client.set("warmup_key", "warmup_value"),
            lambda _: dummy_fail(),
        )

    @staticmethod
    def _preload_with_logging(multi_db_client):
        logging.info(
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        TestActiveActiveWithHitless._preload_data(
            multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE
        )

    @staticmethod
    def _create_latency_thread(fault_injector_client, bdb_id, delay_ms=NETWORK_LATENCY_DELAY_MS, duration=NETWORK_LATENCY_DURATION):
        return threading.Thread(
            target=fault_injector_client.trigger_network_latency,
            daemon=True,
            args=(bdb_id, delay_ms, duration),
            kwargs={"cluster_index": 0},
        )

    @staticmethod
    def _log_latency_trigger(bdb_id, delay_ms=NETWORK_LATENCY_DELAY_MS, duration=NETWORK_LATENCY_DURATION):
        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
            delay_ms,
            duration,
            bdb_id,
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": 5,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 20,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 120,
            },
        ],
        ids=["hitless_relaxation_enabled"],
        indirect=True,
    )
    @pytest.mark.timeout(600)
    def test_hitless_relaxation_prevents_cb_trip_during_network_latency(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")
        self._preload_with_logging(multi_db_client)

        self._log_latency_trigger(bdb_id)
        latency_thread = self._create_latency_thread(fault_injector_client_oss_api, bdb_id)
        latency_thread.start()

        migration_complete = threading.Event()

        logging.info("Step 2: Triggering SLOT_SHUFFLE (trigger=migrate)")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle_and_signal,
            daemon=True,
            args=(fault_injector_client_oss_api, endpoint_config, migration_complete),
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info("Step 3: Executing commands during latency + migration")
        cmd_thread = threading.Thread(
            target=self._execute_commands_until_event_or_timeout,
            daemon=True,
            args=(multi_db_client, errors, migration_complete, NETWORK_LATENCY_DURATION),
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

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": 2,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 120,
            },
        ],
        ids=["hitless_boundary_exceeded"],
        indirect=True,
    )
    @pytest.mark.timeout(600)
    def test_cb_trips_when_latency_exceeds_relaxed_timeout(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")
        self._preload_with_logging(multi_db_client)

        self._log_latency_trigger(bdb_id, BOUNDARY_LATENCY_MS, NETWORK_LATENCY_DURATION)
        latency_thread = self._create_latency_thread(
            fault_injector_client_oss_api, bdb_id, BOUNDARY_LATENCY_MS
        )
        latency_thread.start()

        logging.info("Step 2: Triggering SLOT_SHUFFLE (trigger=migrate)")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle,
            daemon=True,
            args=(fault_injector_client_oss_api, endpoint_config),
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info("Step 3: Executing commands -- expecting CB trip")
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
            "AA failover SHOULD have occurred -- latency (3s) exceeds "
            "relaxed_timeout (2s), so commands should have timed out"
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": -1,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 5,
            },
        ],
        ids=["hitless_relaxation_disabled"],
        indirect=True,
    )
    @pytest.mark.timeout(600)
    def test_cb_trips_without_hitless_relaxation_during_network_latency(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")
        self._preload_with_logging(multi_db_client)

        self._log_latency_trigger(bdb_id)
        latency_thread = self._create_latency_thread(fault_injector_client_oss_api, bdb_id)
        latency_thread.start()
        logging.info("Waiting for network latency to be applied...")
        latency_thread.join()
        logging.info("Network latency applied, continuing test")

        logging.info("Step 2: Triggering SLOT_SHUFFLE (trigger=migrate)")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle,
            daemon=True,
            args=(fault_injector_client_oss_api, endpoint_config),
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info("Step 3: Executing commands -- expecting CB trip")
        cmd_thread = threading.Thread(
            target=self._execute_commands_until_failover_or_timeout,
            daemon=True,
            args=(multi_db_client, listener, errors, NETWORK_LATENCY_DURATION),
        )
        cmd_thread.start()
        cmd_thread.join()

        trigger_effect_thread.join()

        logging.info(
            "Verifying CB DID trip (is_changed_flag=%s)", listener.is_changed_flag
        )
        assert listener.is_changed_flag, (
            "AA failover SHOULD have occurred -- without hitless relaxation "
            "the CB should have tripped due to command timeouts"
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": 5,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 20,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 120,
            },
        ],
        ids=["hitless_with_failover_trigger"],
        indirect=True,
    )
    @pytest.mark.timeout(600)
    def test_hitless_relaxation_with_failover_trigger(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")
        self._preload_with_logging(multi_db_client)

        self._log_latency_trigger(bdb_id)
        latency_thread = self._create_latency_thread(fault_injector_client_oss_api, bdb_id)
        latency_thread.start()

        migration_complete = threading.Event()

        logging.info("Step 2: Triggering SLOT_SHUFFLE (trigger=failover)")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle_and_signal,
            daemon=True,
            args=(
                fault_injector_client_oss_api,
                endpoint_config,
                migration_complete,
            ),
            kwargs={"trigger_name": "failover"},
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info("Step 3: Executing commands during latency + migration")
        cmd_thread = threading.Thread(
            target=self._execute_commands_until_event_or_timeout,
            daemon=True,
            args=(
                multi_db_client,
                errors,
                migration_complete,
                NETWORK_LATENCY_DURATION,
            ),
        )
        cmd_thread.start()
        cmd_thread.join()

        trigger_effect_thread.join()
        latency_thread.join()

        logging.info(
            "Verifying CB did NOT trip (is_changed_flag=%s)",
            listener.is_changed_flag,
        )
        assert not listener.is_changed_flag, (
            "AA failover should NOT have occurred -- hitless relaxation "
            "should work with trigger=failover just as with trigger=migrate"
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": -1,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 5,
            },
        ],
        ids=["aa_recovery_after_cb_trip"],
        indirect=True,
    )
    @pytest.mark.timeout(600)
    def test_aa_recovery_after_hitless_cb_trip(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")
        self._preload_with_logging(multi_db_client)

        self._log_latency_trigger(bdb_id, NETWORK_LATENCY_DELAY_MS, RECOVERY_LATENCY_DURATION)
        latency_thread = self._create_latency_thread(
            fault_injector_client_oss_api, bdb_id, duration=RECOVERY_LATENCY_DURATION
        )
        latency_thread.start()
        logging.info("Waiting for network latency to be applied...")
        latency_thread.join()
        logging.info("Network latency applied, continuing test")

        logging.info("Step 2: Triggering SLOT_SHUFFLE (trigger=migrate)")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle,
            daemon=True,
            args=(fault_injector_client_oss_api, endpoint_config),
        )
        trigger_effect_thread.start()

        errors = Queue()
        logging.info("Step 3: Executing commands -- expecting CB trip")
        cmd_thread = threading.Thread(
            target=self._execute_commands_until_failover_or_timeout,
            daemon=True,
            args=(multi_db_client, listener, errors, RECOVERY_LATENCY_DURATION),
        )
        cmd_thread.start()
        cmd_thread.join()

        logging.info(
            "Verifying CB DID trip (is_changed_flag=%s)",
            listener.is_changed_flag,
        )
        assert listener.is_changed_flag, (
            "AA failover SHOULD have occurred as precondition for recovery test"
        )

        trigger_effect_thread.join()
        latency_thread.join()

        logging.info(
            "Step 4: Network latency expired. Waiting up to %ds for "
            "circuit recovery (OPEN -> HALF_OPEN -> CLOSED)",
            RECOVERY_POLL_TIMEOUT,
        )
        recovered = self._wait_for_circuit_recovery(
            multi_db_client,
            db_index=0,
            timeout=RECOVERY_POLL_TIMEOUT,
        )
        assert recovered, (
            "Original DB circuit should have recovered to CLOSED after "
            "network latency expired"
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": 5,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 20,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 180,
            },
        ],
        ids=["successive_migrations"],
        indirect=True,
    )
    @pytest.mark.timeout(900)
    def test_successive_migrations_with_hitless(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")
        self._preload_with_logging(multi_db_client)

        self._log_latency_trigger(bdb_id, NETWORK_LATENCY_DELAY_MS, SUCCESSIVE_LATENCY_DURATION)
        latency_thread = self._create_latency_thread(
            fault_injector_client_oss_api, bdb_id, duration=SUCCESSIVE_LATENCY_DURATION
        )
        latency_thread.start()

        both_migrations_complete = threading.Event()

        def _run_two_shuffles():
            logging.info("Starting SLOT_SHUFFLE #1")
            self._trigger_slot_shuffle(fault_injector_client_oss_api, endpoint_config)
            logging.info("SLOT_SHUFFLE #1 done, starting SLOT_SHUFFLE #2")
            self._trigger_slot_shuffle(fault_injector_client_oss_api, endpoint_config)
            logging.info("SLOT_SHUFFLE #2 done, signalling completion")
            both_migrations_complete.set()

        logging.info("Step 2: Triggering two successive SLOT_SHUFFLEs")
        migration_thread = threading.Thread(target=_run_two_shuffles, daemon=True)
        migration_thread.start()

        errors = Queue()
        logging.info("Step 3: Executing commands during both migrations")
        cmd_thread = threading.Thread(
            target=self._execute_commands_until_event_or_timeout,
            daemon=True,
            args=(
                multi_db_client,
                errors,
                both_migrations_complete,
                SUCCESSIVE_LATENCY_DURATION,
            ),
        )
        cmd_thread.start()
        cmd_thread.join()

        migration_thread.join()
        latency_thread.join()

        logging.info(
            "Verifying CB did NOT trip after two successive migrations "
            "(is_changed_flag=%s)",
            listener.is_changed_flag,
        )
        assert not listener.is_changed_flag, (
            "AA failover should NOT have occurred -- hitless relaxation "
            "should handle successive SMIGRATING/SMIGRATED cycles "
            "without accumulating failures"
        )

    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": 5,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 20,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 120,
            },
        ],
        ids=["data_integrity_during_hitless"],
        indirect=True,
    )
    @pytest.mark.timeout(600)
    def test_data_integrity_during_hitless_migration(
        self,
        r_multi_db_with_hitless,
        fault_injector_client_oss_api,
    ):
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless
        self._ensure_cluster_healthy(endpoint_config)

        retry = self._create_retry()
        self._warmup_client(multi_db_client, retry)

        bdb_id = endpoint_config.get("bdb_id")

        logging.info(
            "Step 0: Pre-loading %d keys with deterministic values",
            DATA_INTEGRITY_PRELOAD_COUNT,
        )
        expected_data = {}
        for i in range(DATA_INTEGRITY_PRELOAD_COUNT):
            key = f"integrity_preload_{i}"
            value = f"preload_val_{i}"
            multi_db_client.set(key, value)
            expected_data[key] = value
        logging.info("Pre-load complete: %d keys", DATA_INTEGRITY_PRELOAD_COUNT)

        migration_complete = threading.Event()
        latency_start_time = time.time()

        self._log_latency_trigger(bdb_id)
        latency_thread = self._create_latency_thread(fault_injector_client_oss_api, bdb_id)
        latency_thread.start()

        logging.info("Step 2: Triggering SLOT_SHUFFLE (trigger=migrate)")
        trigger_effect_thread = threading.Thread(
            target=self._trigger_slot_shuffle_and_signal,
            daemon=True,
            args=(
                fault_injector_client_oss_api,
                endpoint_config,
                migration_complete,
            ),
        )
        trigger_effect_thread.start()

        logging.info("Step 3: Writing numbered keys during migration")
        migration_keys = self._execute_numbered_sets(
            multi_db_client,
            prefix="integrity_migration",
            count=DATA_INTEGRITY_MIGRATION_WRITES,
            stop_event=migration_complete,
            max_duration=NETWORK_LATENCY_DURATION,
        )
        expected_data.update(migration_keys)

        trigger_effect_thread.join()
        latency_thread.join(timeout=CLUSTER_HEALTH_TIMEOUT)

        elapsed = time.time() - latency_start_time
        remaining = NETWORK_LATENCY_DURATION - elapsed + LATENCY_BUFFER_SECONDS
        if remaining > 0:
            logging.info(
                "Waiting %.1fs for network latency to be removed...", remaining
            )
            sleep(remaining)

        logging.info("Waiting for cluster to become healthy after latency...")
        self._ensure_cluster_healthy(endpoint_config, timeout=120)

        logging.info(
            "Step 4: Verifying data integrity for %d total keys",
            len(expected_data),
        )
        mismatches = self._verify_data_integrity(multi_db_client, expected_data)
        assert not mismatches, (
            f"Data integrity failed -- {len(mismatches)} mismatches:\n"
            + "\n".join(mismatches[:20])
        )

        logging.info(
            "CB state after migration (is_changed_flag=%s) - CB trip during "
            "SMIGRATED handling is acceptable if data integrity passed",
            listener.is_changed_flag,
        )

    @staticmethod
    def _preload_data(multi_db_client, key_count: int, value_size: int):
        value = "x" * value_size
        for i in range(key_count):
            multi_db_client.set(f"preload_{i}", value)
            if (i + 1) % 100 == 0:
                logging.info("Pre-loaded %d / %d keys", i + 1, key_count)
        logging.info(
            "Pre-load complete: %d keys, ~%d MB",
            key_count,
            key_count * value_size // (1024 * 1024),
        )

    @staticmethod
    def _trigger_slot_shuffle(
        fault_injector_client, endpoint_config, trigger_name="migrate"
    ):
        logging.info(
            "Starting SLOT_SHUFFLE trigger effect (trigger=%s)", trigger_name
        )
        action_id = ClusterOperations.trigger_effect(
            fault_injector=fault_injector_client,
            endpoint_config=endpoint_config,
            effect_name=SlotMigrateEffects.SLOT_SHUFFLE,
            trigger_name=trigger_name,
        )
        fault_injector_client.get_operation_result(
            action_id, timeout=SLOT_SHUFFLE_TIMEOUT
        )
        logging.info("SLOT_SHUFFLE trigger effect completed")

    @staticmethod
    def _trigger_slot_shuffle_and_signal(
        fault_injector_client, endpoint_config, event, trigger_name="migrate"
    ):
        TestActiveActiveWithHitless._trigger_slot_shuffle(
            fault_injector_client, endpoint_config, trigger_name=trigger_name
        )
        logging.info("Setting migration_complete event")
        event.set()

    @staticmethod
    def _execute_commands_until_event_or_timeout(
        multi_db_client, errors: Queue, stop_event: threading.Event, max_duration: int
    ):
        start = time.time()
        executed = 0
        while time.time() - start < max_duration:
            if stop_event.is_set():
                logging.info(
                    "Migration complete signal received after %d commands, stopping",
                    executed,
                )
                return
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
            sleep(COMMAND_POLL_INTERVAL)
        logging.info("Command execution finished. Total commands: %d", executed)

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
            sleep(COMMAND_POLL_INTERVAL)
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
            sleep(COMMAND_POLL_INTERVAL)
        logging.info(
            "Max duration reached without failover. Total commands: %d",
            executed,
        )

    @staticmethod
    def _wait_for_circuit_recovery(
        multi_db_client, db_index: int, timeout: int, poll_interval: float = CIRCUIT_RECOVERY_POLL_INTERVAL
    ):
        from redis.multidb.circuit import State as CBState

        databases = multi_db_client.get_databases()
        db_list = databases.get_top_n(len(list(databases)))
        target_db, _ = db_list[db_index]

        start = time.time()
        while time.time() - start < timeout:
            state = target_db.circuit.state
            logging.info(
                "Polling circuit state for db[%d]: %s (elapsed=%.1fs)",
                db_index,
                state,
                time.time() - start,
            )
            if state == CBState.CLOSED:
                logging.info(
                    "Circuit recovered to CLOSED after %.1fs",
                    time.time() - start,
                )
                return True
            sleep(poll_interval)
        logging.info(
            "Circuit did NOT recover within %ds, final state: %s",
            timeout,
            target_db.circuit.state,
        )
        return False

    @staticmethod
    def _execute_numbered_sets(
        multi_db_client,
        prefix: str,
        count: int,
        stop_event: threading.Event,
        max_duration: int,
    ):
        start = time.time()
        written_keys = {}
        i = 0
        while i < count and time.time() - start < max_duration:
            if stop_event.is_set():
                logging.info(
                    "Stop signal received after writing %d/%d keys", i, count
                )
                break
            key = f"{prefix}_{i}"
            value = f"val_{i}"
            try:
                multi_db_client.set(key, value)
                written_keys[key] = value
                i += 1
            except Exception as e:
                logging.info("SET failed for %s: %s", key, e)
            sleep(NUMBERED_SET_INTERVAL)
        logging.info("Numbered SET complete: %d keys written", len(written_keys))
        return written_keys

    @staticmethod
    def _verify_data_integrity(multi_db_client, expected: dict):
        mismatches = []
        for key, expected_value in expected.items():
            actual = multi_db_client.get(key)
            if actual != expected_value:
                mismatches.append(
                    f"{key}: expected={expected_value!r}, got={actual!r}"
                )
        if mismatches:
            logging.info(
                "Data integrity check: %d mismatches out of %d keys",
                len(mismatches),
                len(expected),
            )
        else:
            logging.info(
                "Data integrity check passed: all %d keys correct",
                len(expected),
            )
        return mismatches
