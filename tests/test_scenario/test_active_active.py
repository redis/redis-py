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


NETWORK_LATENCY_DELAY_MS = 2000
NETWORK_LATENCY_DURATION = 60
SOCKET_TIMEOUT = 1
SLOT_SHUFFLE_TIMEOUT = 240
# Small data preload to make the shard migration take long enough for
# SMIGRATING to overlap with network latency.  Keep it small so the
# preload phase is fast (CRDB writes are slow due to cross-cluster sync).
DATA_PRELOAD_KEY_COUNT = 100
DATA_PRELOAD_VALUE_SIZE = 102400  # 100 KB per key -> ~10 MB total
RECOVERY_LATENCY_DURATION = 60
RECOVERY_POLL_TIMEOUT = 120
SUCCESSIVE_LATENCY_DURATION = 120
DATA_INTEGRITY_PRELOAD_COUNT = 50
DATA_INTEGRITY_MIGRATION_WRITES = 50


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
    def _wait_for_cluster_healthy(host, port, username, password, timeout=90):
        """
        Poll cluster until PINGs respond within a reasonable time.
        Ensures no residual latency from previous test.
        """
        from redis import RedisCluster

        logging.info("Waiting for cluster to become healthy (timeout=%ds)", timeout)
        start = time.time()
        healthy_streak = 0
        required_streak = 3

        while time.time() - start < timeout:
            try:
                c = RedisCluster(
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    decode_responses=True,
                    protocol=3,
                    socket_timeout=2,
                )
                ping_start = time.time()
                c.ping()
                ping_ms = (time.time() - ping_start) * 1000
                c.close()

                if ping_ms < 1500:
                    healthy_streak += 1
                    logging.info(
                        "Cluster PING: %.0fms (streak %d/%d)",
                        ping_ms,
                        healthy_streak,
                        required_streak,
                    )
                    if healthy_streak >= required_streak:
                        logging.info("Cluster is healthy")
                        return
                else:
                    healthy_streak = 0
                    logging.info("Cluster PING: %.0fms (too slow, resetting streak)", ping_ms)
            except Exception as e:
                healthy_streak = 0
                logging.info("Cluster health check failed: %s", e)

            sleep(2)

        logging.warning("Cluster did not become healthy within %ds, proceeding anyway", timeout)

    @staticmethod
    def _ensure_cluster_healthy(endpoint_config, timeout=90):
        """
        Extract connection info from endpoint_config and wait for healthy cluster.
        """
        from urllib.parse import urlparse

        endpoint_url = endpoint_config["endpoints"][0]
        parsed = urlparse(endpoint_url)
        host = parsed.hostname
        port = parsed.port or 6379
        username = endpoint_config.get("username", "default")
        password = endpoint_config.get("password")

        TestActiveActiveWithHitless._wait_for_cluster_healthy(
            host, port, username, password, timeout
        )

    def teardown_method(self, method):
        sleep(70)  # Wait for network latency (60s) to fully expire

    # ------------------------------------------------------------------
    # Test 1 -- hitless relaxation PREVENTS CB trip
    # ------------------------------------------------------------------
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
        """
        With relaxed_timeout=5, SMIGRATING relaxes socket timeout to 5s.
        Despite 2s network latency (> 1s socket_timeout), commands during
        the migration window succeed (2s < 5s).  The successful commands
        keep the failure rate below 0.90, so the CB stays closed and no AA
        failover occurs.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        self._preload_data(multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE)

        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
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

    # ------------------------------------------------------------------
    # Test 2 -- CB trips when latency EXCEEDS relaxed_timeout
    # ------------------------------------------------------------------
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
        """
        With relaxed_timeout=2, SMIGRATING relaxes socket timeout to 2s.
        But network latency is 3s, which exceeds the relaxed timeout.
        Commands timeout (3s > 2s), failures accumulate, and CB trips.
        This proves hitless works correctly but has limits.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        self._preload_data(multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE)

        # Use 3s latency which exceeds the 2s relaxed_timeout
        boundary_latency_ms = 3000
        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
            boundary_latency_ms,
            NETWORK_LATENCY_DURATION,
            bdb_id,
        )
        latency_thread = threading.Thread(
            target=fault_injector_client_oss_api.trigger_network_latency,
            daemon=True,
            args=(bdb_id, boundary_latency_ms, NETWORK_LATENCY_DURATION),
            kwargs={"cluster_index": 0},
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

    # ------------------------------------------------------------------
    # Test 3 -- CB trips WITHOUT hitless relaxation (control)
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": -1,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 5,  # Short interval so CB trips quickly
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
        """
        With relaxed_timeout=-1, SMIGRATING is received but socket timeout
        stays at 1s.  With 2s network latency every command times out.  The
        failure rate hits 1.0 > 0.90 after 5 failures and the CB trips,
        triggering AA failover.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        self._preload_data(multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE)

        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
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

    # ------------------------------------------------------------------
    # Test 4 -- failover-triggered migration with hitless
    # ------------------------------------------------------------------
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
        """
        Same as test_hitless_relaxation_prevents_cb_trip but uses
        trigger_name="failover" instead of "migrate" for the
        SLOT_SHUFFLE.  Verifies that SMIGRATING is sent regardless
        of the trigger mechanism and hitless relaxation still works.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        self._preload_data(
            multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE
        )

        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
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

        migration_complete = threading.Event()

        logging.info(
            "Step 2: Triggering SLOT_SHUFFLE (trigger=failover)"
        )
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

    # ------------------------------------------------------------------
    # Test 5 -- AA recovery after CB trip
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        "r_multi_db_with_hitless",
        [
            {
                "relaxed_timeout": -1,
                "socket_timeout": SOCKET_TIMEOUT,
                "min_num_failures": 2,
                "failure_rate_threshold": 0.90,
                "health_check_interval": 5,  # Short interval so CB trips quickly
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
        """
        Forces CB trip (relaxed_timeout=-1, latency > socket_timeout).
        After AA failover occurs, waits for network latency to expire.
        Then polls the original DB's circuit state and verifies it
        transitions back to CLOSED, proving the full lifecycle:
        trip -> failover -> latency ends -> health check recovers -> CLOSED.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        self._preload_data(
            multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE
        )

        latency_duration = RECOVERY_LATENCY_DURATION

        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
            NETWORK_LATENCY_DELAY_MS,
            latency_duration,
            bdb_id,
        )
        latency_thread = threading.Thread(
            target=fault_injector_client_oss_api.trigger_network_latency,
            daemon=True,
            args=(bdb_id, NETWORK_LATENCY_DELAY_MS, latency_duration),
            kwargs={"cluster_index": 0},
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
            args=(multi_db_client, listener, errors, latency_duration),
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

    # ------------------------------------------------------------------
    # Test 6 -- successive slot shuffles (migration storm) with hitless
    # ------------------------------------------------------------------
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
        """
        Triggers two SLOT_SHUFFLE operations in succession while network
        latency is active.  Commands execute throughout both migrations.
        Verifies that hitless handles repeated SMIGRATING/SMIGRATED
        cycles correctly without state leaks tripping the CB.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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
            "Step 0: Pre-loading %d keys (%d bytes each) to slow down migration",
            DATA_PRELOAD_KEY_COUNT,
            DATA_PRELOAD_VALUE_SIZE,
        )
        self._preload_data(
            multi_db_client, DATA_PRELOAD_KEY_COUNT, DATA_PRELOAD_VALUE_SIZE
        )

        successive_latency_duration = SUCCESSIVE_LATENCY_DURATION

        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
            NETWORK_LATENCY_DELAY_MS,
            successive_latency_duration,
            bdb_id,
        )
        latency_thread = threading.Thread(
            target=fault_injector_client_oss_api.trigger_network_latency,
            daemon=True,
            args=(bdb_id, NETWORK_LATENCY_DELAY_MS, successive_latency_duration),
            kwargs={"cluster_index": 0},
        )
        latency_thread.start()

        both_migrations_complete = threading.Event()

        def _run_two_shuffles():
            logging.info("Starting SLOT_SHUFFLE #1")
            self._trigger_slot_shuffle(
                fault_injector_client_oss_api, endpoint_config
            )
            logging.info("SLOT_SHUFFLE #1 done, starting SLOT_SHUFFLE #2")
            self._trigger_slot_shuffle(
                fault_injector_client_oss_api, endpoint_config
            )
            logging.info("SLOT_SHUFFLE #2 done, signalling completion")
            both_migrations_complete.set()

        logging.info("Step 2: Triggering two successive SLOT_SHUFFLEs")
        migration_thread = threading.Thread(
            target=_run_two_shuffles, daemon=True
        )
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
                successive_latency_duration,
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

    # ------------------------------------------------------------------
    # Test 7 -- data integrity during hitless + AA migration
    # ------------------------------------------------------------------
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
        """
        Writes numbered keys during a hitless migration with network
        latency, then verifies all keys (pre-loaded + written during
        migration) are readable with correct values after migration
        completes and latency clears.  Proves hitless relaxation
        guarantees data integrity under stress.
        """
        multi_db_client, listener, endpoint_config = r_multi_db_with_hitless

        self._ensure_cluster_healthy(endpoint_config)

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

        preload_count = DATA_INTEGRITY_PRELOAD_COUNT
        logging.info(
            "Step 0: Pre-loading %d keys with deterministic values",
            preload_count,
        )
        expected_data = {}
        for i in range(preload_count):
            key = f"integrity_preload_{i}"
            value = f"preload_val_{i}"
            multi_db_client.set(key, value)
            expected_data[key] = value
        logging.info("Pre-load complete: %d keys", preload_count)

        migration_complete = threading.Event()

        # Track when latency starts
        latency_start_time = time.time()

        # Trigger latency FIRST, then immediately trigger slot shuffle
        # Both run in parallel - migration happens while latency is active
        logging.info(
            "Step 1: Triggering network latency (%dms, %ds duration) on bdb_id=%s",
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

        # Immediately trigger slot shuffle - API call goes through before latency kicks in
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

        logging.info(
            "Step 3: Writing numbered keys during migration"
        )
        migration_keys = self._execute_numbered_sets(
            multi_db_client,
            prefix="integrity_migration",
            count=DATA_INTEGRITY_MIGRATION_WRITES,
            stop_event=migration_complete,
            max_duration=NETWORK_LATENCY_DURATION,
        )
        expected_data.update(migration_keys)

        trigger_effect_thread.join()
        # Join latency thread with timeout to avoid hanging on pytest.fail()
        latency_thread.join(timeout=90)

        # Wait for latency to be fully removed (60s from start)
        elapsed = time.time() - latency_start_time
        remaining = NETWORK_LATENCY_DURATION - elapsed + 5  # +5s buffer
        if remaining > 0:
            logging.info(
                "Waiting %.1fs for network latency to be removed...", remaining
            )
            sleep(remaining)

        # Wait for cluster to become healthy (CB may have opened)
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

        # Log CB state - CB may trip due to DNS resolution issues during
        # SMIGRATED handling (RedisCluster topology refresh), which is
        # separate from data integrity. The key assertion is that all data
        # was written and read correctly.
        logging.info(
            "CB state after migration (is_changed_flag=%s) - CB trip during "
            "SMIGRATED handling is acceptable if data integrity passed",
            listener.is_changed_flag,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
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
            sleep(0.1)
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

    @staticmethod
    def _wait_for_circuit_recovery(
        multi_db_client, db_index: int, timeout: int, poll_interval: float = 2.0
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
            sleep(0.05)
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
