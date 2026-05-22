import asyncio
import copy
import json
import logging
import threading
import time
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

import pytest
import pytest_asyncio
from redis import RedisCluster
from redis.asyncio import RedisCluster as AsyncRedisCluster
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import ExponentialWithJitterBackoff
from redis.retry import Retry
from tests.helpers import wait_for_condition
from tests.test_asyncio.helpers import wait_for_condition as async_wait_for_condition
from tests.test_scenario.fault_injector_client import (
    ActionRequest,
    ActionType,
    FaultInjectorClient,
    NodeInfo,
    ProxyServerFaultInjector,
)
from tests.test_scenario.common_scenario_helpers import (
    ClusterOperations,
    KeyGenerationHelpers,
    delete_database_if_exists,
)


POST_RECOVERY_DELIVERY_RATIO = 0.90
BASELINE_TIMEOUT = 30
RECOVERY_TIMEOUT = 120
PUBLISH_INTERVAL = 0.02
PUBSUB_PROGRESS_LOG_MESSAGE_INTERVAL = 300
PUBSUB_TEST_SHARDS_COUNT = 3
PUBSUB_CLIENT_TIMEOUT = 5


FAILURE_SCENARIOS = [
    pytest.param(
        "failover",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.FAILOVER,
            parameters={
                "bdb_id": endpoint_config["bdb_id"],
                "cluster_index": 0,
            },
        ),
        id="failover",
    ),
    pytest.param(
        "node_reboot",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.NODE_FAILURE,
            parameters={
                "cluster_index": 0,
                "node_id": 1,
                "method": "reboot",
            },
        ),
        id="node-reboot",
    ),
    pytest.param(
        "proxy_restart",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.PROXY_FAILURE,
            parameters={
                "bdb_id": endpoint_config["bdb_id"],
                "cluster_index": 0,
                "action": "restart",
            },
        ),
        id="proxy-restart",
    ),
    pytest.param(
        "shard_failure",
        lambda endpoint_config: ActionRequest(
            action_type=ActionType.SHARD_FAILURE,
            parameters={
                "bdb_id": endpoint_config["bdb_id"],
                "cluster_index": 0,
            },
        ),
        id="shard-failure",
    ),
]


def make_pubsub_db_config(base_config, test_name):
    db_config = copy.deepcopy(base_config)
    db_config["shards_count"] = PUBSUB_TEST_SHARDS_COUNT
    return db_config


def get_cluster_client(
    endpoints_config: dict[str, Any],
    client_class: type[RedisCluster] | type[AsyncRedisCluster] = RedisCluster,
    protocol: int = 3,
    retry_class: type[Retry] | type[AsyncRetry] = Retry,
    socket_timeout: float = PUBSUB_CLIENT_TIMEOUT,
) -> RedisCluster | AsyncRedisCluster:
    endpoints = endpoints_config.get("endpoints", [])
    if not endpoints:
        raise ValueError("No endpoints found in configuration")

    parsed = urlparse(endpoints[0])
    if not parsed.hostname:
        raise ValueError(f"Could not parse host from endpoint URL: {endpoints[0]}")
    if parsed.scheme == "rediss":
        raise ValueError("Pub/Sub scenario tests do not support TLS endpoints")

    return client_class(
        host=parsed.hostname,
        port=parsed.port,
        socket_timeout=socket_timeout,
        username=endpoints_config.get("username"),
        password=endpoints_config.get("password"),
        protocol=protocol,
        retry=retry_class(
            backoff=ExponentialWithJitterBackoff(base=0.1, cap=10),
            retries=10,
        ),
    )


@pytest.fixture()
def cluster_endpoint_config(
    request,
    fault_injector_client_oss_api: FaultInjectorClient,
    pubsub_cluster_bdb_config,
):
    if isinstance(fault_injector_client_oss_api, ProxyServerFaultInjector):
        pytest.skip("mock proxy does not currently support Pub/Sub flows")

    db_config = make_pubsub_db_config(pubsub_cluster_bdb_config, request.node.name)

    delete_database_if_exists(fault_injector_client_oss_api, db_config["name"])
    try:
        endpoint_config = fault_injector_client_oss_api.create_database(db_config)
        endpoint_config["shards_count"] = db_config["shards_count"]
        yield endpoint_config
    finally:
        delete_database_if_exists(fault_injector_client_oss_api, db_config["name"])


@pytest.fixture()
def cluster_client(
    cluster_endpoint_config,
):
    client = get_cluster_client(
        endpoints_config=cluster_endpoint_config,
    )
    try:
        yield client
    finally:
        client.close()


@pytest_asyncio.fixture()
async def async_cluster_client(
    cluster_endpoint_config,
):
    client = get_cluster_client(
        endpoints_config=cluster_endpoint_config,
        client_class=AsyncRedisCluster,
        retry_class=AsyncRetry,
    )
    try:
        yield client
    finally:
        await client.aclose()


def run_sharded_pubsub_scenario(
    client,
    endpoint_config,
    channel_prefix,
    subscriber_count,
    cluster_op_action,
):
    channels = KeyGenerationHelpers.generate_keys_for_all_shards(
        shards_count=endpoint_config["shards_count"],
        prefix=channel_prefix,
        keys_per_shard=1,
    )
    state_lock = threading.Lock()
    subscribers = []
    received_by_subscriber = [defaultdict(set) for _ in range(subscriber_count)]
    stop_event = threading.Event()
    sent_by_channel = defaultdict(set)
    publisher_thread = None
    sent_messages = 0
    received_messages = 0
    publish_errors = 0
    subscriber_errors = 0

    logging.info(
        "Pub/Sub scenario started: channels=%s subscribers=%s",
        len(channels),
        subscriber_count,
    )

    def progress_message():
        with state_lock:
            subscriber_threads_alive = sum(
                thread.is_alive() for _, thread in subscribers
            )
            return (
                f"sent={sent_messages}, received={received_messages}, "
                f"publish_errors={publish_errors}, "
                f"subscriber_errors={subscriber_errors}, "
                f"subscriber_threads_alive={subscriber_threads_alive}/{len(subscribers)}"
            )

    def handle_subscriber_error(error, pubsub, thread):
        nonlocal subscriber_errors
        with state_lock:
            subscriber_errors += 1
            errors = subscriber_errors
        if errors == 1 or errors % 10 == 0:
            logging.info(
                "Pub/Sub subscriber read error: errors=%s error=%r",
                errors,
                error,
            )

    def publish_messages():
        nonlocal publish_errors, sent_messages
        seq_by_channel = defaultdict(int)
        while not stop_event.is_set():
            for channel in channels:
                seq = seq_by_channel[channel]
                payload = json.dumps({"channel": channel, "seq": seq})
                try:
                    client.spublish(channel, payload)
                except Exception:
                    # Pub/Sub is best-effort during injected infrastructure faults.
                    # Only successfully published post-recovery messages enter the
                    # delivery-ratio denominator.
                    with state_lock:
                        publish_errors += 1
                else:
                    seq_by_channel[channel] += 1
                    with state_lock:
                        sent_by_channel[channel].add(seq)
                        sent_messages += 1
                        should_log_progress = (
                            sent_messages % PUBSUB_PROGRESS_LOG_MESSAGE_INTERVAL == 0
                        )
                        sent = sent_messages
                        received = received_messages
                        errors = publish_errors
                        sub_errors = subscriber_errors
                        subscriber_threads_alive = sum(
                            thread.is_alive() for _, thread in subscribers
                        )
                    if should_log_progress:
                        logging.info(
                            "Pub/Sub progress: sent=%s received=%s "
                            "publish_errors=%s subscriber_errors=%s "
                            "subscriber_threads_alive=%s/%s",
                            sent,
                            received,
                            errors,
                            sub_errors,
                            subscriber_threads_alive,
                            len(subscribers),
                        )
            time.sleep(PUBLISH_INTERVAL)

    try:
        for index in range(subscriber_count):
            pubsub = client.pubsub()

            def make_handler(subscriber_index):
                def handler(message):
                    nonlocal received_messages
                    payload = json.loads(message["data"])
                    with state_lock:
                        received_by_subscriber[subscriber_index][
                            payload["channel"]
                        ].add(payload["seq"])
                        received_messages += 1

                return handler

            pubsub.ssubscribe(**{channel: make_handler(index) for channel in channels})
            thread = pubsub.run_in_thread(
                sleep_time=0.01,
                daemon=True,
                exception_handler=handle_subscriber_error,
                sharded_pubsub=True,
            )
            subscribers.append((pubsub, thread))

        publisher_thread = threading.Thread(
            target=publish_messages,
            daemon=True,
        )
        publisher_thread.start()
        logging.info("Pub/Sub publisher thread started")

        def baseline_messages_received():
            with state_lock:
                return all(
                    all(len(subscriber[channel]) >= 3 for channel in channels)
                    for subscriber in received_by_subscriber
                )

        try:
            wait_for_condition(
                baseline_messages_received,
                timeout=BASELINE_TIMEOUT,
                check_interval=0.1,
                error_message=(
                    "Timed out waiting for each subscriber to receive messages"
                ),
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Pub/Sub baseline reached: %s", progress_message())

        logging.info("Pub/Sub cluster action started: %s", cluster_op_action.__name__)
        cluster_op_action()
        logging.info("Pub/Sub cluster action completed: %s", progress_message())

        client.nodes_manager.initialize()
        time.sleep(5)

        with state_lock:
            recovery_baseline = {
                channel: set(seqs) for channel, seqs in sent_by_channel.items()
            }

        def enough_messages_sent():
            with state_lock:
                return all(
                    len(
                        sent_by_channel[channel] - recovery_baseline.get(channel, set())
                    )
                    >= 20
                    for channel in channels
                )

        try:
            wait_for_condition(
                enough_messages_sent,
                timeout=RECOVERY_TIMEOUT,
                check_interval=0.1,
                error_message="Timed out waiting for post-recovery messages to be sent",
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Pub/Sub post-recovery publishes reached: %s", progress_message())

        with state_lock:
            sent_after_recovery = {
                channel: sent_by_channel[channel]
                - recovery_baseline.get(channel, set())
                for channel in channels
            }

        def delivery_ratio_met():
            with state_lock:
                if any(not sent_after_recovery[channel] for channel in channels):
                    return False
                for subscriber in received_by_subscriber:
                    for channel in channels:
                        delivered = len(
                            subscriber[channel] & sent_after_recovery[channel]
                        )
                        ratio = delivered / len(sent_after_recovery[channel])
                        if ratio < POST_RECOVERY_DELIVERY_RATIO:
                            return False
                return True

        try:
            wait_for_condition(
                delivery_ratio_met,
                timeout=RECOVERY_TIMEOUT,
                check_interval=0.1,
                error_message="Timed out waiting for post-recovery delivery ratio",
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Pub/Sub delivery ratio reached: %s", progress_message())
    finally:
        stop_event.set()
        if publisher_thread is not None:
            publisher_thread.join(timeout=5)
        for pubsub, thread in subscribers:
            thread.stop()
            thread.join(timeout=5)
            pubsub.close()
        logging.info("Pub/Sub scenario stopped: %s", progress_message())


async def async_run_sharded_pubsub_recovery_scenario(
    client,
    endpoint_config,
    channel_prefix,
    subscriber_count,
    cluster_op_action,
):
    channels = KeyGenerationHelpers.generate_keys_for_all_shards(
        shards_count=endpoint_config["shards_count"],
        prefix=channel_prefix,
        keys_per_shard=1,
    )
    subscribers = []
    reader_tasks = []
    received_by_subscriber = [defaultdict(set) for _ in range(subscriber_count)]
    stop_event = asyncio.Event()
    sent_by_channel = defaultdict(set)
    publisher_task = None
    sent_messages = 0
    received_messages = 0
    publish_errors = 0

    logging.info(
        "Async Pub/Sub scenario started: channels=%s subscribers=%s",
        len(channels),
        subscriber_count,
    )

    def progress_message():
        return (
            f"sent={sent_messages}, received={received_messages}, "
            f"publish_errors={publish_errors}"
        )

    async def publish_messages():
        nonlocal publish_errors, sent_messages
        seq_by_channel = defaultdict(int)
        while not stop_event.is_set():
            for channel in channels:
                seq = seq_by_channel[channel]
                payload = json.dumps({"channel": channel, "seq": seq})
                try:
                    await client.spublish(channel, payload)
                except Exception:
                    # Pub/Sub is best-effort during injected infrastructure faults.
                    # Only successfully published post-recovery messages enter the
                    # delivery-ratio denominator.
                    publish_errors += 1
                else:
                    seq_by_channel[channel] += 1
                    sent_by_channel[channel].add(seq)
                    sent_messages += 1
                    if sent_messages % PUBSUB_PROGRESS_LOG_MESSAGE_INTERVAL == 0:
                        logging.info(
                            "Async Pub/Sub progress: sent=%s received=%s "
                            "publish_errors=%s",
                            sent_messages,
                            received_messages,
                            publish_errors,
                        )
            await asyncio.sleep(PUBLISH_INTERVAL)

    async def read_messages(pubsub):
        while not stop_event.is_set():
            try:
                await pubsub.get_sharded_message(
                    ignore_subscribe_messages=True,
                    timeout=0.01,
                )
            except Exception:
                if stop_event.is_set():
                    return
                await asyncio.sleep(0.05)

    try:
        for index in range(subscriber_count):
            pubsub = client.pubsub()

            def make_handler(subscriber_index):
                async def handler(message):
                    nonlocal received_messages
                    payload = json.loads(message["data"])
                    received_by_subscriber[subscriber_index][payload["channel"]].add(
                        payload["seq"]
                    )
                    received_messages += 1

                return handler

            await pubsub.ssubscribe(
                **{channel: make_handler(index) for channel in channels}
            )
            subscribers.append(pubsub)
            reader_tasks.append(asyncio.create_task(read_messages(pubsub)))

        publisher_task = asyncio.create_task(publish_messages())
        logging.info("Async Pub/Sub publisher task started")

        def baseline_messages_received():
            return all(
                all(len(subscriber[channel]) >= 3 for channel in channels)
                for subscriber in received_by_subscriber
            )

        try:
            await async_wait_for_condition(
                baseline_messages_received,
                timeout=BASELINE_TIMEOUT,
                check_interval=0.1,
                error_message=(
                    "Timed out waiting for each subscriber to receive messages"
                ),
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Async Pub/Sub baseline reached: %s", progress_message())

        logging.info("Async Pub/Sub cluster action started: %s", cluster_op_action.__name__)
        await asyncio.to_thread(cluster_op_action)
        logging.info("Async Pub/Sub cluster action completed: %s", progress_message())

        await client.nodes_manager.initialize()
        await asyncio.sleep(5)

        recovery_baseline = {
            channel: set(seqs) for channel, seqs in sent_by_channel.items()
        }

        def enough_messages_sent():
            return all(
                len(sent_by_channel[channel] - recovery_baseline.get(channel, set()))
                >= 20
                for channel in channels
            )

        try:
            await async_wait_for_condition(
                enough_messages_sent,
                timeout=RECOVERY_TIMEOUT,
                check_interval=0.1,
                error_message="Timed out waiting for post-recovery messages to be sent",
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info(
            "Async Pub/Sub post-recovery publishes reached: %s",
            progress_message(),
        )

        sent_after_recovery = {
            channel: sent_by_channel[channel] - recovery_baseline.get(channel, set())
            for channel in channels
        }

        def delivery_ratio_met():
            if any(not sent_after_recovery[channel] for channel in channels):
                return False
            for subscriber in received_by_subscriber:
                for channel in channels:
                    delivered = len(subscriber[channel] & sent_after_recovery[channel])
                    ratio = delivered / len(sent_after_recovery[channel])
                    if ratio < POST_RECOVERY_DELIVERY_RATIO:
                        return False
            return True

        try:
            await async_wait_for_condition(
                delivery_ratio_met,
                timeout=RECOVERY_TIMEOUT,
                check_interval=0.1,
                error_message="Timed out waiting for post-recovery delivery ratio",
            )
        except AssertionError as error:
            raise AssertionError(f"{error}; {progress_message()}") from error
        logging.info("Async Pub/Sub delivery ratio reached: %s", progress_message())
    finally:
        stop_event.set()
        tasks = reader_tasks + ([publisher_task] if publisher_task is not None else [])
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for pubsub in subscribers:
            await pubsub.aclose()
        logging.info("Async Pub/Sub scenario stopped: %s", progress_message())


class TestPubSubBase:
    @pytest.fixture(autouse=True)
    def setup(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        cluster_endpoint_config,
    ):
        try:
            target_node, empty_node = ClusterOperations.find_target_node_and_empty_node(
                fault_injector_client_oss_api, cluster_endpoint_config
            )
            logging.info(f"Using target_node: {target_node}, empty_node: {empty_node}")
        except Exception as e:
            pytest.fail(f"Failed to find target and empty nodes: {e}")

        # Ensure setup completed successfully
        if not target_node or not empty_node:
            pytest.fail("Setup failed: target_node or empty_node not available")

        self.target_node: NodeInfo = target_node
        self.empty_node: NodeInfo = empty_node


class TestShardedPubSubMigrationScenario(TestPubSubBase):
    @pytest.mark.timeout(300)
    def test_sharded_pubsub_delivery_after_shard_migration(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        cluster_endpoint_config,
        cluster_client,
    ):
        def migrate():
            ClusterOperations.execute_migrate(
                fault_injector=fault_injector_client_oss_api,
                endpoint_config=cluster_endpoint_config,
                target_node=self.target_node.node_id,
                empty_node=self.empty_node.node_id,
                timeout=RECOVERY_TIMEOUT,
            )

        run_sharded_pubsub_scenario(
            cluster_client,
            cluster_endpoint_config,
            channel_prefix="pubsub-migration",
            subscriber_count=1,
            cluster_op_action=migrate,
        )


class TestShardedPubSubInfrastructureRecovery(TestPubSubBase):
    @pytest.mark.timeout(420)
    @pytest.mark.parametrize("subscriber_count", [1, 2])
    @pytest.mark.parametrize("failure_name, create_action", FAILURE_SCENARIOS)
    def test_sharded_pubsub_recovers_after_infrastructure_failure(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        subscriber_count,
        failure_name,
        create_action,
        cluster_endpoint_config,
        cluster_client,
    ):
        def inject_failure():
            result = fault_injector_client_oss_api.trigger_action(
                create_action(cluster_endpoint_config)
            )
            fault_injector_client_oss_api.get_operation_result(
                result["action_id"],
                timeout=RECOVERY_TIMEOUT,
            )

        run_sharded_pubsub_scenario(
            cluster_client,
            cluster_endpoint_config,
            channel_prefix=f"pubsub-recovery-{failure_name}",
            subscriber_count=subscriber_count,
            cluster_op_action=inject_failure,
        )


class TestAsyncShardedPubSubFaultInjectorMigrationScenario(TestPubSubBase):
    @pytest.mark.asyncio
    @pytest.mark.timeout(300)
    async def test_sharded_pubsub_delivery_after_migration(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        cluster_endpoint_config,
        async_cluster_client,
    ):
        def migrate():
            target_node, empty_node = ClusterOperations.find_target_node_and_empty_node(
                fault_injector_client_oss_api,
                cluster_endpoint_config,
            )
            ClusterOperations.execute_migrate(
                fault_injector=fault_injector_client_oss_api,
                endpoint_config=cluster_endpoint_config,
                target_node=target_node.node_id,
                empty_node=empty_node.node_id,
                timeout=RECOVERY_TIMEOUT,
            )

        await async_run_sharded_pubsub_recovery_scenario(
            async_cluster_client,
            cluster_endpoint_config,
            channel_prefix="async-pubsub-migration",
            subscriber_count=1,
            cluster_op_action=migrate,
        )


class TestAsyncShardedPubSubInfrastructureRecovery(TestPubSubBase):
    @pytest.mark.asyncio
    @pytest.mark.timeout(420)
    @pytest.mark.parametrize("subscriber_count", [1, 2])
    @pytest.mark.parametrize("failure_name, create_action", FAILURE_SCENARIOS)
    async def test_sharded_pubsub_recovers_after_infrastructure_failure(
        self,
        fault_injector_client_oss_api: FaultInjectorClient,
        subscriber_count,
        failure_name,
        create_action,
        cluster_endpoint_config,
        async_cluster_client,
    ):
        def inject_failure():
            result = fault_injector_client_oss_api.trigger_action(
                create_action(cluster_endpoint_config)
            )
            fault_injector_client_oss_api.get_operation_result(
                result["action_id"],
                timeout=RECOVERY_TIMEOUT,
            )

        await async_run_sharded_pubsub_recovery_scenario(
            async_cluster_client,
            cluster_endpoint_config,
            channel_prefix=f"async-pubsub-recovery-{failure_name}",
            subscriber_count=subscriber_count,
            cluster_op_action=inject_failure,
        )
