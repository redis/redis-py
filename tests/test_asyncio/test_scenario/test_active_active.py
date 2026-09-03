import asyncio
import json
import logging
import os
from time import monotonic, sleep

import pytest

from redis.asyncio import RedisCluster
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.multidb.failover import (
    DEFAULT_FAILOVER_ATTEMPTS,
    DEFAULT_FAILOVER_DELAY,
)
from redis.asyncio.multidb.healthcheck import LagAwareHealthCheck
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff
from redis.multidb.exception import TemporaryUnavailableException
from redis.utils import dummy_fail_async
from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)

# The injected network failure is transient - the fault injector restores the link a
# few seconds after the action is triggered. A database is only taken out of service by
# a health check probe that runs while the link is down, and on the default interval a
# probe round is short next to the pause that follows it, so most rounds land after the
# link is already back and no failover is ever initiated. Probing back to back keeps a
# round inside the outage.
FAILOVER_HEALTH_CHECK_INTERVAL = 0.1
# Bounded here rather than left to pytest-timeout so a failover that never happens is
# reported as a failover that never happened, instead of as a stack dump of whatever
# the test was doing when the deadline passed. Kept well inside the per-test timeout so
# the assertion below is what fires.
FAILOVER_TIMEOUT = 60
FAILOVER_TIMEOUT_MESSAGE = (
    f"Active database has not changed within {FAILOVER_TIMEOUT} seconds of the "
    "injected network failure"
)
# The Redis Enterprise REST API credentials LagAwareHealthCheck authenticates with.
# They come from the test environment, and without them every probe gets HTTP 401 and
# both databases are reported unhealthy - which fails the initial health check instead
# of exercising the health check the test is about.
LAG_AWARE_CREDENTIAL_ENV_VARS = ("ENV0_USERNAME", "ENV0_PASSWORD")
# The whole health check - every probe of it - has to finish inside this budget, and
# each probe of this one is two REST calls to the Redis Enterprise API. The default of
# 3 seconds covers a PING, not 3 probes x 2 requests over the public internet with a
# second of it spent in the delay between probes, and running out of it reports the
# database as unhealthy.
LAG_AWARE_HEALTH_CHECK_TIMEOUT = 10


def lag_aware_auth_basic():
    """
    Return the REST API credentials for LagAwareHealthCheck as the environment
    supplies them.
    """
    return tuple(os.getenv(name) for name in LAG_AWARE_CREDENTIAL_ENV_VARS)


def require_lag_aware_credentials():
    """
    Fail the calling test up front when the environment does not supply the REST API
    credentials.

    Deliberately a failure and not a skip: a skipped test is invisible in a scenario
    run's log, and the alternative is every probe returning HTTP 401 and the run
    reporting InitialHealthCheckFailedError, which reads like a client bug.

    Kept apart from lag_aware_auth_basic because the health check here is built at
    collection time, where failing the individual test is not available.
    """
    missing = ", ".join(
        name for name in LAG_AWARE_CREDENTIAL_ENV_VARS if not os.getenv(name)
    )

    if missing:
        pytest.fail(
            "LagAwareHealthCheck requires the Redis Enterprise REST API credentials "
            f"in {missing}. Set them from the CI secrets of the same name."
        )


async def trigger_network_failure_action(
    fault_injector_client, config, event: asyncio.Event = None
):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config["bdb_id"], "delay": 3, "cluster_index": 0},
    )

    result = await fault_injector_client.trigger_action(action_request)
    status_result = await fault_injector_client.get_operation_result(
        result["action_id"]
    )

    if event:
        event.set()

    logger.info(f"Action completed. Status: {status_result['status']}")


class TestActiveActive:
    def teardown_method(self, method):
        # Timeout so the cluster could recover from network failure. Runs between
        # tests, outside the per-test event loop, so a blocking sleep is fine.
        sleep(10)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(200)
    async def test_multi_db_client_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        client, listener, endpoint_config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(
                trigger_network_failure_action(
                    fault_injector_client, endpoint_config, event
                )
            )

            await retry.call_with_retry(
                lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail_async()
            )

            # Execute commands before network failure
            while not event.is_set():
                assert (
                    await retry.call_with_retry(
                        lambda: r_multi_db.get("key"), lambda _: dummy_fail_async()
                    )
                    == "value"
                )
                await asyncio.sleep(0.5)

            # Execute commands until database failover
            deadline = monotonic() + FAILOVER_TIMEOUT
            while not listener.is_changed_flag:
                assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
                assert (
                    await retry.call_with_retry(
                        lambda: r_multi_db.get("key"), lambda _: dummy_fail_async()
                    )
                    == "value"
                )
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_checks": [
                    LagAwareHealthCheck(
                        verify_tls=False,
                        auth_basic=lag_aware_auth_basic(),
                        lag_aware_tolerance=10000,
                        health_check_timeout=LAG_AWARE_HEALTH_CHECK_TIMEOUT,
                    )
                ],
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_checks": [
                    LagAwareHealthCheck(
                        verify_tls=False,
                        auth_basic=lag_aware_auth_basic(),
                        lag_aware_tolerance=10000,
                        health_check_timeout=LAG_AWARE_HEALTH_CHECK_TIMEOUT,
                    )
                ],
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(200)
    async def test_multi_db_client_uses_lag_aware_health_check(
        self, r_multi_db, fault_injector_client
    ):
        require_lag_aware_credentials()

        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(
                trigger_network_failure_action(
                    fault_injector_client, endpoint_config, event
                )
            )

            await retry.call_with_retry(
                lambda: r_multi_db.set("key", "value"), lambda _: dummy_fail_async()
            )

            # Execute commands before network failure
            while not event.is_set():
                assert (
                    await retry.call_with_retry(
                        lambda: r_multi_db.get("key"), lambda _: dummy_fail_async()
                    )
                    == "value"
                )
                await asyncio.sleep(0.5)

            # Execute commands after network failure
            deadline = monotonic() + FAILOVER_TIMEOUT
            while not listener.is_changed_flag:
                assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
                assert (
                    await retry.call_with_retry(
                        lambda: r_multi_db.get("key"), lambda _: dummy_fail_async()
                    )
                    == "value"
                )
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(200)
    async def test_context_manager_pipeline_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        async def callback():
            async with r_multi_db.pipeline() as pipe:
                pipe.set("{hash}key1", "value1")
                pipe.set("{hash}key2", "value2")
                pipe.set("{hash}key3", "value3")
                pipe.get("{hash}key1")
                pipe.get("{hash}key2")
                pipe.get("{hash}key3")
                assert await pipe.execute() == [
                    True,
                    True,
                    True,
                    "value1",
                    "value2",
                    "value3",
                ]

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(
                trigger_network_failure_action(
                    fault_injector_client, endpoint_config, event
                )
            )

            # Execute pipeline before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: callback(), lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

            # Execute pipeline until database failover
            deadline = monotonic() + FAILOVER_TIMEOUT
            while not listener.is_changed_flag:
                assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
                await retry.call_with_retry(
                    lambda: callback(), lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(200)
    async def test_chaining_pipeline_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        async def callback():
            pipe = r_multi_db.pipeline()
            pipe.set("{hash}key1", "value1")
            pipe.set("{hash}key2", "value2")
            pipe.set("{hash}key3", "value3")
            pipe.get("{hash}key1")
            pipe.get("{hash}key2")
            pipe.get("{hash}key3")
            assert await pipe.execute() == [
                True,
                True,
                True,
                "value1",
                "value2",
                "value3",
            ]

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(
                trigger_network_failure_action(
                    fault_injector_client, endpoint_config, event
                )
            )

            # Execute pipeline before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: callback(), lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

            # Execute pipeline until database failover
            deadline = monotonic() + FAILOVER_TIMEOUT
            while not listener.is_changed_flag:
                assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
                await retry.call_with_retry(
                    lambda: callback(), lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "client_class": Redis,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
            {
                "client_class": RedisCluster,
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True,
    )
    @pytest.mark.timeout(200)
    async def test_transaction_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        client, listener, endpoint_config = r_multi_db

        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        async def callback(pipe: Pipeline):
            pipe.set("{hash}key1", "value1")
            pipe.set("{hash}key2", "value2")
            pipe.set("{hash}key3", "value3")
            pipe.get("{hash}key1")
            pipe.get("{hash}key2")
            pipe.get("{hash}key3")

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(
                trigger_network_failure_action(
                    fault_injector_client, endpoint_config, event
                )
            )

            # Execute transaction before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: r_multi_db.transaction(callback),
                    lambda _: dummy_fail_async(),
                )
                await asyncio.sleep(0.5)

            # Execute transaction until database failover
            deadline = monotonic() + FAILOVER_TIMEOUT
            while not listener.is_changed_flag:
                assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
                assert await retry.call_with_retry(
                    lambda: r_multi_db.transaction(callback),
                    lambda _: dummy_fail_async(),
                ) == [True, True, True, "value1", "value2", "value3"]
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {
                "min_num_failures": 2,
                "health_check_interval": FAILOVER_HEALTH_CHECK_INTERVAL,
            }
        ],
        indirect=True,
    )
    @pytest.mark.timeout(200)
    async def test_pubsub_failover_to_another_db(
        self, r_multi_db, fault_injector_client
    ):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY),
        )

        data = json.dumps({"message": "test"})
        messages_count = 0

        async def handler(message):
            nonlocal messages_count
            messages_count += 1

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(
                trigger_network_failure_action(
                    fault_injector_client, endpoint_config, event
                )
            )

            pubsub = await r_multi_db.pubsub()

            # Assign a handler and run in a separate thread.
            await retry.call_with_retry(
                lambda: pubsub.subscribe(**{"test-channel": handler}),
                lambda _: dummy_fail_async(),
            )
            task = asyncio.create_task(pubsub.run(poll_timeout=0.1))

            # Execute publish before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: r_multi_db.publish("test-channel", data),
                    lambda _: dummy_fail_async(),
                )
                await asyncio.sleep(0.5)

            # Execute publish until database failover
            deadline = monotonic() + FAILOVER_TIMEOUT
            while not listener.is_changed_flag:
                assert monotonic() < deadline, FAILOVER_TIMEOUT_MESSAGE
                await retry.call_with_retry(
                    lambda: r_multi_db.publish("test-channel", data),
                    lambda _: dummy_fail_async(),
                )
                await asyncio.sleep(0.5)

            # After db changed still generates some traffic.
            for _ in range(5):
                await retry.call_with_retry(
                    lambda: r_multi_db.publish("test-channel", data),
                    lambda _: dummy_fail_async(),
                )

            # A timeout to ensure that an async handler will handle all previous messages.
            await asyncio.sleep(0.1)
            task.cancel()
            assert messages_count >= 2
