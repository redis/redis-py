import asyncio
import json
import logging
import os
from time import sleep

import pytest

from redis.asyncio import RedisCluster
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.failover import DEFAULT_FAILOVER_ATTEMPTS, DEFAULT_FAILOVER_DELAY
from redis.asyncio.multidb.healthcheck import LagAwareHealthCheck
from redis.asyncio.retry import Retry
from redis.backoff import ConstantBackoff
from redis.multidb.exception import TemporaryUnavailableException
from redis.utils import dummy_fail_async
from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)

async def trigger_network_failure_action(fault_injector_client, config, event: asyncio.Event = None):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config['bdb_id'], "delay": 3, "cluster_index": 0}
    )

    result = fault_injector_client.trigger_action(action_request)
    status_result = fault_injector_client.get_action_status(result['action_id'])

    while status_result['status'] != "success":
        await asyncio.sleep(0.1)
        status_result = fault_injector_client.get_action_status(result['action_id'])
        logger.info(f"Waiting for action to complete. Status: {status_result['status']}")

    if event:
        event.set()

    logger.info(f"Action completed. Status: {status_result['status']}")

class TestActiveActive:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(200)
    async def test_multi_db_client_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client, listener, endpoint_config = r_multi_db

        # Handle unavailable databases from previous test.
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY)
        )

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            await retry.call_with_retry(
                lambda : r_multi_db.set('key', 'value'),
                lambda _: dummy_fail_async()
            )

            # Execute commands before network failure
            while not event.is_set():
                assert await retry.call_with_retry(
                    lambda: r_multi_db.get('key') ,
                    lambda _: dummy_fail_async()
                ) == 'value'
                await asyncio.sleep(0.5)

            # Execute commands until database failover
            while not listener.is_changed_flag:
                assert await retry.call_with_retry(
                    lambda: r_multi_db.get('key'),
                    lambda _: dummy_fail_async()
                ) == 'value'
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2, "health_checks":
                [
                    LagAwareHealthCheck(
                        verify_tls=False,
                        auth_basic=(os.getenv('ENV0_USERNAME'),os.getenv('ENV0_PASSWORD')),
                    )
                ],
             "health_check_interval": 20,
            },
            {"client_class": RedisCluster, "failure_threshold": 2, "health_checks":
                [
                    LagAwareHealthCheck(
                        verify_tls=False,
                        auth_basic=(os.getenv('ENV0_USERNAME'), os.getenv('ENV0_PASSWORD')),
                    )
                ],
             "health_check_interval": 20,
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(200)
    async def test_multi_db_client_uses_lag_aware_health_check(self, r_multi_db, fault_injector_client):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY)
        )

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            await retry.call_with_retry(
                lambda: r_multi_db.set('key', 'value'),
                lambda _: dummy_fail_async()
            )

            # Execute commands before network failure
            while not event.is_set():
                assert await retry.call_with_retry(
                    lambda: r_multi_db.get('key'),
                    lambda _: dummy_fail_async()
                ) == 'value'
                await asyncio.sleep(0.5)

            # Execute commands after network failure
            while not listener.is_changed_flag:
                assert await retry.call_with_retry(
                    lambda: r_multi_db.get('key'),
                    lambda _: dummy_fail_async()
                ) == 'value'
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(200)
    async def test_context_manager_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY)
        )

        async def callback():
            async with r_multi_db.pipeline() as pipe:
                pipe.set('{hash}key1', 'value1')
                pipe.set('{hash}key2', 'value2')
                pipe.set('{hash}key3', 'value3')
                pipe.get('{hash}key1')
                pipe.get('{hash}key2')
                pipe.get('{hash}key3')
                assert await pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            # Execute pipeline before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: callback(),
                    lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

        # Execute commands until database failover
        while not listener.is_changed_flag:
            await retry.call_with_retry(
                lambda: callback(),
                lambda _: dummy_fail_async()
            )
            await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(200)
    async def test_chaining_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY)
        )

        async def callback():
            pipe = r_multi_db.pipeline()
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')
            assert await pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            # Execute pipeline before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: callback(),
                    lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

        # Execute pipeline until database failover
        while not listener.is_changed_flag:
            await retry.call_with_retry(
                lambda: callback(),
                lambda _: dummy_fail_async()
            )
            await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2},
            {"client_class": RedisCluster, "failure_threshold": 2},
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(200)
    async def test_transaction_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client, listener, endpoint_config = r_multi_db

        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY)
        )

        async def callback(pipe: Pipeline):
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            # Execute transaction before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: r_multi_db.transaction(callback),
                    lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

            # Execute transaction until database failover
            while not listener.is_changed_flag:
                assert await retry.call_with_retry(
                    lambda: r_multi_db.transaction(callback),
                    lambda _: dummy_fail_async()
                ) == [True, True, True, 'value1', 'value2', 'value3']
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [{"failure_threshold": 2}],
        indirect=True
    )
    @pytest.mark.timeout(200)
    async def test_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client, listener, endpoint_config = r_multi_db
        retry = Retry(
            supported_errors=(TemporaryUnavailableException,),
            retries=DEFAULT_FAILOVER_ATTEMPTS,
            backoff=ConstantBackoff(backoff=DEFAULT_FAILOVER_DELAY)
        )

        data = json.dumps({'message': 'test'})
        messages_count = 0

        async def handler(message):
            nonlocal messages_count
            messages_count += 1

        async with client as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            pubsub = await r_multi_db.pubsub()

            # Assign a handler and run in a separate thread.
            await retry.call_with_retry(
                lambda: pubsub.subscribe(**{'test-channel': handler}),
                lambda _: dummy_fail_async()
            )
            task = asyncio.create_task(pubsub.run(poll_timeout=0.1))

            # Execute publish before network failure
            while not event.is_set():
                await retry.call_with_retry(
                    lambda: r_multi_db.publish('test-channel', data),
                    lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

            # Execute publish until database failover
            while not listener.is_changed_flag:
                await retry.call_with_retry(
                    lambda: r_multi_db.publish('test-channel', data),
                    lambda _: dummy_fail_async()
                )
                await asyncio.sleep(0.5)

            # After db changed still generates some traffic.
            for _ in range(5):
                await retry.call_with_retry(
                    lambda: r_multi_db.publish('test-channel', data),
                    lambda _: dummy_fail_async()
                )

            # A timeout to ensure that an async handler will handle all previous messages.
            await asyncio.sleep(0.1)
            task.cancel()
            await pubsub.unsubscribe('test-channel') is True
            assert messages_count >= 2