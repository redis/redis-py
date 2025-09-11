import asyncio
import json
import logging
import os
from time import sleep

import pytest

from redis.asyncio import RedisCluster
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.multidb.client import MultiDBClient
from redis.asyncio.multidb.healthcheck import LagAwareHealthCheck
from tests.test_scenario.fault_injector_client import ActionRequest, ActionType

logger = logging.getLogger(__name__)

async def trigger_network_failure_action(fault_injector_client, config, event: asyncio.Event = None):
    action_request = ActionRequest(
        action_type=ActionType.NETWORK_FAILURE,
        parameters={"bdb_id": config['bdb_id'], "delay": 2, "cluster_index": 0}
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

    def teardown_method(self, method):
        # Timeout so the cluster could recover from network failure.
        sleep(6)

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
    @pytest.mark.timeout(50)
    async def test_multi_db_client_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client_config, listener, endpoint_config = r_multi_db

        async with MultiDBClient(client_config) as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            await r_multi_db.set('key', 'value')

            # Execute commands before network failure
            while not event.is_set():
                assert await r_multi_db.get('key') == 'value'
                await asyncio.sleep(0.5)

            # Execute commands until database failover
            while not listener.is_changed_flag:
                assert await r_multi_db.get('key') == 'value'
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [
            {"client_class": Redis, "failure_threshold": 2, "health_checks":
                [LagAwareHealthCheck(verify_tls=False, auth_basic=(os.getenv('ENV0_USERNAME'),os.getenv('ENV0_PASSWORD')))]
            },
            {"client_class": RedisCluster, "failure_threshold": 2, "health_checks":
                [LagAwareHealthCheck(verify_tls=False, auth_basic=(os.getenv('ENV0_USERNAME'),os.getenv('ENV0_PASSWORD')))]
            },
        ],
        ids=["standalone", "cluster"],
        indirect=True
    )
    @pytest.mark.timeout(50)
    async def test_multi_db_client_uses_lag_aware_health_check(self, r_multi_db, fault_injector_client):
        client_config, listener, endpoint_config = r_multi_db

        async with MultiDBClient(client_config) as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            await r_multi_db.set('key', 'value')

            # Execute commands before network failure
            while not event.is_set():
                assert await r_multi_db.get('key') == 'value'
                await asyncio.sleep(0.5)

            # Execute commands after network failure
            while not listener.is_changed_flag:
                assert await r_multi_db.get('key') == 'value'
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
    @pytest.mark.timeout(50)
    async def test_context_manager_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client_config, listener, endpoint_config = r_multi_db

        async with MultiDBClient(client_config) as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            # Execute pipeline before network failure
            while not event.is_set():
               async with r_multi_db.pipeline() as pipe:
                    pipe.set('{hash}key1', 'value1')
                    pipe.set('{hash}key2', 'value2')
                    pipe.set('{hash}key3', 'value3')
                    pipe.get('{hash}key1')
                    pipe.get('{hash}key2')
                    pipe.get('{hash}key3')
                    assert await pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
               await asyncio.sleep(0.5)

            # Execute pipeline until database failover
            for _ in range(5):
                async with r_multi_db.pipeline() as pipe:
                    pipe.set('{hash}key1', 'value1')
                    pipe.set('{hash}key2', 'value2')
                    pipe.set('{hash}key3', 'value3')
                    pipe.get('{hash}key1')
                    pipe.get('{hash}key2')
                    pipe.get('{hash}key3')
                    assert await pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
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
    @pytest.mark.timeout(50)
    async def test_chaining_pipeline_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client_config, listener, endpoint_config = r_multi_db

        async with MultiDBClient(client_config) as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            # Execute pipeline before network failure
            while not event.is_set():
                pipe = r_multi_db.pipeline()
                pipe.set('{hash}key1', 'value1')
                pipe.set('{hash}key2', 'value2')
                pipe.set('{hash}key3', 'value3')
                pipe.get('{hash}key1')
                pipe.get('{hash}key2')
                pipe.get('{hash}key3')
                assert await pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
            await asyncio.sleep(0.5)

            # Execute pipeline until database failover
            for _ in range(5):
                pipe = r_multi_db.pipeline()
                pipe.set('{hash}key1', 'value1')
                pipe.set('{hash}key2', 'value2')
                pipe.set('{hash}key3', 'value3')
                pipe.get('{hash}key1')
                pipe.get('{hash}key2')
                pipe.get('{hash}key3')
                assert await pipe.execute() == [True, True, True, 'value1', 'value2', 'value3']
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
    @pytest.mark.timeout(50)
    async def test_transaction_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client_config, listener, endpoint_config = r_multi_db

        async def callback(pipe: Pipeline):
            pipe.set('{hash}key1', 'value1')
            pipe.set('{hash}key2', 'value2')
            pipe.set('{hash}key3', 'value3')
            pipe.get('{hash}key1')
            pipe.get('{hash}key2')
            pipe.get('{hash}key3')

        async with MultiDBClient(client_config) as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            # Execute transaction before network failure
            while not event.is_set():
                await r_multi_db.transaction(callback)
                await asyncio.sleep(0.5)

            # Execute transaction until database failover
            while not listener.is_changed_flag:
                await r_multi_db.transaction(callback) == [True, True, True, 'value1', 'value2', 'value3']
                await asyncio.sleep(0.5)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "r_multi_db",
        [{"failure_threshold": 2}],
        indirect=True
    )
    @pytest.mark.timeout(50)
    async def test_pubsub_failover_to_another_db(self, r_multi_db, fault_injector_client):
        client_config, listener, endpoint_config = r_multi_db

        data = json.dumps({'message': 'test'})
        messages_count = 0

        async def handler(message):
            nonlocal messages_count
            messages_count += 1

        async with MultiDBClient(client_config) as r_multi_db:
            event = asyncio.Event()
            asyncio.create_task(trigger_network_failure_action(fault_injector_client, endpoint_config, event))

            pubsub = await r_multi_db.pubsub()

            # Assign a handler and run in a separate thread.
            await pubsub.subscribe(**{'test-channel': handler})
            task = asyncio.create_task(pubsub.run(poll_timeout=0.1))

            # Execute publish before network failure
            while not event.is_set():
                await r_multi_db.publish('test-channel', data)
                await asyncio.sleep(0.5)

            # Execute publish until database failover
            while not listener.is_changed_flag:
                await r_multi_db.publish('test-channel', data)
                await asyncio.sleep(0.5)

            # After db changed still generates some traffic.
            for _ in range(5):
                await r_multi_db.publish('test-channel', data)

            # A timeout to ensure that an async handler will handle all previous messages.
            await asyncio.sleep(0.1)
            task.cancel()
            await pubsub.unsubscribe('test-channel') is True
            assert messages_count >= 5