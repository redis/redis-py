import asyncio

import pytest
from redis.asyncio import BusyLoadingError, Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError
from tests.scenario import Endpoint, get_endpoint

from .fake_app import AsyncFakeApp, AsyncFakeSubscriber
from .fault_injection_client import AsyncFaultInjectionClient


@pytest.fixture
async def endpoint_name():
    return "re-standalone"


@pytest.fixture
async def endpoint(request: pytest.FixtureRequest, endpoint_name: str):
    try:
        return get_endpoint(request, endpoint_name)
    except FileNotFoundError as e:
        pytest.skip(
            f"Skipping scenario test because endpoints file is missing: {str(e)}"
        )


@pytest.fixture
async def clients(
    request: pytest.FixtureRequest, endpoint: Endpoint, create_redis: callable
):
    # Use Recommended settings
    retry = Retry(ExponentialBackoff(base=1), 5)

    clients = []

    for _ in range(2):
        client = await create_redis(
            endpoint.url,
            decode_responses=True,
            retry=retry,
            retry_on_error=[
                BusyLoadingError,
                RedisConnectionError,
                TimeoutError,
                # FIXME: This is a workaround for a bug in redis-py
                # https://github.com/redis/redis-py/issues/3203
                ConnectionError,
                OSError,
            ],
            retry_on_timeout=True,
        )
        await client.flushdb()
        clients.append(client)

    return clients


@pytest.fixture
async def fault_injection_client(request: pytest.FixtureRequest):
    return AsyncFaultInjectionClient()


@pytest.mark.parametrize("action", ("dmc_restart", "network_failure"))
async def test_connection_interruptions(
    clients: list[Redis],
    endpoint: Endpoint,
    fault_injection_client: AsyncFaultInjectionClient,
    action: str,
):
    client = clients.pop()
    app = AsyncFakeApp(client, lambda c: c.set("foo", "bar"))

    stop_app, task = await app.run()

    triggered_action = await fault_injection_client.trigger_action(
        action, {"bdb_id": endpoint.bdb_id}
    )

    await triggered_action.wait_until_complete()

    stop_app.set()
    await task

    if triggered_action.status == "failed":
        pytest.fail(f"Action failed: {triggered_action.data['error']}")

    assert app.disconnects > 0


@pytest.mark.parametrize("action", ("dmc_restart",))  # "network_failure"))
async def test_pubsub_with_connection_interruptions(
    clients: list[Redis],
    endpoint: Endpoint,
    fault_injection_client: AsyncFaultInjectionClient,
    action: str,
):
    channel = "test"

    received_messages = []

    async def read_message(message):
        nonlocal received_messages
        if message and message["type"] == "message":
            received_messages.append(message["data"])

    messages_sent = 0

    async def publish_message(c):
        nonlocal messages_sent, channel
        messages_sent += 1
        await c.publish(channel, messages_sent)

    subscriber_client = clients.pop()
    publisher_client = clients.pop()

    subscriber = AsyncFakeSubscriber(subscriber_client, read_message)
    stop_subscriber, subscriber_t = await subscriber.run(channel)

    publisher = AsyncFakeApp(publisher_client, publish_message)
    stop_publisher, publisher_t = await publisher.run()

    triggered_action = await fault_injection_client.trigger_action(
        action, {"bdb_id": endpoint.bdb_id}
    )

    await triggered_action.wait_until_complete()
    last_message_sent_after_trigger = messages_sent

    if triggered_action.status == "failed":
        pytest.fail(f"Action failed: {triggered_action.data['error']}")

    await asyncio.sleep(3)

    stop_publisher.set()
    await publisher_t

    stop_subscriber.set()
    await subscriber_t

    assert publisher.disconnects > 0
    assert subscriber.disconnects > 0

    assert (
        last_message_sent_after_trigger < messages_sent
    ), "No messages were sent after the failure"
    assert int(received_messages[-1]) == messages_sent, "Not all messages were received"
