import multiprocessing
import time
from typing import List

import pytest
from redis import BusyLoadingError, Redis
from redis.backoff import ExponentialBackoff
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from redis.retry import Retry

from ..conftest import _get_client
from . import Endpoint, get_endpoint
from .fake_app import FakeApp, FakeSubscriber
from .fault_injection_client import FaultInjectionClient


@pytest.fixture
def endpoint_name():
    return "re-standalone"


@pytest.fixture
def endpoint(request: pytest.FixtureRequest, endpoint_name: str):
    try:
        return get_endpoint(request, endpoint_name)
    except FileNotFoundError as e:
        pytest.skip(
            f"Skipping scenario test because endpoints file is missing: {str(e)}"
        )


@pytest.fixture
def clients(request: pytest.FixtureRequest, endpoint: Endpoint):
    # Use Recommended settings
    retry = Retry(ExponentialBackoff(base=1), 3)

    clients = []

    for _ in range(2):
        r = _get_client(
            Redis,
            request,
            decode_responses=True,
            from_url=endpoint.url,
            retry=retry,
            retry_on_error=[
                BusyLoadingError,
                RedisConnectionError,
                RedisTimeoutError,
                # FIXME: This is a workaround for a bug in redis-py
                # https://github.com/redis/redis-py/issues/3203
                ConnectionError,
                TimeoutError,
            ],
        )
        r.flushdb()
        clients.append(r)
    return clients


@pytest.fixture
def fault_injection_client(request: pytest.FixtureRequest):
    return FaultInjectionClient()


@pytest.mark.parametrize("action", ("dmc_restart", "network_failure"))
def test_connection_interruptions(
    clients: List[Redis],
    endpoint: Endpoint,
    fault_injection_client: FaultInjectionClient,
    action: str,
):
    client = clients.pop()
    app = FakeApp(client, lambda c: c.set("foo", "bar"))

    stop_app, thread = app.run()

    triggered_action = fault_injection_client.trigger_action(
        action, {"bdb_id": endpoint.bdb_id}
    )

    triggered_action.wait_until_complete()

    stop_app.set()
    thread.join()

    if triggered_action.status == "failed":
        pytest.fail(f"Action failed: {triggered_action.data['error']}")

    assert app.disconnects > 0, "Client did not disconnect"


@pytest.mark.parametrize("action", ("dmc_restart", "network_failure"))
def test_pubsub_with_connection_interruptions(
    clients: List[Redis],
    endpoint: Endpoint,
    fault_injection_client: FaultInjectionClient,
    action: str,
):
    channel = "test"

    # Subscriber is executed in a separate process to ensure it reacts
    # to the disconnection at the same time as the publisher
    with multiprocessing.Manager() as manager:
        received_messages = manager.list()

        def read_message(message):
            nonlocal received_messages
            if message and message["type"] == "message":
                received_messages.append(message["data"])

        subscriber_client = clients.pop()
        subscriber = FakeSubscriber(subscriber_client, read_message)
        stop_subscriber, subscriber_started, subscriber_t = subscriber.run(channel)

        # Allow subscriber subscribe to the channel
        subscriber_started.wait(timeout=5)

        messages_sent = 0

        def publish_message(c):
            nonlocal messages_sent, channel
            messages_sent += 1
            c.publish(channel, messages_sent)

        publisher_client = clients.pop()
        publisher = FakeApp(publisher_client, publish_message)
        stop_publisher, publisher_t = publisher.run()

        triggered_action = fault_injection_client.trigger_action(
            action, {"bdb_id": endpoint.bdb_id}
        )

        triggered_action.wait_until_complete()
        last_message_sent_after_trigger = messages_sent

        time.sleep(3)  # Wait for the publisher to send more messages

        stop_publisher.set()
        publisher_t.join()

        stop_subscriber.set()
        subscriber_t.join()

        assert publisher.disconnects > 0
        assert subscriber.disconnects.value > 0

        if triggered_action.status == "failed":
            pytest.fail(f"Action failed: {triggered_action.data['error']}")

        assert (
            last_message_sent_after_trigger < messages_sent
        ), "No messages were sent after the failure"
        assert (
            int(received_messages[-1]) == messages_sent
        ), "Not all messages were received"
