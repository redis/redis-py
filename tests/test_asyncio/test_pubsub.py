import asyncio
import sys
from typing import Optional

import pytest

if sys.version_info[0:2] == (3, 6):
    import pytest as pytest_asyncio
else:
    import pytest_asyncio

import redis.asyncio as redis
from redis.exceptions import ConnectionError
from redis.typing import EncodableT
from tests.conftest import skip_if_server_version_lt

from .compat import mock

pytestmark = pytest.mark.asyncio(forbid_global_loop=True)


async def wait_for_message(pubsub, timeout=0.1, ignore_subscribe_messages=False):
    now = asyncio.get_event_loop().time()
    timeout = now + timeout
    while now < timeout:
        message = await pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages
        )
        if message is not None:
            return message
        await asyncio.sleep(0.01)
        now = asyncio.get_event_loop().time()
    return None


def make_message(
    type, channel: Optional[str], data: EncodableT, pattern: Optional[str] = None
):
    return {
        "type": type,
        "pattern": pattern and pattern.encode("utf-8") or None,
        "channel": channel and channel.encode("utf-8") or None,
        "data": data.encode("utf-8") if isinstance(data, str) else data,
    }


def make_subscribe_test_data(pubsub, type):
    if type == "channel":
        return {
            "p": pubsub,
            "sub_type": "subscribe",
            "unsub_type": "unsubscribe",
            "sub_func": pubsub.subscribe,
            "unsub_func": pubsub.unsubscribe,
            "keys": ["foo", "bar", "uni" + chr(4456) + "code"],
        }
    elif type == "pattern":
        return {
            "p": pubsub,
            "sub_type": "psubscribe",
            "unsub_type": "punsubscribe",
            "sub_func": pubsub.psubscribe,
            "unsub_func": pubsub.punsubscribe,
            "keys": ["f*", "b*", "uni" + chr(4456) + "*"],
        }
    assert False, f"invalid subscribe type: {type}"


@pytest.mark.onlynoncluster
class TestPubSubSubscribeUnsubscribe:
    async def _test_subscribe_unsubscribe(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert await sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert await wait_for_message(p) == make_message(sub_type, key, i + 1)

        for key in keys:
            assert await unsub_func(key) is None

        # should be a message for each channel/pattern we just unsubscribed
        # from
        for i, key in enumerate(keys):
            i = len(keys) - 1 - i
            assert await wait_for_message(p) == make_message(unsub_type, key, i)

    async def test_channel_subscribe_unsubscribe(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def test_pattern_subscribe_unsubscribe(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        await self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlynoncluster
    async def _test_resubscribe_on_reconnection(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):

        for key in keys:
            assert await sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert await wait_for_message(p) == make_message(sub_type, key, i + 1)

        # manually disconnect
        await p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        # note, we may not re-subscribe to channels in exactly the same order
        # so we have to do some extra checks to make sure we got them all
        messages = []
        for i in range(len(keys)):
            messages.append(await wait_for_message(p))

        unique_channels = set()
        assert len(messages) == len(keys)
        for i, message in enumerate(messages):
            assert message["type"] == sub_type
            assert message["data"] == i + 1
            assert isinstance(message["channel"], bytes)
            channel = message["channel"].decode("utf-8")
            unique_channels.add(channel)

        assert len(unique_channels) == len(keys)
        for channel in unique_channels:
            assert channel in keys

    async def test_resubscribe_to_channels_on_reconnection(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_resubscribe_to_patterns_on_reconnection(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def _test_subscribed_property(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):

        assert p.subscribed is False
        await sub_func(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all channels
        await unsub_func()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        await sub_func(keys[0])
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)

        # unsubscribe again
        await unsub_func()
        assert p.subscribed is True
        # subscribe to another channel before reading the unsubscribe response
        await sub_func(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert await wait_for_message(p) == make_message(sub_type, keys[1], 1)
        await unsub_func()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(unsub_type, keys[1], 0)
        # now we're finally unsubscribed
        assert p.subscribed is False

    async def test_subscribe_property_with_channels(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    async def test_subscribe_property_with_patterns(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        await self._test_subscribed_property(**kwargs)

    async def test_ignore_all_subscribe_messages(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)

        checks = (
            (p.subscribe, "foo"),
            (p.unsubscribe, "foo"),
            (p.psubscribe, "f*"),
            (p.punsubscribe, "f*"),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert await func(channel) is None
            assert p.subscribed is True
            assert await wait_for_message(p) is None
        assert p.subscribed is False

    async def test_ignore_individual_subscribe_messages(self, r: redis.Redis):
        p = r.pubsub()

        checks = (
            (p.subscribe, "foo"),
            (p.unsubscribe, "foo"),
            (p.psubscribe, "f*"),
            (p.punsubscribe, "f*"),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert await func(channel) is None
            assert p.subscribed is True
            message = await wait_for_message(p, ignore_subscribe_messages=True)
            assert message is None
        assert p.subscribed is False

    async def test_sub_unsub_resub_channels(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        await self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    async def test_sub_unsub_resub_patterns(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        await self._test_sub_unsub_resub(**kwargs)

    async def _test_sub_unsub_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        await sub_func(key)
        await unsub_func(key)
        await sub_func(key)
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert await wait_for_message(p) == make_message(unsub_type, key, 0)
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True

    async def test_sub_unsub_all_resub_channels(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        await self._test_sub_unsub_all_resub(**kwargs)

    async def test_sub_unsub_all_resub_patterns(self, r: redis.Redis):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        await self._test_sub_unsub_all_resub(**kwargs)

    async def _test_sub_unsub_all_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        await sub_func(key)
        await unsub_func()
        await sub_func(key)
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert await wait_for_message(p) == make_message(unsub_type, key, 0)
        assert await wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True


@pytest.mark.onlynoncluster
class TestPubSubMessages:
    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    async def async_message_handler(self, message):
        self.async_message = message

    async def test_published_message_to_channel(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await r.publish("foo", "test message") == 1

        message = await wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("message", "foo", "test message")

    async def test_published_message_to_pattern(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe("foo")
        await p.psubscribe("f*")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await wait_for_message(p) == make_message("psubscribe", "f*", 2)
        # 1 to pattern, 1 to channel
        assert await r.publish("foo", "test message") == 2

        message1 = await wait_for_message(p)
        message2 = await wait_for_message(p)
        assert isinstance(message1, dict)
        assert isinstance(message2, dict)

        expected = [
            make_message("message", "foo", "test message"),
            make_message("pmessage", "foo", "test message", pattern="f*"),
        ]

        assert message1 in expected
        assert message2 in expected
        assert message1 != message2

    async def test_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.message_handler)
        assert await wait_for_message(p) is None
        assert await r.publish("foo", "test message") == 1
        assert await wait_for_message(p) is None
        assert self.message == make_message("message", "foo", "test message")

    async def test_channel_async_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.async_message_handler)
        assert await wait_for_message(p) is None
        assert await r.publish("foo", "test message") == 1
        assert await wait_for_message(p) is None
        assert self.async_message == make_message("message", "foo", "test message")

    async def test_channel_sync_async_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.message_handler)
        await p.subscribe(bar=self.async_message_handler)
        assert await wait_for_message(p) is None
        assert await r.publish("foo", "test message") == 1
        assert await r.publish("bar", "test message 2") == 1
        assert await wait_for_message(p) is None
        assert self.message == make_message("message", "foo", "test message")
        assert self.async_message == make_message("message", "bar", "test message 2")

    @pytest.mark.onlynoncluster
    async def test_pattern_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.psubscribe(**{"f*": self.message_handler})
        assert await wait_for_message(p) is None
        assert await r.publish("foo", "test message") == 1
        assert await wait_for_message(p) is None
        assert self.message == make_message(
            "pmessage", "foo", "test message", pattern="f*"
        )

    async def test_unicode_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        await p.subscribe(**channels)
        assert await wait_for_message(p) is None
        assert await r.publish(channel, "test message") == 1
        assert await wait_for_message(p) is None
        assert self.message == make_message("message", channel, "test message")

    @pytest.mark.onlynoncluster
    # see: https://redis-py-cluster.readthedocs.io/en/stable/pubsub.html
    # #known-limitations-with-pubsub
    async def test_unicode_pattern_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = "uni" + chr(4456) + "*"
        channel = "uni" + chr(4456) + "code"
        await p.psubscribe(**{pattern: self.message_handler})
        assert await wait_for_message(p) is None
        assert await r.publish(channel, "test message") == 1
        assert await wait_for_message(p) is None
        assert self.message == make_message(
            "pmessage", channel, "test message", pattern=pattern
        )

    async def test_get_message_without_subscribe(self, r: redis.Redis):
        p = r.pubsub()
        with pytest.raises(RuntimeError) as info:
            await p.get_message()
        expect = (
            "connection not set: " "did you forget to call subscribe() or psubscribe()?"
        )
        assert expect in info.exconly()


@pytest.mark.onlynoncluster
class TestPubSubAutoDecoding:
    """These tests only validate that we get unicode values back"""

    channel = "uni" + chr(4456) + "code"
    pattern = "uni" + chr(4456) + "*"
    data = "abc" + chr(4458) + "123"

    def make_message(self, type, channel, data, pattern=None):
        return {"type": type, "channel": channel, "pattern": pattern, "data": data}

    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    @pytest_asyncio.fixture()
    async def r(self, create_redis):
        return await create_redis(
            decode_responses=True,
        )

    async def test_channel_subscribe_unsubscribe(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "subscribe", self.channel, 1
        )

        await p.unsubscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "unsubscribe", self.channel, 0
        )

    async def test_pattern_subscribe_unsubscribe(self, r: redis.Redis):
        p = r.pubsub()
        await p.psubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "psubscribe", self.pattern, 1
        )

        await p.punsubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "punsubscribe", self.pattern, 0
        )

    async def test_channel_publish(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "subscribe", self.channel, 1
        )
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message(
            "message", self.channel, self.data
        )

    @pytest.mark.onlynoncluster
    async def test_pattern_publish(self, r: redis.Redis):
        p = r.pubsub()
        await p.psubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "psubscribe", self.pattern, 1
        )
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message(
            "pmessage", self.channel, self.data, pattern=self.pattern
        )

    async def test_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(**{self.channel: self.message_handler})
        assert await wait_for_message(p) is None
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) is None
        assert self.message == self.make_message("message", self.channel, self.data)

        # test that we reconnected to the correct channel
        self.message = None
        await p.connection.disconnect()
        assert await wait_for_message(p) is None  # should reconnect
        new_data = self.data + "new data"
        await r.publish(self.channel, new_data)
        assert await wait_for_message(p) is None
        assert self.message == self.make_message("message", self.channel, new_data)

    async def test_pattern_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.psubscribe(**{self.pattern: self.message_handler})
        assert await wait_for_message(p) is None
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) is None
        assert self.message == self.make_message(
            "pmessage", self.channel, self.data, pattern=self.pattern
        )

        # test that we reconnected to the correct pattern
        self.message = None
        await p.connection.disconnect()
        assert await wait_for_message(p) is None  # should reconnect
        new_data = self.data + "new data"
        await r.publish(self.channel, new_data)
        assert await wait_for_message(p) is None
        assert self.message == self.make_message(
            "pmessage", self.channel, new_data, pattern=self.pattern
        )

    async def test_context_manager(self, r: redis.Redis):
        async with r.pubsub() as pubsub:
            await pubsub.subscribe("foo")
            assert pubsub.connection is not None

        assert pubsub.connection is None
        assert pubsub.channels == {}
        assert pubsub.patterns == {}


@pytest.mark.onlynoncluster
class TestPubSubRedisDown:
    async def test_channel_subscribe(self, r: redis.Redis):
        r = redis.Redis(host="localhost", port=6390)
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            await p.subscribe("foo")


@pytest.mark.onlynoncluster
class TestPubSubSubcommands:
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_channels(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert (await wait_for_message(p))["type"] == "subscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all([channel in await r.pubsub_channels() for channel in expected])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_numsub(self, r: redis.Redis):
        p1 = r.pubsub()
        await p1.subscribe("foo", "bar", "baz")
        for i in range(3):
            assert (await wait_for_message(p1))["type"] == "subscribe"
        p2 = r.pubsub()
        await p2.subscribe("bar", "baz")
        for i in range(2):
            assert (await wait_for_message(p2))["type"] == "subscribe"
        p3 = r.pubsub()
        await p3.subscribe("baz")
        assert (await wait_for_message(p3))["type"] == "subscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert await r.pubsub_numsub("foo", "bar", "baz") == channels

    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_numpat(self, r: redis.Redis):
        p = r.pubsub()
        await p.psubscribe("*oo", "*ar", "b*z")
        for i in range(3):
            assert (await wait_for_message(p))["type"] == "psubscribe"
        assert await r.pubsub_numpat() == 3


@pytest.mark.onlynoncluster
class TestPubSubPings:
    @skip_if_server_version_lt("3.0.0")
    async def test_send_pubsub_ping(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        await p.ping()
        assert await wait_for_message(p) == make_message(
            type="pong", channel=None, data="", pattern=None
        )

    @skip_if_server_version_lt("3.0.0")
    async def test_send_pubsub_ping_message(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        await p.ping(message="hello world")
        assert await wait_for_message(p) == make_message(
            type="pong", channel=None, data="hello world", pattern=None
        )


@pytest.mark.onlynoncluster
class TestPubSubConnectionKilled:
    @skip_if_server_version_lt("3.0.0")
    async def test_connection_error_raised_when_connection_dies(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        for client in await r.client_list():
            if client["cmd"] == "subscribe":
                await r.client_kill_filter(_id=client["id"])
        with pytest.raises(ConnectionError):
            await wait_for_message(p)


@pytest.mark.onlynoncluster
class TestPubSubTimeouts:
    async def test_get_message_with_timeout_returns_none(self, r: redis.Redis):
        p = r.pubsub()
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await p.get_message(timeout=0.01) is None


@pytest.mark.onlynoncluster
class TestPubSubRun:
    async def _subscribe(self, p, *args, **kwargs):
        await p.subscribe(*args, **kwargs)
        # Wait for the server to act on the subscription, to be sure that
        # a subsequent publish on another connection will reach the pubsub.
        while True:
            message = await p.get_message(timeout=1)
            if (
                message is not None
                and message["type"] == "subscribe"
                and message["channel"] == b"foo"
            ):
                return

    async def test_callbacks(self, r: redis.Redis):
        def callback(message):
            messages.put_nowait(message)

        messages = asyncio.Queue()
        p = r.pubsub()
        await self._subscribe(p, foo=callback)
        task = asyncio.get_event_loop().create_task(p.run())
        await r.publish("foo", "bar")
        message = await messages.get()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert message == {
            "channel": b"foo",
            "data": b"bar",
            "pattern": None,
            "type": "message",
        }

    async def test_exception_handler(self, r: redis.Redis):
        def exception_handler_callback(e, pubsub) -> None:
            assert pubsub == p
            exceptions.put_nowait(e)

        exceptions = asyncio.Queue()
        p = r.pubsub()
        await self._subscribe(p, foo=lambda x: None)
        with mock.patch.object(p, "get_message", side_effect=Exception("error")):
            task = asyncio.get_event_loop().create_task(
                p.run(exception_handler=exception_handler_callback)
            )
            e = await exceptions.get()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert str(e) == "error"
