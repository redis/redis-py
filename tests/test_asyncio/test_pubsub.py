import asyncio
import functools
import logging
import socket
import sys
from collections import defaultdict
from typing import Optional
from unittest.mock import patch, AsyncMock, MagicMock

# the functionality is available in 3.11.x but has a major issue before
# 3.11.3. See https://github.com/redis/redis-py/issues/2633
if sys.version_info >= (3, 11, 3):
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

from unittest import mock

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.asyncio import Subscription
from redis._parsers import Encoder
from redis.asyncio.client import PubSub
from redis.asyncio.cluster import ClusterPubSub
from redis.crc import key_slot

from redis.exceptions import (
    ConnectionError,
    RedisError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
)
from redis.typing import EncodableT
from redis.utils import str_if_bytes
from tests.conftest import REDIS_INFO, get_protocol_version, skip_if_server_version_lt

from .compat import aclosing, create_task


def with_timeout(t):
    def wrapper(corofunc):
        @functools.wraps(corofunc)
        async def run(*args, **kwargs):
            async with async_timeout(t):
                return await corofunc(*args, **kwargs)

        return run

    return wrapper


async def wait_for_message(pubsub, timeout=0.2, ignore_subscribe_messages=False):
    now = asyncio.get_running_loop().time()
    timeout = now + timeout
    while now < timeout:
        message = await pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages
        )
        if message is not None:
            return message
        await asyncio.sleep(0.01)
        now = asyncio.get_running_loop().time()
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
    elif type == "shard_channel":
        return {
            "p": pubsub,
            "sub_type": "ssubscribe",
            "unsub_type": "sunsubscribe",
            "sub_func": pubsub.ssubscribe,
            "unsub_func": pubsub.sunsubscribe,
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


@pytest_asyncio.fixture()
async def pubsub(r: redis.Redis):
    async with r.pubsub() as p:
        yield p


@pytest.mark.fixed_client
class TestPubSubSubscriberModeProtocol:
    DEFAULT_STANDALONE_URL = "redis://localhost:6379/0"
    DEFAULT_CLUSTER_URL = "redis://localhost:16379/0"

    async def _get_resp2_client(self, request, cluster=False):
        client = None
        redis_url = self._get_url(request, cluster)
        try:
            if cluster:
                client = redis.RedisCluster.from_url(
                    redis_url,
                    protocol=2,
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )
                await client.initialize()
            else:
                client = redis.Redis.from_url(
                    redis_url,
                    protocol=2,
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )
            await client.ping()
        except RedisError as exc:
            await self._close_client(client)
            pytest.skip(
                f"Redis {'cluster' if cluster else 'standalone'} unavailable: {exc}"
            )

        if get_protocol_version(client) not in [2, "2"]:
            await self._close_client(client)
            pytest.skip("subscriber-mode command rejection is RESP2-specific")

        return client

    def _get_url(self, request, cluster):
        if bool(REDIS_INFO.get("cluster_enabled", False)) == cluster:
            return request.config.getoption("--redis-url")
        if cluster:
            return self.DEFAULT_CLUSTER_URL
        return self.DEFAULT_STANDALONE_URL

    async def _close_client(self, client):
        if client is not None:
            await client.aclose()

    async def test_subscriber_mode_rejects_regular_command_after_subscribe(
        self, request
    ):
        r = await self._get_resp2_client(request)
        p = r.pubsub()
        try:
            await p.subscribe("subscriber-mode")
            assert (await wait_for_message(p))["type"] == "subscribe"

            await p.execute_command("SET", "should-not-run", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                await p.parse_response(block=False, timeout=1)
        finally:
            await p.aclose()
            await self._close_client(r)

    async def test_subscriber_mode_rejects_regular_command_after_psubscribe(
        self, request
    ):
        r = await self._get_resp2_client(request)
        p = r.pubsub()
        try:
            await p.psubscribe("subscriber-*")
            assert (await wait_for_message(p))["type"] == "psubscribe"

            await p.execute_command("SET", "should-not-run", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                await p.parse_response(block=False, timeout=1)
        finally:
            await p.aclose()
            await self._close_client(r)

    @skip_if_server_version_lt("7.0.0")
    async def test_subscriber_mode_rejects_regular_command_after_ssubscribe(
        self, request
    ):
        r = await self._get_resp2_client(request, cluster=True)
        channel = "subscriber-mode-{shard}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            await p.ssubscribe(channel)
            message = await p.get_sharded_message(target_node=node, timeout=1)
            assert message["type"] == "ssubscribe"

            per_node_pubsub = p.node_pubsub_mapping[node.name]
            await per_node_pubsub.execute_command("SET", "should-not-run-{shard}", "1")
            with pytest.raises(ResponseError, match="only .* allowed|Can't execute"):
                await per_node_pubsub.parse_response(block=False, timeout=1)
        finally:
            await p.aclose()
            await self._close_client(r)


class TestPubSubCloseServerCleanup:
    async def _wait_for_result_to_match_expected(
        self, get_result, expected, timeout=2.0
    ):
        deadline = asyncio.get_running_loop().time() + timeout
        result = None
        while asyncio.get_running_loop().time() < deadline:
            result = await get_result()
            if result == expected:
                return
            await asyncio.sleep(0.01)
        assert result == expected

    @pytest.mark.onlynoncluster
    async def test_close_while_subscribed_removes_numsub_entry(self, r: redis.Redis):
        channel = "close-cleanup"
        p = r.pubsub()
        try:
            await p.subscribe(channel)
            assert (await wait_for_message(p))["type"] == "subscribe"
            await self._wait_for_result_to_match_expected(
                lambda: r.pubsub_numsub(channel),
                [(channel.encode(), 1)],
            )
        finally:
            await p.aclose()

        assert p.connection is None
        assert p.channels == {}
        await self._wait_for_result_to_match_expected(
            lambda: r.pubsub_numsub(channel),
            [(channel.encode(), 0)],
        )

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    async def test_close_while_ssubscribed_removes_shardnumsub_entry(
        self, r: redis.Redis
    ):
        channel = "close-cleanup-{shard}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            await p.ssubscribe(channel)
            message = await p.get_sharded_message(target_node=node, timeout=1)
            assert message["type"] == "ssubscribe"
            await self._wait_for_result_to_match_expected(
                lambda: r.pubsub_shardnumsub(channel, target_nodes=node),
                [(channel.encode(), 1)],
            )
        finally:
            await p.aclose()

        assert p.node_pubsub_mapping == {}
        assert p.shard_channels == {}
        await self._wait_for_result_to_match_expected(
            lambda: r.pubsub_shardnumsub(channel, target_nodes=node),
            [(channel.encode(), 0)],
        )


@pytest.mark.onlycluster
@skip_if_server_version_lt("7.0.0")
class TestClusterShardedPubSubDelivery:
    async def _wait_for_sharded_acks(
        self, pubsub, count, node=None, message_type="ssubscribe"
    ):
        for _ in range(count):
            if node is not None:
                message = await pubsub.get_sharded_message(
                    target_node=node, timeout=2.0
                )
            else:
                message = await pubsub.get_sharded_message(timeout=2.0)
            assert message is not None
            assert message["type"] == message_type

    async def _read_sharded_messages(self, pubsub, expected_count, timeout=5.0):
        messages = []
        deadline = asyncio.get_running_loop().time() + timeout
        while (
            asyncio.get_running_loop().time() < deadline
            and len(messages) < expected_count
        ):
            message = await pubsub.get_sharded_message(timeout=0.05)
            if message is not None and message["type"] == "smessage":
                messages.append(message)
        return messages

    async def test_multiple_sharded_subscribers_receive_all_messages(
        self, r: redis.Redis
    ):
        channels = ["multi-sub-{shared}:0", "multi-sub-{shared}:1"]
        subscriber_count = 2
        message_count = 20
        expected = set(range(message_count))
        received = [defaultdict(set) for _ in range(subscriber_count)]
        subscribers = [r.pubsub() for _ in range(subscriber_count)]

        try:
            for pubsub in subscribers:
                await pubsub.ssubscribe(*channels)
                await self._wait_for_sharded_acks(pubsub, len(channels))

            for seq in range(message_count):
                for channel in channels:
                    assert await r.spublish(channel, str(seq)) == subscriber_count

            deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < deadline:
                for index, pubsub in enumerate(subscribers):
                    message = await pubsub.get_sharded_message(timeout=0.01)
                    if message is not None and message["type"] == "smessage":
                        channel = str_if_bytes(message["channel"])
                        received[index][channel].add(int(message["data"]))

                if all(
                    all(subscriber[channel] == expected for channel in channels)
                    for subscriber in received
                ):
                    return

            assert [
                {channel: sorted(seqs) for channel, seqs in subscriber.items()}
                for subscriber in received
            ] == [
                {channel: list(range(message_count)) for channel in channels}
                for _ in range(subscriber_count)
            ]
        finally:
            for pubsub in subscribers:
                await pubsub.aclose()

    async def test_duplicate_ssubscribe_delivers_each_message_once(
        self, r: redis.Redis
    ):
        channel = "duplicate-ssubscribe-{shared}"
        p = r.pubsub()
        try:
            await p.ssubscribe(channel)
            await self._wait_for_sharded_acks(p, 1)

            await p.ssubscribe(channel)
            maybe_ack = await p.get_sharded_message(timeout=0.5)
            assert maybe_ack is None or maybe_ack["type"] == "ssubscribe"

            for seq in range(10):
                assert await r.spublish(channel, str(seq)) == 1

            messages = await self._read_sharded_messages(p, expected_count=10)
            received = [int(message["data"]) for message in messages]

            assert sorted(received) == list(range(10))
            assert await p.get_sharded_message(timeout=0.5) is None
        finally:
            await p.aclose()

    async def test_no_messages_delivered_after_sunsubscribe(self, r: redis.Redis):
        kept = "still-subscribed-{shared}"
        removed = "removed-{shared}"
        p = r.pubsub()
        try:
            await p.ssubscribe(kept, removed)
            await self._wait_for_sharded_acks(p, 2)

            await p.sunsubscribe(removed)
            await self._wait_for_sharded_acks(p, 1, message_type="sunsubscribe")

            for seq in range(10):
                assert await r.spublish(kept, str(seq)) == 1
                assert await r.spublish(removed, str(seq)) == 0

            messages = await self._read_sharded_messages(p, expected_count=10)
            delivered_channels = [
                str_if_bytes(message["channel"]) for message in messages
            ]

            assert delivered_channels == [kept] * 10
            assert await p.get_sharded_message(timeout=0.5) is None
        finally:
            await p.aclose()


@pytest.mark.onlycluster
@skip_if_server_version_lt("7.0.0")
class TestClusterShardedPubSubConnectionLifecycle:
    async def _wait_for_ssubscribe_ack(self, pubsub):
        message = await pubsub.get_sharded_message(timeout=2.0)
        assert message is not None
        assert message["type"] == "ssubscribe"

    async def test_cluster_sharded_pubsub_creates_connections_lazily(
        self, r: redis.Redis
    ):
        channel = "lazy-connection-{shared}"
        node = r.get_node_from_key(channel)
        p = r.pubsub()
        try:
            assert p.node_pubsub_mapping == {}
            assert p.connection is None

            await p.ssubscribe(channel)
            await self._wait_for_ssubscribe_ack(p)

            assert list(p.node_pubsub_mapping) == [node.name]
            assert p.node_pubsub_mapping[node.name].connection is not None
        finally:
            await p.aclose()

    async def test_cluster_sharded_pubsub_reuses_connection_for_same_shard(
        self, r: redis.Redis
    ):
        first = "reuse-1-{same-shard}"
        second = "reuse-2-{same-shard}"
        node = r.get_node_from_key(first)
        assert node.name == r.get_node_from_key(second).name
        p = r.pubsub()
        try:
            await p.ssubscribe(first)
            await self._wait_for_ssubscribe_ack(p)
            first_mapping = dict(p.node_pubsub_mapping)
            first_pubsub = p.node_pubsub_mapping[node.name]

            await p.ssubscribe(second)
            await self._wait_for_ssubscribe_ack(p)

            assert p.node_pubsub_mapping == first_mapping
            assert p.node_pubsub_mapping[node.name] is first_pubsub
            assert len(p.node_pubsub_mapping) == 1
        finally:
            await p.aclose()


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

    async def test_channel_subscribe_unsubscribe(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_subscribe_unsubscribe(**kwargs)

    async def test_pattern_subscribe_unsubscribe(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    async def test_shard_channel_subscribe_unsubscribe(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "shard_channel")
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

    async def test_resubscribe_to_channels_on_reconnection(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_resubscribe_to_patterns_on_reconnection(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def test_resubscribe_binary_channel_on_reconnection(self, pubsub):
        """Binary channel names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        # b'\x80\x81\x82' is deliberately invalid UTF-8
        binary_channel = b"\x80\x81\x82"
        p = pubsub
        await p.subscribe(binary_channel)
        assert await wait_for_message(p) is not None  # consume subscribe ack

        # force reconnect
        await p.connection.disconnect()

        # get_message triggers on_connect → re-subscribe; must not raise
        messages = []
        for _ in range(1):
            message = await wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "subscribe"
        assert messages[0]["channel"] == binary_channel

    async def test_resubscribe_binary_pattern_on_reconnection(self, pubsub):
        """Binary pattern names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        binary_pattern = b"\x80\x81*"
        p = pubsub
        await p.psubscribe(binary_pattern)
        assert await wait_for_message(p) is not None  # consume psubscribe ack

        # force reconnect
        await p.connection.disconnect()

        messages = []
        for _ in range(1):
            message = await wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "psubscribe"
        assert messages[0]["channel"] == binary_pattern

    async def test_resubscribe_binary_channel_with_handler_on_reconnection(
        self, pubsub, r
    ):
        binary_channel = b"\x80\x81\x82"
        handled = []

        def handler(message):
            handled.append(message)

        p = pubsub
        await p.subscribe(Subscription(binary_channel, handler))
        assert await wait_for_message(p) == {
            "type": "subscribe",
            "pattern": None,
            "channel": binary_channel,
            "data": 1,
        }

        await p.connection.disconnect()

        assert await wait_for_message(p) == {
            "type": "subscribe",
            "pattern": None,
            "channel": binary_channel,
            "data": 1,
        }
        assert await r.publish(binary_channel, b"test message") == 1
        assert await p.get_message(timeout=1) is None
        assert handled == [
            {
                "type": "message",
                "pattern": None,
                "channel": binary_channel,
                "data": b"test message",
            }
        ]

    @pytest.mark.fixed_client
    async def test_resubscribe_binary_handler_uses_subscription(self):
        pool = MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)
        binary_channel = b"\x80\x81\x82"
        handler = MagicMock()
        p.channels = {binary_channel: handler}

        calls = []

        async def fake_subscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "subscribe", side_effect=fake_subscribe):
            await p._resubscribe(p.channels, p.subscribe)

        assert calls == [((Subscription(binary_channel, handler),), {})]

    @pytest.mark.fixed_client
    async def test_cluster_pubsub_resubscribe_shard_channels_groups_by_slot(self):
        """The cluster-aware ``_resubscribe_shard_channels`` must group shard
        channels by hash slot on reconnect. A cluster node can own multiple
        slot ranges, so a single batched SSUBSCRIBE across different slots
        would be rejected by Redis with CROSSSLOT and resubscription would
        fail silently. The same method is bound to the per-node PubSub
        instances managed by ``ClusterPubSub``.
        """
        # Pure client-side logic: mock the pool, use a real encoder, no
        # server needed. ``key_slot`` is client-side CRC16, so slot
        # assignments are deterministic without a cluster.
        pool = MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)

        # Two channels share a hash tag (same slot); a third hashes to a
        # different slot.
        ch_a1 = b"{slot-a}-one"
        ch_a2 = b"{slot-a}-two"
        ch_b = b"{slot-b}-one"
        assert key_slot(ch_a1) == key_slot(ch_a2)
        assert key_slot(ch_a1) != key_slot(ch_b)

        # Populate state as if previously subscribed, without going to the wire.
        p.shard_channels = {ch_a1: None, ch_a2: None, ch_b: None}

        calls = []

        async def fake_ssubscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "ssubscribe", side_effect=fake_ssubscribe):
            await ClusterPubSub._resubscribe_shard_channels(p)

        # Every ssubscribe call must target exactly one slot.
        slots_per_call = []
        all_channels = set()
        for args, kwargs in calls:
            channels = set(args) | set(
                k.encode() if isinstance(k, str) else k for k in kwargs
            )
            assert channels, "ssubscribe called with no channels"
            call_slots = {key_slot(c) for c in channels}
            assert len(call_slots) == 1, (
                f"ssubscribe grouped channels from multiple slots: {call_slots}"
            )
            slots_per_call.append(next(iter(call_slots)))
            all_channels.update(channels)

        # All original channels must be resubscribed, across exactly two
        # distinct slot groups.
        assert all_channels == {ch_a1, ch_a2, ch_b}
        assert set(slots_per_call) == {key_slot(ch_a1), key_slot(ch_b)}
        # Channels sharing a slot are batched: two unique slots => two calls.
        assert len(calls) == 2

    @pytest.mark.fixed_client
    async def test_cluster_pubsub_resubscribe_binary_shard_channel_with_handler(self):
        pool = MagicMock()
        encoder = Encoder(
            encoding="utf-8", encoding_errors="strict", decode_responses=False
        )
        p = PubSub(connection_pool=pool, encoder=encoder)
        binary_channel = b"\x80\x81\x82"
        handler = MagicMock()
        p.shard_channels = {binary_channel: handler}

        calls = []

        async def fake_ssubscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "ssubscribe", side_effect=fake_ssubscribe):
            await ClusterPubSub._resubscribe_shard_channels(p)

        assert calls == [((Subscription(binary_channel, handler),), {})]

    @skip_if_server_version_lt("7.0.0")
    async def test_standalone_pubsub_on_connect_batches_shard_channels(self, pubsub):
        """The standalone PubSub has no notion of slots and must keep
        batching every tracked shard channel into a single SSUBSCRIBE call
        on reconnect, as it did before cluster-aware resubscription existed.
        """
        p = pubsub
        ch_a = b"{slot-a}-one"
        ch_b = b"{slot-b}-one"
        assert key_slot(ch_a) != key_slot(ch_b)

        p.shard_channels = {ch_a: None, ch_b: None}

        calls = []

        async def fake_ssubscribe(*args, **kwargs):
            calls.append((tuple(args), dict(kwargs)))

        with mock.patch.object(p, "ssubscribe", side_effect=fake_ssubscribe):
            await p.on_connect(connection=None)

        assert len(calls) == 1
        args, kwargs = calls[0]
        assert set(args) == {ch_a, ch_b}
        assert kwargs == {}

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

    async def test_subscribe_property_with_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    async def test_subscribe_property_with_patterns(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    async def test_subscribe_property_with_shard_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "shard_channel")
        await self._test_subscribed_property(**kwargs)

    async def test_aclosing(self, r: redis.Redis):
        p = r.pubsub()
        async with aclosing(p):
            assert p.subscribed is False
            await p.subscribe("foo")
            assert p.subscribed is True
        assert p.subscribed is False

    async def test_context_manager(self, r: redis.Redis):
        p = r.pubsub()
        async with p:
            assert p.subscribed is False
            await p.subscribe("foo")
            assert p.subscribed is True
        assert p.subscribed is False

    async def test_close_is_aclose(self, r: redis.Redis):
        """
        Test backwards compatible close method
        """
        p = r.pubsub()
        assert p.subscribed is False
        await p.subscribe("foo")
        assert p.subscribed is True
        with pytest.deprecated_call():
            await p.close()
        assert p.subscribed is False

    async def test_reset_is_aclose(self, r: redis.Redis):
        """
        Test backwards compatible reset method
        """
        p = r.pubsub()
        assert p.subscribed is False
        await p.subscribe("foo")
        assert p.subscribed is True
        with pytest.deprecated_call():
            await p.reset()
        assert p.subscribed is False

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
        await p.aclose()

    async def test_ignore_individual_subscribe_messages(self, pubsub):
        p = pubsub

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

    async def test_sub_unsub_resub_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    async def test_sub_unsub_resub_patterns(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
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

    async def test_sub_unsub_all_resub_channels(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "channel")
        await self._test_sub_unsub_all_resub(**kwargs)

    async def test_sub_unsub_all_resub_patterns(self, pubsub):
        kwargs = make_subscribe_test_data(pubsub, "pattern")
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

    async def test_published_message_to_channel(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await r.publish("foo", "test message") == 1

        message = await wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("message", "foo", "test message")

    async def test_published_message_to_pattern(self, r: redis.Redis, pubsub):
        p = pubsub
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
        await p.aclose()

    async def test_binary_channel_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = b"\x80\x81\x82"
        await p.subscribe(Subscription(channel, self.message_handler))
        assert await p.get_message(timeout=1) is None
        assert await r.publish(channel, b"test message") == 1
        assert await p.get_message(timeout=1) is None
        assert self.message == {
            "type": "message",
            "pattern": None,
            "channel": channel,
            "data": b"test message",
        }
        await p.aclose()

    async def test_channel_async_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(foo=self.async_message_handler)
        assert await wait_for_message(p) is None
        assert await r.publish("foo", "test message") == 1
        assert await wait_for_message(p) is None
        assert self.async_message == make_message("message", "foo", "test message")
        await p.aclose()

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
        await p.aclose()

    @skip_if_server_version_lt("7.0.0")
    async def test_binary_shard_channel_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = b"\x80\x81\x82"
        await p.ssubscribe(Subscription(channel, self.message_handler))
        assert await p.get_message(timeout=1) is None
        assert await r.spublish(channel, b"test message") == 1
        assert await p.get_message(timeout=1) is None
        assert self.message == {
            "type": "smessage",
            "pattern": None,
            "channel": channel,
            "data": b"test message",
        }
        await p.aclose()

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
        await p.aclose()

    @pytest.mark.onlynoncluster
    async def test_binary_pattern_message_handler_with_subscription(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = b"\x80*"
        channel = b"\x80channel"
        await p.psubscribe(Subscription(pattern, self.message_handler))
        assert await p.get_message(timeout=1) is None
        assert await r.publish(channel, b"test message") == 1
        assert await p.get_message(timeout=1) is None
        assert self.message == {
            "type": "pmessage",
            "pattern": pattern,
            "channel": channel,
            "data": b"test message",
        }
        await p.aclose()

    async def test_unicode_channel_message_handler(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        await p.subscribe(**channels)
        assert await wait_for_message(p) is None
        assert await r.publish(channel, "test message") == 1
        assert await wait_for_message(p) is None
        assert self.message == make_message("message", channel, "test message")
        await p.aclose()

    @pytest.mark.onlynoncluster
    # see: https://redis.readthedocs.io/en/stable/clustering.html#known-pubsub-limitations
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
        await p.aclose()

    async def test_get_message_without_subscribe(self, r: redis.Redis, pubsub):
        p = pubsub
        with pytest.raises(RuntimeError) as info:
            await p.get_message()
        expect = (
            "connection not set: did you forget to call subscribe() or psubscribe()?"
        )
        assert expect in info.exconly()


@pytest.mark.onlynoncluster
class TestPubSubRESP3Handler:
    async def my_handler(self, message):
        self.message = ["my handler", message]

    async def test_push_handler(self, r):
        if get_protocol_version(r) in [2, "2", None]:
            return
        p = r.pubsub(push_handler_func=self.my_handler)
        await p.subscribe("foo")
        assert await wait_for_message(p) is None
        assert self.message == ["my handler", [b"subscribe", b"foo", 1]]
        assert await r.publish("foo", "test message") == 1
        assert await wait_for_message(p) is None
        assert self.message == ["my handler", [b"message", b"foo", b"test message"]]


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
        return await create_redis(decode_responses=True)

    async def test_channel_subscribe_unsubscribe(self, pubsub):
        p = pubsub
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "subscribe", self.channel, 1
        )

        await p.unsubscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "unsubscribe", self.channel, 0
        )

    async def test_pattern_subscribe_unsubscribe(self, pubsub):
        p = pubsub
        await p.psubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "psubscribe", self.pattern, 1
        )

        await p.punsubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message(
            "punsubscribe", self.pattern, 0
        )

    async def test_channel_publish(self, r: redis.Redis, pubsub):
        p = pubsub
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message(
            "subscribe", self.channel, 1
        )
        await r.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message(
            "message", self.channel, self.data
        )

    @pytest.mark.onlynoncluster
    async def test_pattern_publish(self, r: redis.Redis, pubsub):
        p = pubsub
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
        await p.aclose()

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
        await p.aclose()

    async def test_context_manager(self, r: redis.Redis):
        async with r.pubsub() as pubsub:
            await pubsub.subscribe("foo")
            assert pubsub.connection is not None

        assert pubsub.connection is None
        assert pubsub.channels == {}
        assert pubsub.patterns == {}
        await pubsub.aclose()


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
    async def test_pubsub_channels(self, r: redis.Redis, pubsub):
        p = pubsub
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
        await p1.aclose()
        await p2.aclose()
        await p3.aclose()

    @skip_if_server_version_lt("2.8.0")
    async def test_pubsub_numpat(self, r: redis.Redis):
        p = r.pubsub()
        await p.psubscribe("*oo", "*ar", "b*z")
        for i in range(3):
            assert (await wait_for_message(p))["type"] == "psubscribe"
        assert await r.pubsub_numpat() == 3
        await p.aclose()


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
        await p.aclose()

    @skip_if_server_version_lt("3.0.0")
    async def test_send_pubsub_ping_message(self, r: redis.Redis):
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        await p.ping(message="hello world")
        assert await wait_for_message(p) == make_message(
            type="pong", channel=None, data="hello world", pattern=None
        )
        await p.aclose()


@pytest.mark.onlynoncluster
class TestPubSubHealthCheckResponse:
    """Tests for health check response validation with different decode_responses settings"""

    async def test_health_check_response_decode_false_list_format(self, r: redis.Redis):
        """Test health_check_response includes list format with decode_responses=False"""
        p = r.pubsub()
        # List format: [b"pong", b"redis-py-health-check"]
        assert [b"pong", b"redis-py-health-check"] in p.health_check_response
        await p.aclose()

    async def test_health_check_response_decode_false_bytes_format(
        self, r: redis.Redis
    ):
        """Test health_check_response includes bytes format with decode_responses=False"""
        p = r.pubsub()
        # Bytes format: b"redis-py-health-check"
        assert b"redis-py-health-check" in p.health_check_response
        await p.aclose()

    async def test_health_check_response_decode_true_list_format(self, create_redis):
        """Test health_check_response includes list format with decode_responses=True"""
        r = await create_redis(decode_responses=True)
        p = r.pubsub()
        # List format: ["pong", "redis-py-health-check"]
        assert ["pong", "redis-py-health-check"] in p.health_check_response
        await p.aclose()
        await r.aclose()

    async def test_health_check_response_decode_true_string_format(self, create_redis):
        """Test health_check_response includes string format with decode_responses=True"""
        r = await create_redis(decode_responses=True)
        p = r.pubsub()
        # String format: "redis-py-health-check" (THE FIX!)
        assert "redis-py-health-check" in p.health_check_response
        await p.aclose()
        await r.aclose()

    async def test_health_check_response_decode_false_excludes_string(
        self, r: redis.Redis
    ):
        """Test health_check_response excludes string format with decode_responses=False"""
        p = r.pubsub()
        # String format should NOT be in the list when decode_responses=False
        assert "redis-py-health-check" not in p.health_check_response
        await p.aclose()

    async def test_health_check_response_decode_true_excludes_bytes(self, create_redis):
        """Test health_check_response excludes bytes format with decode_responses=True"""
        r = await create_redis(decode_responses=True)
        p = r.pubsub()
        # Bytes format should NOT be in the list when decode_responses=True
        assert b"redis-py-health-check" not in p.health_check_response
        await p.aclose()
        await r.aclose()


@pytest.mark.onlynoncluster
class TestPubSubConnectionKilled:
    @skip_if_server_version_lt("3.0.0")
    async def test_connection_error_raised_when_connection_dies(
        self, r: redis.Redis, pubsub
    ):
        p = pubsub
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        for client in await r.client_list():
            if client["cmd"] == "subscribe":
                await r.client_kill_filter(_id=client["id"])
        with pytest.raises(ConnectionError):
            await wait_for_message(p)


@pytest.mark.onlynoncluster
class TestPubSubTimeouts:
    async def test_get_message_with_timeout_returns_none(self, pubsub):
        p = pubsub
        await p.subscribe("foo")
        assert await wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert await p.get_message(timeout=0.01) is None


@pytest.mark.onlynoncluster
class TestPubSubReconnect:
    @with_timeout(2)
    async def test_reconnect_listen(self, r: redis.Redis, pubsub):
        """
        Test that a loop processing PubSub messages can survive
        a disconnect, by issuing a connect() call.
        """
        messages = asyncio.Queue()
        interrupt = False

        async def loop():
            # must make sure the task exits
            async with async_timeout(2):
                nonlocal interrupt
                await pubsub.subscribe("foo")
                while True:
                    try:
                        try:
                            await pubsub.connect()
                            await loop_step()
                        except redis.ConnectionError:
                            await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        # we use a cancel to interrupt the "listen"
                        # when we perform a disconnect
                        if interrupt:
                            interrupt = False
                        else:
                            raise

        async def loop_step():
            # get a single message via listen()
            async for message in pubsub.listen():
                await messages.put(message)
                break

        task = asyncio.get_running_loop().create_task(loop())
        # get the initial connect message
        async with async_timeout(1):
            message = await messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }
        # now, disconnect the connection.
        await pubsub.connection.disconnect()
        interrupt = True
        task.cancel()  # interrupt the listen call
        # await another auto-connect message
        message = await messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


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

    async def test_callbacks(self, r: redis.Redis, pubsub):
        def callback(message):
            messages.put_nowait(message)

        messages = asyncio.Queue()
        p = pubsub
        await self._subscribe(p, foo=callback)
        task = asyncio.get_running_loop().create_task(p.run())
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

    async def test_exception_handler(self, r: redis.Redis, pubsub):
        def exception_handler_callback(e, pubsub) -> None:
            assert pubsub == p
            exceptions.put_nowait(e)

        exceptions = asyncio.Queue()
        p = pubsub
        await self._subscribe(p, foo=lambda x: None)
        with mock.patch.object(p, "get_message", side_effect=Exception("error")):
            task = asyncio.get_running_loop().create_task(
                p.run(exception_handler=exception_handler_callback)
            )
            e = await exceptions.get()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert str(e) == "error"

    async def test_late_subscribe(self, r: redis.Redis, pubsub):
        def callback(message):
            messages.put_nowait(message)

        messages = asyncio.Queue()
        p = pubsub
        # Establish the connection before run() so CI does not race startup.
        await p.connect()
        task = asyncio.get_running_loop().create_task(p.run())
        await p.subscribe(foo=callback)
        # wait tof the subscribe to finish.  Cannot use _subscribe() because
        # p.run() is already accepting messages
        while True:
            n = await r.publish("foo", "bar")
            if n == 1:
                break
            await asyncio.sleep(0.1)
        message_task = asyncio.create_task(messages.get())
        done, _ = await asyncio.wait(
            {message_task, task},
            timeout=1,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if task in done:
            message_task.cancel()
            await asyncio.gather(message_task, return_exceptions=True)
            await task
            pytest.fail("PubSub run task exited before receiving late subscription")
        if message_task not in done:
            message_task.cancel()
            await asyncio.gather(message_task, return_exceptions=True)
            pytest.fail("Timed out waiting for late subscription message")

        message = message_task.result()
        task.cancel()
        # we expect a cancelled error, not the Runtime error
        # ("did you forget to call subscribe()"")
        with pytest.raises(asyncio.CancelledError):
            await task
        assert message == {
            "channel": b"foo",
            "data": b"bar",
            "pattern": None,
            "type": "message",
        }


# @pytest.mark.xfail
@pytest.mark.parametrize("method", ["get_message", "listen"])
@pytest.mark.onlynoncluster
class TestPubSubAutoReconnect:
    timeout = 4

    async def mysetup(self, r, method):
        self.messages = asyncio.Queue()
        self.pubsub = r.pubsub()
        # State: 0 = initial state , 1 = after disconnect, 2 = ConnectionError is seen,
        # 3=successfully reconnected 4 = exit
        self.state = 0
        self.cond = asyncio.Condition()
        if method == "get_message":
            self.get_message = self.loop_step_get_message
        else:
            self.get_message = self.loop_step_listen

        self.task = create_task(self.loop())
        # get the initial connect message
        message = await self.messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    async def myfinish(self):
        message = await self.messages.get()
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    async def mykill(self):
        # kill thread
        async with self.cond:
            self.state = 4  # quit
        await self.task

    async def test_reconnect_socket_error(self, r: redis.Redis, method):
        """
        Test that a socket error will cause reconnect
        """
        try:
            async with async_timeout(self.timeout):
                await self.mysetup(r, method)
                # now, disconnect the connection, and wait for it to be re-established
                async with self.cond:
                    assert self.state == 0
                    self.state = 1
                    with mock.patch.object(self.pubsub.connection, "_parser") as m:
                        m.read_response.side_effect = socket.error
                        m.can_read_destructive.side_effect = socket.error
                        # wait until task noticies the disconnect until we
                        # undo the patch
                        await self.cond.wait_for(lambda: self.state >= 2)
                        assert not self.pubsub.connection.is_connected
                        # it is in a disconnecte state
                    # wait for reconnect
                    await self.cond.wait_for(
                        lambda: self.pubsub.connection.is_connected
                    )
                    assert self.state == 3

                await self.myfinish()
        finally:
            await self.mykill()

    async def test_reconnect_disconnect(self, r: redis.Redis, method):
        """
        Test that a manual disconnect() will cause reconnect
        """
        try:
            async with async_timeout(self.timeout):
                await self.mysetup(r, method)
                # now, disconnect the connection, and wait for it to be re-established
                async with self.cond:
                    self.state = 1
                    await self.pubsub.connection.disconnect()
                    assert not self.pubsub.connection.is_connected
                    # wait for reconnect
                    await self.cond.wait_for(
                        lambda: self.pubsub.connection.is_connected
                    )
                    assert self.state == 3

                await self.myfinish()
        finally:
            await self.mykill()

    async def loop(self):
        # reader loop, performing state transitions as it
        # discovers disconnects and reconnects
        await self.pubsub.subscribe("foo")
        while True:
            await asyncio.sleep(0.01)  # give main thread chance to get lock
            async with self.cond:
                old_state = self.state
                try:
                    if self.state == 4:
                        break
                    got_msg = await self.get_message()
                    assert got_msg
                    if self.state in (1, 2):
                        self.state = 3  # successful reconnect
                except redis.ConnectionError:
                    assert self.state in (1, 2)
                    self.state = 2  # signal that we noticed the disconnect
                finally:
                    self.cond.notify()
                # make sure that we did notice the connection error
                # or reconnected without any error
                if old_state == 1:
                    assert self.state in (2, 3)

    async def loop_step_get_message(self):
        # get a single message via get_message
        message = await self.pubsub.get_message(timeout=0.1)
        if message is not None:
            await self.messages.put(message)
            return True
        return False

    async def loop_step_listen(self):
        # get a single message via listen()
        try:
            async with async_timeout(0.5):
                async for message in self.pubsub.listen():
                    await self.messages.put(message)
                    return True
        except asyncio.TimeoutError:
            return False


@pytest.mark.onlynoncluster
class TestBaseException:
    @pytest.mark.skipif(
        sys.version_info < (3, 8), reason="requires python 3.8 or higher"
    )
    async def test_outer_timeout(self, r: redis.Redis):
        """
        Using asyncio_timeout manually outside the inner method timeouts works.
        This works on Python versions 3.8 and greater, at which time asyncio.
        CancelledError became a BaseException instead of an Exception before.
        """
        pubsub = r.pubsub()
        await pubsub.subscribe("foo")
        assert pubsub.connection.is_connected

        async def get_msg_or_timeout(timeout=0.1):
            async with async_timeout(timeout):
                # blocking method to return messages
                while True:
                    response = await pubsub.parse_response(block=True)
                    message = await pubsub.handle_message(
                        response, ignore_subscribe_messages=False
                    )
                    if message is not None:
                        return message

        # get subscribe message
        msg = await get_msg_or_timeout(10)
        assert msg is not None
        # timeout waiting for another message which never arrives
        assert pubsub.connection.is_connected
        with pytest.raises(asyncio.TimeoutError):
            await get_msg_or_timeout()
        # the timeout on the read should not cause disconnect
        assert pubsub.connection.is_connected

    @pytest.mark.skipif(
        sys.version_info < (3, 8), reason="requires python 3.8 or higher"
    )
    async def test_base_exception(self, r: redis.Redis):
        """
        Manually trigger a BaseException inside the parser's .read_response method
        and verify that it isn't caught
        """
        pubsub = r.pubsub()
        await pubsub.subscribe("foo")
        assert pubsub.connection.is_connected

        async def get_msg():
            # blocking method to return messages
            while True:
                response = await pubsub.parse_response(block=True)
                message = await pubsub.handle_message(
                    response, ignore_subscribe_messages=False
                )
                if message is not None:
                    return message

        # get subscribe message
        msg = await get_msg()
        assert msg is not None
        # timeout waiting for another message which never arrives
        assert pubsub.connection.is_connected
        with (
            patch("redis._parsers._AsyncRESP2Parser.read_response") as mock1,
            patch("redis._parsers._AsyncHiredisParser.read_response") as mock2,
            patch("redis._parsers._AsyncRESP3Parser.read_response") as mock3,
        ):
            mock1.side_effect = BaseException("boom")
            mock2.side_effect = BaseException("boom")
            mock3.side_effect = BaseException("boom")

            with pytest.raises(BaseException):
                await get_msg()

        # the timeout on the read should not cause disconnect
        assert pubsub.connection.is_connected


@pytest.mark.onlynoncluster
class TestAsyncPubSubTimeoutPropagation:
    """
    Tests for timeout propagation through the entire async pubsub read chain.
    Ensures that timeouts are properly passed from get_message() through
    parse_response() to the parser and socket buffer layers.
    """

    @pytest.mark.asyncio
    async def test_get_message_timeout_is_respected(self, r):
        """
        Test that get_message() with timeout parameter respects the timeout
        and returns None when no message arrives within the timeout period.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "subscribe"

        # Call get_message with a short timeout - should return None
        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=0.1)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is None
        # Verify timeout was actually respected (within reasonable bounds)
        assert elapsed < 0.5
        await p.aclose()

    @pytest.mark.asyncio
    async def test_get_message_timeout_with_published_message(self, r):
        """
        Test that get_message() with timeout returns a message if one
        arrives before the timeout expires.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message
        await r.publish("foo", "hello")

        # get_message with timeout should return the message
        msg = await p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"
        await p.aclose()

    @pytest.mark.asyncio
    async def test_parse_response_timeout_propagation(self, r):
        """
        Test that parse_response() properly propagates timeout to read_response().
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Call parse_response with timeout - should respect it
        start = asyncio.get_running_loop().time()
        response = await p.parse_response(block=False, timeout=0.1)
        elapsed = asyncio.get_running_loop().time() - start
        assert response is None
        assert elapsed < 0.5
        await p.aclose()

    @pytest.mark.asyncio
    async def test_get_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_message(timeout=0) returns immediately without blocking.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout=0 should return immediately
        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=0)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is None
        assert elapsed < 0.1
        await p.aclose()

    @pytest.mark.asyncio
    async def test_get_message_timeout_none_blocks(self, r):
        """
        Test that get_message(timeout=None) blocks indefinitely.
        We test this by using a task to publish a message after a delay.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message after a short delay in a task
        start_publishing = asyncio.Event()

        async def publish_after_delay():
            await start_publishing.wait()
            await asyncio.sleep(0.2)
            await r.publish("foo", "delayed_message")

        task = asyncio.create_task(publish_after_delay())

        # get_message with timeout=None should block until message arrives
        start = asyncio.get_running_loop().time()
        start_publishing.set()
        msg = await p.get_message(timeout=None)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed_message"
        # Should have waited at least 0.15 seconds
        assert elapsed >= 0.15
        await task
        await p.aclose()

    @pytest.mark.asyncio
    async def test_multiple_messages_with_timeout(self, r):
        """
        Test that timeout is properly handled when reading multiple messages.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish multiple messages
        await r.publish("foo", "msg1")
        await r.publish("foo", "msg2")
        await r.publish("foo", "msg3")

        # Read all messages with timeout
        messages = []
        for _ in range(3):
            msg = await wait_for_message(p, timeout=1.0)
            if msg:
                messages.append(msg)

        assert len(messages) == 3
        assert messages[0]["data"] == b"msg1"
        assert messages[1]["data"] == b"msg2"
        assert messages[2]["data"] == b"msg3"
        await p.aclose()

    @pytest.mark.asyncio
    async def test_timeout_with_pattern_subscribe(self, r):
        """
        Test that timeout works correctly with pattern subscriptions.
        """
        p = r.pubsub()
        await p.psubscribe("foo*")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "psubscribe"

        # Publish a message matching the pattern
        await r.publish("foobar", "hello")

        # get_message with timeout should return the message
        msg = await p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "pmessage"
        assert msg["data"] == b"hello"
        await p.aclose()

    @pytest.mark.asyncio
    async def test_timeout_with_no_subscription(self, r):
        """
        Test that get_message with timeout returns None when subscribed but no messages.
        """
        p = r.pubsub()
        await p.subscribe("foo")
        # Read subscription message
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout should return None when no messages
        msg = await p.get_message(timeout=0.1)
        assert msg is None
        await p.aclose()


@pytest.mark.asyncio
@pytest.mark.onlynoncluster
class TestAsyncPubSubBlockingListen:
    """
    Tests that PubSub.listen() and parse_response(block=True) block
    indefinitely waiting for a message, ignoring the connection's
    configured socket_timeout. Regression tests for the issue where
    listen() would raise TimeoutError once socket_timeout elapsed.

    Retries are disabled on the subscriber so a TimeoutError from the
    blocking read surfaces immediately instead of being silently retried
    by the client's default retry policy (which would mask the bug).
    """

    SOCKET_TIMEOUT = 0.3
    PUBLISH_DELAY = SOCKET_TIMEOUT * 3

    async def _make_subscriber(self, create_redis):
        from redis.asyncio.retry import Retry
        from redis.backoff import NoBackoff

        return await create_redis(
            socket_timeout=self.SOCKET_TIMEOUT,
            retry=Retry(NoBackoff(), 0),
            retry_on_timeout=False,
        )

    async def test_listen_blocks_longer_than_socket_timeout(self, create_redis):
        """
        listen() must keep blocking past socket_timeout. Configure a very
        short socket_timeout, then publish after a delay that exceeds it,
        and assert listen() yields the message instead of raising
        TimeoutError.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None and msg["type"] == "subscribe"

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "hello")

        task = asyncio.create_task(publish_after_delay())

        start = asyncio.get_running_loop().time()
        msg = await p.listen().__anext__()
        elapsed = asyncio.get_running_loop().time() - start
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"
        # Should have blocked past socket_timeout to receive the message.
        assert elapsed > self.SOCKET_TIMEOUT
        await task
        await p.aclose()

    async def test_get_message_block_indefinitely_past_socket_timeout(
        self, create_redis
    ):
        """
        get_message(timeout=None) must block past socket_timeout.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        msg = await wait_for_message(p, timeout=1.0)
        assert msg is not None

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "delayed")

        task = asyncio.create_task(publish_after_delay())

        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=None)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed"
        assert elapsed > self.SOCKET_TIMEOUT
        await task
        await p.aclose()

    async def test_parse_response_block_true_blocks_past_socket_timeout(
        self, create_redis
    ):
        """
        parse_response(block=True) must block past socket_timeout.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe("foo")
        # Drain the subscribe ack before calling parse_response directly.
        await wait_for_message(p, timeout=1.0)

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "raw")

        task = asyncio.create_task(publish_after_delay())

        start = asyncio.get_running_loop().time()
        response = await p.parse_response(block=True)
        elapsed = asyncio.get_running_loop().time() - start
        # Response is the raw pubsub frame [b"message", b"foo", b"raw"].
        assert response is not None
        assert response[0] == b"message"
        assert response[-1] == b"raw"
        assert elapsed > self.SOCKET_TIMEOUT
        await task
        await p.aclose()

    async def test_blocking_listen_does_not_mutate_socket_timeout(self, create_redis):
        """
        A blocking read must not mutate the connection's configured
        socket_timeout. Otherwise reconnect/AUTH/HELLO/resubscribe paths
        triggered by retries inside _execute would inherit no timeout and
        could hang indefinitely. The fix uses a per-read sentinel rather
        than clearing socket_timeout for the duration of the call.
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        await wait_for_message(p, timeout=1.0)

        orig_timeout = p.connection.socket_timeout
        assert orig_timeout == self.SOCKET_TIMEOUT

        # Sample socket_timeout while the blocking read is in progress, to
        # catch any mutation that happens only for the duration of the call.
        async def sample_timeout_during_read():
            # Yield a few times so the get_message read is in flight.
            for _ in range(10):
                await asyncio.sleep(0)
            assert p.connection.socket_timeout == orig_timeout

        async def publish_after_delay():
            await asyncio.sleep(self.PUBLISH_DELAY)
            await publisher.publish("foo", "msg")

        sample_task = asyncio.create_task(sample_timeout_during_read())
        publish_task = asyncio.create_task(publish_after_delay())

        msg = await p.get_message(timeout=None)
        await sample_task
        assert msg is not None and msg["data"] == b"msg"
        assert p.connection.socket_timeout == orig_timeout
        await publish_task
        await p.aclose()

    async def test_block_indefinitely_does_not_alter_subsequent_reads(
        self, create_redis
    ):
        """
        After a blocking read returns, a follow-up non-blocking read must
        still honor the configured ``socket_timeout`` (i.e. the per-read
        ``block_indefinitely`` does not bleed into later operations and
        does not require mutating any connection-wide state).
        """
        r = await self._make_subscriber(create_redis)
        publisher = await create_redis()
        p = r.pubsub()
        await p.subscribe("foo")
        await wait_for_message(p, timeout=1.0)

        # First a blocking read.
        await publisher.publish("foo", "first")
        msg = await p.get_message(timeout=None)
        assert msg is not None and msg["data"] == b"first"

        # Then a non-blocking read with a short timeout and no message.
        # If block_indefinitely leaked, this would block past the timeout.
        start = asyncio.get_running_loop().time()
        msg = await p.get_message(timeout=0.1)
        elapsed = asyncio.get_running_loop().time() - start
        assert msg is None
        assert elapsed < 0.5
        await p.aclose()


@pytest.mark.asyncio
class TestPubSubHandleMessageMetrics:
    """Tests for handle_message recording pubsub metrics."""

    @pytest.fixture
    def mock_pubsub(self):
        """Create a mock PubSub instance for testing handle_message."""
        pubsub = MagicMock()
        pubsub.UNSUBSCRIBE_MESSAGE_TYPES = ("unsubscribe", "punsubscribe")
        pubsub.PUBLISH_MESSAGE_TYPES = ("message", "pmessage")
        pubsub.pending_unsubscribe_patterns = set()
        pubsub.pending_unsubscribe_channels = set()
        pubsub.patterns = {}
        pubsub.channels = {}
        pubsub.ignore_subscribe_messages = False
        return pubsub

    async def test_handle_message_records_metric_for_message_type(self, mock_pubsub):
        """Test that handle_message calls record_pubsub_message for 'message' type."""

        response = [b"message", b"test-channel", b"test-data"]

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            # Call the actual handle_message method
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            # Verify record_pubsub_message was called
            mock_record.assert_awaited_once()
            call_kwargs = mock_record.call_args[1]
            from redis.observability.attributes import PubSubDirection

            assert call_kwargs["direction"] == PubSubDirection.RECEIVE
            assert call_kwargs["channel"] == "test-channel"

    async def test_handle_message_records_metric_for_pmessage_type(self, mock_pubsub):
        """Test that handle_message calls record_pubsub_message for 'pmessage' type."""

        response = [b"pmessage", b"test-pattern*", b"test-channel", b"test-data"]

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            mock_record.assert_awaited_once()
            call_kwargs = mock_record.call_args[1]
            from redis.observability.attributes import PubSubDirection

            assert call_kwargs["direction"] == PubSubDirection.RECEIVE
            assert call_kwargs["channel"] == "test-channel"

    async def test_handle_message_does_not_record_metric_for_subscribe_type(
        self, mock_pubsub
    ):
        """Test that handle_message does NOT call record_pubsub_message for 'subscribe' type."""

        response = [b"subscribe", b"test-channel", 1]

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            mock_record.assert_not_called()

    async def test_handle_message_does_not_record_metric_for_pong_type(
        self, mock_pubsub
    ):
        """Test that handle_message does NOT call record_pubsub_message for 'pong' type."""

        response = b"PONG"

        with patch(
            "redis.asyncio.client.record_pubsub_message", new_callable=AsyncMock
        ) as mock_record:
            await PubSub.handle_message(
                mock_pubsub, response, ignore_subscribe_messages=False
            )

            mock_record.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.fixed_client
class TestPubSubAcloseWithMocks:
    """
    Regression tests for https://github.com/redis/redis-py/issues/3941 —
    PubSub.aclose() must not hang inside StreamWriter.wait_closed() when a
    concurrent reader task still owns the pubsub connection's transport.
    """

    def _make_pubsub(self):
        pool = MagicMock()
        pool.get_encoder = MagicMock(
            return_value=MagicMock(
                decode_responses=False,
                encode=lambda v: v.encode() if isinstance(v, str) else v,
            )
        )
        pool.release = AsyncMock()
        pubsub = PubSub(connection_pool=pool)
        connection = MagicMock()
        connection.disconnect = AsyncMock()
        connection.deregister_connect_callback = MagicMock()
        pubsub.connection = connection
        return pubsub, pool, connection

    async def test_aclose_disconnects_with_nowait(self):
        """aclose() must call connection.disconnect(nowait=True) to avoid
        awaiting StreamWriter.wait_closed(), which can deadlock when another
        task is blocked in parse_response() on the same socket."""
        pubsub, pool, connection = self._make_pubsub()

        await pubsub.aclose()

        connection.disconnect.assert_awaited_once_with(nowait=True)
        connection.deregister_connect_callback.assert_called_once_with(
            pubsub.on_connect
        )
        pool.release.assert_awaited_once_with(connection)
        assert pubsub.connection is None

    async def test_aclose_does_not_hang_when_wait_closed_would_block(self):
        """
        End-to-end regression: even if the underlying StreamWriter.wait_closed()
        would hang forever (as happens when a concurrent reader still holds the
        transport), aclose() returns promptly because it passes nowait=True to
        disconnect().
        """
        pubsub, _pool, connection = self._make_pubsub()

        async def fake_disconnect(nowait: bool = False, **_):
            # Simulates AbstractConnection.disconnect(): if nowait=False we
            # would hang awaiting wait_closed(); with nowait=True we return
            # immediately.
            if not nowait:
                await asyncio.Event().wait()

        connection.disconnect = AsyncMock(side_effect=fake_disconnect)

        async with async_timeout(2):
            await pubsub.aclose()

        connection.disconnect.assert_awaited_once_with(nowait=True)


class TestClusterPubSubResubscribe:
    """
    Cluster-only tests verifying that sharded pubsub resubscription after a
    reconnect groups channels by slot and does not trigger CROSSSLOT errors
    on nodes that own multiple slots.
    """

    async def _read_ssubscribe_acks(self, pubsub, expected_channels, timeout=3.0):
        acks = {}
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline and len(acks) < len(expected_channels):
            msg = await pubsub.get_sharded_message(timeout=0.5)
            if msg is None:
                continue
            if msg["type"] == "ssubscribe":
                acks[msg["channel"]] = msg
        return acks

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    async def test_resubscribe_shard_channels_different_slots_same_node(self, r):
        """After a reconnect, shard channels on the same node but different
        slots must be resubscribed without raising CROSSSLOT.
        """
        # Hardcoded for the project's default 3-master test cluster; both
        # channels hash to slots owned by the first master.
        ch_a, ch_b = "resub-a-0", "resub-b-0"
        assert r.keyslot(ch_a) != r.keyslot(ch_b)
        assert r.get_node_from_key(ch_a).name == r.get_node_from_key(ch_b).name

        p = r.pubsub()
        try:
            await p.ssubscribe(ch_a)
            assert await self._read_ssubscribe_acks(p, [ch_a], timeout=2.0), (
                "missing initial ssubscribe ack for ch_a"
            )
            await p.ssubscribe(ch_b)
            assert await self._read_ssubscribe_acks(p, [ch_b], timeout=2.0), (
                "missing initial ssubscribe ack for ch_b"
            )

            # Force reconnect of the per-node subscriber connection.
            node = r.get_node_from_key(ch_a)
            per_node_pubsub = p.node_pubsub_mapping[node.name]
            assert per_node_pubsub.connection is not None
            await per_node_pubsub.connection.disconnect()

            # Trigger reconnect + resubscription. With batched resubscription
            # this would raise a CROSSSLOT ResponseError; with per-slot
            # grouping it must succeed and deliver fresh ssubscribe acks.
            acks = await self._read_ssubscribe_acks(p, [ch_a, ch_b], timeout=3.0)
            assert ch_a.encode() in acks
            assert ch_b.encode() in acks

            # Both channels must still be tracked in the aggregate state.
            assert ch_a.encode() in p.shard_channels
            assert ch_b.encode() in p.shard_channels
        finally:
            await p.aclose()


@pytest.mark.fixed_client
class TestClusterPubSubSlotMigration:
    """
    Deterministic unit tests for async ClusterPubSub shard-channel slot
    migration handling. These tests bypass all I/O by mocking the per-node
    PubSub instances and the cluster's node-resolution layer, verifying the
    reconciler logic and the reverse-index routing in isolation (no live
    cluster required).
    """

    def _make_cluster_pubsub(self):
        from redis._parsers.encoders import Encoder
        from redis.asyncio.cluster import ClusterPubSub

        pubsub = ClusterPubSub.__new__(ClusterPubSub)
        # Base PubSub state normally wired up by PubSub.__init__.
        pubsub.encoder = Encoder("utf-8", "strict", False)
        pubsub.connection = None
        pubsub._lock = asyncio.Lock()
        pubsub.subscribed_event = asyncio.Event()
        pubsub.health_check_response_counter = 0
        pubsub.channels = {}
        pubsub.shard_channels = {}
        pubsub.pending_unsubscribe_shard_channels = set()
        pubsub.patterns = {}
        pubsub.ignore_subscribe_messages = False
        # ClusterPubSub-specific state.
        pubsub.cluster = MagicMock()
        pubsub.node_pubsub_mapping = {}
        pubsub._shard_channel_to_node = {}
        pubsub._shard_state_lock = asyncio.Lock()
        pubsub._reconcile_tasks = set()
        pubsub.push_handler_func = None
        return pubsub

    def _make_node(self, name):
        node = MagicMock()
        node.name = name
        return node

    def _make_node_pubsub(self, shard_channels=None):
        p = MagicMock()
        p.shard_channels = dict(shard_channels or {})
        p.pending_unsubscribe_shard_channels = set()
        p.subscribed = True
        p.ssubscribe = AsyncMock()
        p.sunsubscribe = AsyncMock()
        p.get_message = AsyncMock()
        p.aclose = AsyncMock()
        return p

    async def test_reinitialize_moves_channel_to_new_owner(self):
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        await pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name

    async def test_reinitialize_noop_when_owner_unchanged(self):
        pubsub = self._make_cluster_pubsub()
        owner = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        owner_ps = self._make_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping[owner.name] = owner_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: owner.name}
        pubsub.cluster.get_node_from_key.return_value = owner

        await pubsub.reinitialize_shard_subscriptions()

        owner_ps.sunsubscribe.assert_not_awaited()
        owner_ps.ssubscribe.assert_not_awaited()
        assert pubsub._shard_channel_to_node[channel] == owner.name

    async def test_reinitialize_tolerates_old_node_disconnect(self):
        """
        When the old node is still part of the cluster topology but just
        transiently unreachable / slow, the failed sunsubscribe must not
        abort migration, and the old per-node pubsub must be left in
        place so ``PubSub._execute`` can auto-reconnect and
        ``on_connect`` can re-subscribe the remaining channels on the
        next read.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        old_ps.sunsubscribe.side_effect = TimeoutError("old node is slow")
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        # Old node is still known to the cluster (transient error).
        pubsub.cluster.get_node.return_value = old_node

        await pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        # Node still in topology → keep the pubsub so auto-reconnect can
        # recover it; the dedicated socket was already torn down by
        # Connection.disconnect() before the exception surfaced.
        old_ps.aclose.assert_not_awaited()
        assert old_node.name in pubsub.node_pubsub_mapping

    async def test_reinitialize_drops_old_pubsub_when_node_removed_from_topology(self):
        """
        When the failed sunsubscribe coincides with the old node being
        removed from the cluster topology (failover / topology refresh
        dropped it), the pubsub has nowhere to reconnect to and would
        stay in ``node_pubsub_mapping`` with ``subscribed=True`` forever
        (the ACK can never arrive), poisoning the round-robin generator.
        Drop it eagerly in that case.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        old_ps.sunsubscribe.side_effect = ConnectionError("old node is gone")
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node
        # Old node was removed from topology.
        pubsub.cluster.get_node.return_value = None

        await pubsub.reinitialize_shard_subscriptions()

        assert pubsub._shard_channel_to_node[channel] == new_node.name
        old_ps.aclose.assert_awaited_once()
        assert old_node.name not in pubsub.node_pubsub_mapping

    async def test_sunsubscribe_routes_via_reverse_index_after_migration(self):
        """
        After a slot migration, sunsubscribe must hit the node that currently
        holds the subscription (tracked in ``_shard_channel_to_node``) rather
        than re-resolving via ``cluster.get_node_from_key``, which by then
        points to the new owner and would miss the actual subscription.
        """
        pubsub = self._make_cluster_pubsub()
        holding_node_name = "127.0.0.1:7000"
        new_owner = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        holding_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[holding_node_name] = holding_ps
        pubsub.node_pubsub_mapping[new_owner.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: holding_node_name}
        pubsub.cluster.get_node_from_key.return_value = new_owner

        await pubsub.sunsubscribe(channel)

        holding_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.sunsubscribe.assert_not_awaited()

    async def test_ssubscribe_migration_applies_newly_supplied_handler(self):
        """
        Regression: the lazy-reroute branch in ssubscribe must honour the
        caller's newly supplied handler, matching PubSub.ssubscribe()'s
        dict.update() semantics. Previously it fell back to the stale handler
        tracked in ``shard_channels`` and silently dropped the new one.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"foo"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        # Channel was previously subscribed with no handler.
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        new_handler = MagicMock()
        await pubsub.ssubscribe(foo=new_handler)

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(Subscription(channel, new_handler))

    async def test_ssubscribe_migration_accepts_binary_subscription_with_handler(self):
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        channel = b"\x80\x81\x82"
        old_ps = self._make_node_pubsub({channel: None})
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: old_node.name}
        pubsub.cluster.get_node_from_key.return_value = new_node

        new_handler = MagicMock()
        await pubsub.ssubscribe(Subscription(channel, new_handler))

        old_ps.sunsubscribe.assert_awaited_once_with(channel)
        new_ps.ssubscribe.assert_awaited_once_with(Subscription(channel, new_handler))

    async def test_ssubscribe_skips_channel_when_node_resolution_returns_none(self):
        """
        Regression: cluster.get_node_from_key may return None for channels
        whose slot is transiently uncovered. ssubscribe must skip such
        channels rather than dereference None (which would crash on
        ``node.name`` either in the reverse-index comparison or inside
        ``_get_node_pubsub``). Mirrors the sync counterpart's guard.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.cluster.get_node_from_key.return_value = None
        # Stale reverse-index entry also present to exercise both crash paths:
        # the old_name != node.name comparison and the downstream fallthrough.
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}

        await pubsub.ssubscribe(b"foo")

        # No migration, no per-node pubsub creation, no mapping mutation.
        assert pubsub.node_pubsub_mapping == {}
        assert pubsub._shard_channel_to_node == {b"foo": "127.0.0.1:7000"}

    async def test_ssubscribe_initializes_cluster_before_resolving_slot(self):
        """
        Async RedisCluster lazily initializes on first awaited command.
        ClusterPubSub.ssubscribe must honor that contract before it resolves
        the shard channel slot through the cluster's slots cache.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster._initialize = True
        pubsub.cluster.initialize = AsyncMock()
        pubsub.cluster.get_node_from_key.return_value = node

        await pubsub.ssubscribe(b"foo")

        pubsub.cluster.initialize.assert_awaited_once()
        pubsub.cluster.get_node_from_key.assert_called_once_with(b"foo")
        node_ps.ssubscribe.assert_awaited_once_with(b"foo")

    async def test_aclose_clears_state_and_cancels_reconcile_tasks(self):
        """
        Regression: aclose() must clear the ``_shard_channel_to_node`` reverse
        index so a reused instance does not route against stale mappings, and
        must cancel any in-flight reconciliation tasks so they do not race
        with the teardown of per-node pubsubs.
        """
        pubsub = self._make_cluster_pubsub()
        # Populate the reverse index and per-node pubsub mapping.
        node = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: node.name}
        pubsub.node_pubsub_mapping[node.name] = self._make_node_pubsub({channel: None})

        # Schedule a reconcile task that never completes on its own so we can
        # assert it gets cancelled by aclose().
        async def _never():
            await asyncio.sleep(3600)

        reconcile_task = asyncio.create_task(_never())
        pubsub._reconcile_tasks.add(reconcile_task)

        await pubsub.aclose()

        assert pubsub._shard_channel_to_node == {}
        assert pubsub._reconcile_tasks == set()
        assert reconcile_task.cancelled()

    async def test_migration_driven_sunsubscribe_drops_empty_pubsub(self):
        """
        Regression: when a migration-driven sunsubscribe confirmation arrives
        (the channel is not in ``pending_unsubscribe_shard_channels`` because
        migration must not touch cluster-level tracking), the per-node pubsub
        must still be dropped from ``node_pubsub_mapping`` once it no longer
        holds any subscriptions. Otherwise long-running clients with slot
        churn accumulate dead per-node pubsubs and their connections.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        # Old per-node pubsub has already processed the sunsubscribe internally
        # (shard_channels empty, subscribed=False); the message is about to be
        # surfaced to the cluster-level pubsub.
        old_ps = self._make_node_pubsub()
        old_ps.subscribed = False
        old_ps.get_message.return_value = {"type": "sunsubscribe", "channel": channel}
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        # Migration-driven: channel is already tracked on the new owner, so
        # cluster-level state does not reference old_node and the channel is
        # NOT in pending_unsubscribe_shard_channels.
        pubsub.shard_channels = {}
        pubsub._shard_channel_to_node = {}
        pubsub.pending_unsubscribe_shard_channels = set()

        message = await pubsub.get_sharded_message(target_node=old_node)

        assert message is not None
        assert old_node.name not in pubsub.node_pubsub_mapping
        # The per-node pubsub's dedicated connection must be released back
        # to its pool before the mapping drop; otherwise the instance is
        # GC-eligible with no path that closes it (PubSub.__del__ does not
        # release connections), leaking a pool slot per migration.
        old_ps.aclose.assert_awaited_once()

    async def test_reinitialize_partial_progress_when_slot_uncovered(self):
        """
        A SlotNotCoveredError on one channel (slot transiently uncovered
        mid-migration) must not abort the reconcile pass for coverable
        siblings, but it MUST be surfaced at the end so the caller knows
        reconciliation was incomplete and a retry will happen on the next
        slots-cache change notification.
        """
        pubsub = self._make_cluster_pubsub()
        old_node = self._make_node("127.0.0.1:7000")
        new_node = self._make_node("127.0.0.1:7001")
        covered_channel = b"covered"
        uncovered_channel = b"uncovered"
        old_ps = self._make_node_pubsub(
            {covered_channel: None, uncovered_channel: None}
        )
        new_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[old_node.name] = old_ps
        pubsub.node_pubsub_mapping[new_node.name] = new_ps
        pubsub.shard_channels = {covered_channel: None, uncovered_channel: None}
        pubsub._shard_channel_to_node = {
            covered_channel: old_node.name,
            uncovered_channel: old_node.name,
        }

        def _resolve(channel, *args, **kwargs):
            if channel == uncovered_channel:
                raise SlotNotCoveredError('Slot "0" is not covered by the cluster.')
            return new_node

        pubsub.cluster.get_node_from_key.side_effect = _resolve

        with pytest.raises(SlotNotCoveredError):
            await pubsub.reinitialize_shard_subscriptions()

        # covered_channel was migrated before the error surfaced;
        # uncovered_channel is left in place for the next reconcile pass.
        new_ps.ssubscribe.assert_awaited_once_with(covered_channel)
        old_ps.sunsubscribe.assert_awaited_once_with(covered_channel)
        assert pubsub._shard_channel_to_node[covered_channel] == new_node.name
        assert pubsub._shard_channel_to_node[uncovered_channel] == old_node.name

    async def test_on_slots_changed_consumes_and_logs_task_exception(self, caplog):
        """
        Regression: on_slots_changed schedules reinitialize_shard_subscriptions
        as a fire-and-forget task. When that coroutine raises (e.g. the Option D
        SlotNotCoveredError signalling an incomplete reconcile pass), the
        exception must be consumed and logged so Python does not emit a noisy
        "Task exception was never retrieved" warning at GC time, and so the
        incomplete-reconcile signal flows through the library logger — matching
        the sync path, where ClusterPubSubSlotsCacheListener already logs via
        logger.exception.
        """
        pubsub = self._make_cluster_pubsub()
        # Has at least one shard subscription so on_slots_changed actually
        # schedules a task (the method short-circuits when shard_channels is
        # empty).
        pubsub.shard_channels = {b"foo": None}
        boom = SlotNotCoveredError("simulated transient uncovered slot")

        async def _raise():
            raise boom

        with mock.patch.object(
            pubsub, "reinitialize_shard_subscriptions", side_effect=_raise
        ):
            with caplog.at_level(logging.ERROR, logger="redis.asyncio.cluster"):
                await pubsub.on_slots_changed()
                # Await the scheduled task directly so both its done-callbacks
                # run (exception consumption + set discard). Suppress here so
                # the test itself does not re-raise what the callback already
                # logged.
                assert len(pubsub._reconcile_tasks) == 1
                (task,) = list(pubsub._reconcile_tasks)
                try:
                    await task
                except SlotNotCoveredError:
                    pass
                # Yield once more so the done-callbacks (scheduled via
                # call_soon) actually execute.
                await asyncio.sleep(0)

        # Task is gone from the tracking set (discard callback ran).
        assert pubsub._reconcile_tasks == set()
        # Exception was surfaced via the library logger.
        assert any(
            "shard subscription reconciliation failed" in rec.message
            and rec.levelno == logging.ERROR
            for rec in caplog.records
        )
