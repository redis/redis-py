import logging
import platform
import queue
import socket
import threading
import time
from collections import defaultdict
from unittest import mock
from unittest.mock import patch

import pytest
import redis
from redis._parsers import Encoder
from redis.client import PubSub
from redis.cluster import ClusterPubSub
from redis.crc import key_slot
from redis.event import EventDispatcher
from redis.exceptions import ConnectionError, SlotNotCoveredError, TimeoutError
from redis.observability import recorder
from redis.observability.config import OTelConfig, MetricGroup
from redis.observability.metrics import RedisMetricsCollector

from .conftest import (
    _get_client,
    is_resp2_connection,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
)


def wait_for_message(
    pubsub, timeout=0.5, ignore_subscribe_messages=False, node=None, func=None
):
    now = time.monotonic()
    timeout = now + timeout
    while now < timeout:
        if node:
            message = pubsub.get_sharded_message(
                ignore_subscribe_messages=ignore_subscribe_messages, target_node=node
            )
        elif func:
            message = func(ignore_subscribe_messages=ignore_subscribe_messages)
        else:
            message = pubsub.get_message(
                ignore_subscribe_messages=ignore_subscribe_messages
            )
        if message is not None:
            return message
        time.sleep(0.01)
        now = time.monotonic()
    return None


def make_message(type, channel, data, pattern=None):
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


class TestPubSubSubscribeUnsubscribe:
    def _test_subscribe_unsubscribe(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert wait_for_message(p) == make_message(sub_type, key, i + 1)

        for key in keys:
            assert unsub_func(key) is None

        # should be a message for each channel/pattern we just unsubscribed
        # from
        for i, key in enumerate(keys):
            i = len(keys) - 1 - i
            assert wait_for_message(p) == make_message(unsub_type, key, i)

    def test_channel_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_subscribe_unsubscribe(**kwargs)

    def test_pattern_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_subscribe_unsubscribe_cluster(self, r):
        node_channels = defaultdict(int)
        p = r.pubsub()
        keys = {
            "foo": r.get_node_from_key("foo"),
            "bar": r.get_node_from_key("bar"),
            "uni" + chr(4456) + "code": r.get_node_from_key("uni" + chr(4456) + "code"),
        }

        for key, node in keys.items():
            assert p.ssubscribe(key) is None

        # should be a message for each shard_channel we just subscribed to
        for key, node in keys.items():
            node_channels[node.name] += 1
            assert wait_for_message(p, node=node) == make_message(
                "ssubscribe", key, node_channels[node.name]
            )

        for key in keys.keys():
            assert p.sunsubscribe(key) is None

        # should be a message for each shard_channel we just unsubscribed
        # from
        for key, node in keys.items():
            node_channels[node.name] -= 1
            assert wait_for_message(p, node=node) == make_message(
                "sunsubscribe", key, node_channels[node.name]
            )

    def _test_resubscribe_on_reconnection(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        for key in keys:
            assert sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert wait_for_message(p) == make_message(sub_type, key, i + 1)

        # manually disconnect
        p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        # note, we may not re-subscribe to channels in exactly the same order
        # so we have to do some extra checks to make sure we got them all
        messages = []
        for i in range(len(keys)):
            messages.append(wait_for_message(p))

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

    def test_resubscribe_to_channels_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.onlynoncluster
    def test_resubscribe_to_patterns_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_resubscribe_to_shard_channels_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.onlynoncluster
    def test_resubscribe_binary_channel_on_reconnection(self, r):
        """Binary channel names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        # b'\x80\x81\x82' is deliberately invalid UTF-8
        binary_channel = b"\x80\x81\x82"
        p = r.pubsub()
        p.subscribe(binary_channel)
        assert wait_for_message(p) is not None  # consume subscribe ack

        # force reconnect
        p.connection.disconnect()

        # get_message triggers on_connect → re-subscribe; must not raise
        messages = []
        for _ in range(1):
            message = wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "subscribe"
        assert messages[0]["channel"] == binary_channel

    @pytest.mark.onlynoncluster
    def test_resubscribe_binary_pattern_on_reconnection(self, r):
        """Binary pattern names that are not valid UTF-8 must survive
        reconnection without raising ``UnicodeDecodeError``.
        See https://github.com/redis/redis-py/issues/3912
        """
        binary_pattern = b"\x80\x81*"
        p = r.pubsub()
        p.psubscribe(binary_pattern)
        assert wait_for_message(p) is not None  # consume psubscribe ack

        # force reconnect
        p.connection.disconnect()

        messages = []
        for _ in range(1):
            message = wait_for_message(p)
            assert message is not None
            messages.append(message)

        assert len(messages) == 1
        assert messages[0]["type"] == "psubscribe"
        assert messages[0]["channel"] == binary_pattern

    @pytest.mark.fixed_client
    def test_cluster_pubsub_resubscribe_shard_channels_groups_by_slot(self):
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
        pool = mock.MagicMock()
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
        with mock.patch.object(
            p,
            "ssubscribe",
            side_effect=lambda *a, **kw: calls.append((tuple(a), dict(kw))),
        ):
            ClusterPubSub._resubscribe_shard_channels(p)

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

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_standalone_pubsub_on_connect_batches_shard_channels(self, r):
        """The standalone PubSub has no notion of slots and must keep
        batching every tracked shard channel into a single SSUBSCRIBE call
        on reconnect, as it did before cluster-aware resubscription existed.
        """
        p = r.pubsub()
        ch_a = b"{slot-a}-one"
        ch_b = b"{slot-b}-one"
        assert key_slot(ch_a) != key_slot(ch_b)

        p.shard_channels = {ch_a: None, ch_b: None}

        calls = []
        with mock.patch.object(
            p,
            "ssubscribe",
            side_effect=lambda *a, **kw: calls.append((tuple(a), dict(kw))),
        ):
            p.on_connect(connection=None)

        assert len(calls) == 1
        args, kwargs = calls[0]
        assert set(args) == {ch_a, ch_b}
        assert kwargs == {}
        p.close()

    def _test_subscribed_property(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        assert p.subscribed is False
        sub_func(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, keys[0], 1)
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all channels
        unsub_func()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        sub_func(keys[0])
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, keys[0], 1)

        # unsubscribe again
        unsub_func()
        assert p.subscribed is True
        # subscribe to another channel before reading the unsubscribe response
        sub_func(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert wait_for_message(p) == make_message(sub_type, keys[1], 1)
        unsub_func()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(unsub_type, keys[1], 0)
        # now we're finally unsubscribed
        assert p.subscribed is False

    def test_subscribe_property_with_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    def test_subscribe_property_with_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_subscribed_property(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_subscribe_property_with_shard_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_subscribed_property(**kwargs)

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_subscribe_property_with_shard_channels_cluster(self, r):
        p = r.pubsub()
        keys = ["foo", "bar", "uni" + chr(4456) + "code"]
        nodes = [r.get_node_from_key(key) for key in keys]
        assert p.subscribed is False
        p.ssubscribe(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "ssubscribe", keys[0], 1
        )
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all shard_channels
        p.sunsubscribe()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "sunsubscribe", keys[0], 0
        )
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        p.ssubscribe(keys[0])
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "ssubscribe", keys[0], 1
        )

        # unsubscribe again
        p.sunsubscribe()
        assert p.subscribed is True
        # subscribe to another shard_channel before reading the unsubscribe response
        p.ssubscribe(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert wait_for_message(p, node=nodes[0]) == make_message(
            "sunsubscribe", keys[0], 0
        )
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert wait_for_message(p, node=nodes[1]) == make_message(
            "ssubscribe", keys[1], 1
        )
        p.sunsubscribe()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert wait_for_message(p, node=nodes[1]) == make_message(
            "sunsubscribe", keys[1], 0
        )
        # now we're finally unsubscribed
        assert p.subscribed is False

    @skip_if_server_version_lt("7.0.0")
    def test_ignore_all_subscribe_messages(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)

        checks = (
            (p.subscribe, "foo", p.get_message),
            (p.unsubscribe, "foo", p.get_message),
            (p.psubscribe, "f*", p.get_message),
            (p.punsubscribe, "f*", p.get_message),
            (p.ssubscribe, "foo", p.get_sharded_message),
            (p.sunsubscribe, "foo", p.get_sharded_message),
        )

        assert p.subscribed is False
        for func, channel, get_func in checks:
            assert func(channel) is None
            assert p.subscribed is True
            assert wait_for_message(p, func=get_func) is None
        assert p.subscribed is False

    @skip_if_server_version_lt("7.0.0")
    def test_ignore_individual_subscribe_messages(self, r):
        p = r.pubsub()

        checks = (
            (p.subscribe, "foo", p.get_message),
            (p.unsubscribe, "foo", p.get_message),
            (p.psubscribe, "f*", p.get_message),
            (p.punsubscribe, "f*", p.get_message),
            (p.ssubscribe, "foo", p.get_sharded_message),
            (p.sunsubscribe, "foo", p.get_sharded_message),
        )

        assert p.subscribed is False
        for func, channel, get_func in checks:
            assert func(channel) is None
            assert p.subscribed is True
            message = wait_for_message(p, ignore_subscribe_messages=True, func=get_func)
            assert message is None
        assert p.subscribed is False

    def test_sub_unsub_resub_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    def test_sub_unsub_resub_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_sub_unsub_resub(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_resub_shard_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_sub_unsub_resub(**kwargs)

    def _test_sub_unsub_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        sub_func(key)
        unsub_func(key)
        sub_func(key)
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert wait_for_message(p) == make_message(unsub_type, key, 0)
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_resub_shard_channels_cluster(self, r):
        p = r.pubsub()
        key = "foo"
        p.ssubscribe(key)
        p.sunsubscribe(key)
        p.ssubscribe(key)
        assert p.subscribed is True
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "sunsubscribe", key, 0
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert p.subscribed is True

    def test_sub_unsub_all_resub_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "channel")
        self._test_sub_unsub_all_resub(**kwargs)

    def test_sub_unsub_all_resub_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "pattern")
        self._test_sub_unsub_all_resub(**kwargs)

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_all_resub_shard_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), "shard_channel")
        self._test_sub_unsub_all_resub(**kwargs)

    def _test_sub_unsub_all_resub(
        self, p, sub_type, unsub_type, sub_func, unsub_func, keys
    ):
        # https://github.com/andymccurdy/redis-py/issues/764
        key = keys[0]
        sub_func(key)
        unsub_func()
        sub_func(key)
        assert p.subscribed is True
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert wait_for_message(p) == make_message(unsub_type, key, 0)
        assert wait_for_message(p) == make_message(sub_type, key, 1)
        assert p.subscribed is True

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_sub_unsub_all_resub_shard_channels_cluster(self, r):
        p = r.pubsub()
        key = "foo"
        p.ssubscribe(key)
        p.sunsubscribe()
        p.ssubscribe(key)
        assert p.subscribed is True
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "sunsubscribe", key, 0
        )
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", key, 1
        )
        assert p.subscribed is True


class TestPubSubMessages:
    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    def test_published_message_to_channel(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert r.publish("foo", "test message") == 1

        message = wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("message", "foo", "test message")

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_published_message_to_shard_channel(self, r):
        p = r.pubsub()
        p.ssubscribe("foo")
        assert wait_for_message(p) == make_message("ssubscribe", "foo", 1)
        assert r.spublish("foo", "test message") == 1

        message = wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message("smessage", "foo", "test message")

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_published_message_to_shard_channel_cluster(self, r):
        p = r.pubsub()
        p.ssubscribe("foo")
        assert wait_for_message(p, func=p.get_sharded_message) == make_message(
            "ssubscribe", "foo", 1
        )
        assert r.spublish("foo", "test message") == 1

        message = wait_for_message(p, func=p.get_sharded_message)
        assert isinstance(message, dict)
        assert message == make_message("smessage", "foo", "test message")

    def test_published_message_to_pattern(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        p.psubscribe("f*")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert wait_for_message(p) == make_message("psubscribe", "f*", 2)
        # 1 to pattern, 1 to channel
        assert r.publish("foo", "test message") == 2

        message1 = wait_for_message(p)
        message2 = wait_for_message(p)
        assert isinstance(message1, dict)
        assert isinstance(message2, dict)

        expected = [
            make_message("message", "foo", "test message"),
            make_message("pmessage", "foo", "test message", pattern="f*"),
        ]

        assert message1 in expected
        assert message2 in expected
        assert message1 != message2

    def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(foo=self.message_handler)
        assert wait_for_message(p) is None
        assert r.publish("foo", "test message") == 1
        assert wait_for_message(p) is None
        assert self.message == make_message("message", "foo", "test message")

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.ssubscribe(foo=self.message_handler)
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert r.spublish("foo", "test message") == 1
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert self.message == make_message("smessage", "foo", "test message")

    @pytest.mark.onlynoncluster
    def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(**{"f*": self.message_handler})
        assert wait_for_message(p) is None
        assert r.publish("foo", "test message") == 1
        assert wait_for_message(p) is None
        assert self.message == make_message(
            "pmessage", "foo", "test message", pattern="f*"
        )

    def test_unicode_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        p.subscribe(**channels)
        assert wait_for_message(p) is None
        assert r.publish(channel, "test message") == 1
        assert wait_for_message(p) is None
        assert self.message == make_message("message", channel, "test message")

    @skip_if_server_version_lt("7.0.0")
    def test_unicode_shard_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = "uni" + chr(4456) + "code"
        channels = {channel: self.message_handler}
        p.ssubscribe(**channels)
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert r.spublish(channel, "test message") == 1
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert self.message == make_message("smessage", channel, "test message")

    @pytest.mark.onlynoncluster
    # see: https://redis.readthedocs.io/en/stable/clustering.html#known-pubsub-limitations
    def test_unicode_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = "uni" + chr(4456) + "*"
        channel = "uni" + chr(4456) + "code"
        p.psubscribe(**{pattern: self.message_handler})
        assert wait_for_message(p) is None
        assert r.publish(channel, "test message") == 1
        assert wait_for_message(p) is None
        assert self.message == make_message(
            "pmessage", channel, "test message", pattern=pattern
        )


class TestPubSubRESP3Handler:
    def my_handler(self, message):
        self.message = ["my handler", message]

    def test_push_handler(self, r):
        if is_resp2_connection(r):
            return
        p = r.pubsub(push_handler_func=self.my_handler)
        p.subscribe("foo")
        assert wait_for_message(p) is None
        assert self.message == ["my handler", [b"subscribe", b"foo", 1]]
        assert r.publish("foo", "test message") == 1
        assert wait_for_message(p) is None
        assert self.message == ["my handler", [b"message", b"foo", b"test message"]]

    @skip_if_server_version_lt("7.0.0")
    def test_push_handler_sharded_pubsub(self, r):
        if is_resp2_connection(r):
            return
        p = r.pubsub(push_handler_func=self.my_handler)
        p.ssubscribe("foo")
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert self.message == ["my handler", [b"ssubscribe", b"foo", 1]]
        assert r.spublish("foo", "test message") == 1
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert self.message == ["my handler", [b"smessage", b"foo", b"test message"]]


class TestPubSubAutoDecoding:
    "These tests only validate that we get unicode values back"

    channel = "uni" + chr(4456) + "code"
    pattern = "uni" + chr(4456) + "*"
    data = "abc" + chr(4458) + "123"

    def make_message(self, type, channel, data, pattern=None):
        return {"type": type, "channel": channel, "pattern": pattern, "data": data}

    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    @pytest.fixture()
    def r(self, request):
        return _get_client(redis.Redis, request=request, decode_responses=True)

    def test_channel_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.subscribe(self.channel)
        assert wait_for_message(p) == self.make_message("subscribe", self.channel, 1)

        p.unsubscribe(self.channel)
        assert wait_for_message(p) == self.make_message("unsubscribe", self.channel, 0)

    def test_pattern_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.psubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message("psubscribe", self.pattern, 1)

        p.punsubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message("punsubscribe", self.pattern, 0)

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.ssubscribe(self.channel)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "ssubscribe", self.channel, 1
        )

        p.sunsubscribe(self.channel)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "sunsubscribe", self.channel, 0
        )

    def test_channel_publish(self, r):
        p = r.pubsub()
        p.subscribe(self.channel)
        assert wait_for_message(p) == self.make_message("subscribe", self.channel, 1)
        r.publish(self.channel, self.data)
        assert wait_for_message(p) == self.make_message(
            "message", self.channel, self.data
        )

    @pytest.mark.onlynoncluster
    def test_pattern_publish(self, r):
        p = r.pubsub()
        p.psubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message("psubscribe", self.pattern, 1)
        r.publish(self.channel, self.data)
        assert wait_for_message(p) == self.make_message(
            "pmessage", self.channel, self.data, pattern=self.pattern
        )

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_publish(self, r):
        p = r.pubsub()
        p.ssubscribe(self.channel)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "ssubscribe", self.channel, 1
        )
        r.spublish(self.channel, self.data)
        assert wait_for_message(p, func=p.get_sharded_message) == self.make_message(
            "smessage", self.channel, self.data
        )

    def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(**{self.channel: self.message_handler})
        assert wait_for_message(p) is None
        r.publish(self.channel, self.data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message("message", self.channel, self.data)

        # test that we reconnected to the correct channel
        self.message = None
        p.connection.disconnect()
        assert wait_for_message(p) is None  # should reconnect
        new_data = self.data + "new data"
        r.publish(self.channel, new_data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message("message", self.channel, new_data)

    def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(**{self.pattern: self.message_handler})
        assert wait_for_message(p) is None
        r.publish(self.channel, self.data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message(
            "pmessage", self.channel, self.data, pattern=self.pattern
        )

        # test that we reconnected to the correct pattern
        self.message = None
        p.connection.disconnect()
        assert wait_for_message(p) is None  # should reconnect
        new_data = self.data + "new data"
        r.publish(self.channel, new_data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message(
            "pmessage", self.channel, new_data, pattern=self.pattern
        )

    @skip_if_server_version_lt("7.0.0")
    def test_shard_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.ssubscribe(**{self.channel: self.message_handler})
        assert wait_for_message(p, func=p.get_sharded_message) is None
        r.spublish(self.channel, self.data)
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert self.message == self.make_message("smessage", self.channel, self.data)

        # test that we reconnected to the correct channel
        self.message = None
        try:
            # cluster mode
            p.disconnect()
        except AttributeError:
            # standalone mode
            p.connection.disconnect()
        # should reconnect
        assert wait_for_message(p, func=p.get_sharded_message) is None
        new_data = self.data + "new data"
        r.spublish(self.channel, new_data)
        assert wait_for_message(p, func=p.get_sharded_message) is None
        assert self.message == self.make_message("smessage", self.channel, new_data)

    def test_context_manager(self, r):
        with r.pubsub() as pubsub:
            pubsub.subscribe("foo")
            assert pubsub.connection is not None

        assert pubsub.connection is None
        assert pubsub.channels == {}
        assert pubsub.patterns == {}


class TestPubSubRedisDown:
    def test_channel_subscribe(self, r):
        r = redis.Redis(host="localhost", port=6390)
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            p.subscribe("foo")


class TestPubSubSubcommands:
    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_pubsub_channels(self, r):
        p = r.pubsub()
        p.subscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert wait_for_message(p)["type"] == "subscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all([channel in r.pubsub_channels() for channel in expected])

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("7.0.0")
    def test_pubsub_shardchannels(self, r):
        p = r.pubsub()
        p.ssubscribe("foo", "bar", "baz", "quux")
        for i in range(4):
            assert wait_for_message(p)["type"] == "ssubscribe"
        expected = [b"bar", b"baz", b"foo", b"quux"]
        assert all([channel in r.pubsub_shardchannels() for channel in expected])

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_pubsub_shardchannels_cluster(self, r):
        channels = {
            b"foo": r.get_node_from_key("foo"),
            b"bar": r.get_node_from_key("bar"),
            b"baz": r.get_node_from_key("baz"),
            b"quux": r.get_node_from_key("quux"),
        }
        p = r.pubsub()
        p.ssubscribe("foo", "bar", "baz", "quux")
        for node in channels.values():
            assert wait_for_message(p, node=node)["type"] == "ssubscribe"
        for channel, node in channels.items():
            assert channel in r.pubsub_shardchannels(target_nodes=node)
        assert all(
            [
                channel in r.pubsub_shardchannels(target_nodes="all")
                for channel in channels.keys()
            ]
        )

    @pytest.mark.onlynoncluster
    @skip_if_server_version_lt("2.8.0")
    def test_pubsub_numsub(self, r):
        p1 = r.pubsub()
        p1.subscribe("foo", "bar", "baz")
        for i in range(3):
            assert wait_for_message(p1)["type"] == "subscribe"
        p2 = r.pubsub()
        p2.subscribe("bar", "baz")
        for i in range(2):
            assert wait_for_message(p2)["type"] == "subscribe"
        p3 = r.pubsub()
        p3.subscribe("baz")
        assert wait_for_message(p3)["type"] == "subscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert r.pubsub_numsub("foo", "bar", "baz") == channels

    @skip_if_server_version_lt("2.8.0")
    def test_pubsub_numpat(self, r):
        p = r.pubsub()
        p.psubscribe("*oo", "*ar", "b*z")
        for i in range(3):
            assert wait_for_message(p)["type"] == "psubscribe"
        assert r.pubsub_numpat() == 3

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_pubsub_shardnumsub(self, r):
        channels = {
            b"foo": r.get_node_from_key("foo"),
            b"bar": r.get_node_from_key("bar"),
            b"baz": r.get_node_from_key("baz"),
        }
        p1 = r.pubsub()
        p1.ssubscribe(*channels.keys())
        for node in channels.values():
            assert wait_for_message(p1, node=node)["type"] == "ssubscribe"
        p2 = r.pubsub()
        p2.ssubscribe("bar", "baz")
        for i in range(2):
            assert (
                wait_for_message(p2, func=p2.get_sharded_message)["type"]
                == "ssubscribe"
            )
        p3 = r.pubsub()
        p3.ssubscribe("baz")
        assert wait_for_message(p3, node=channels[b"baz"])["type"] == "ssubscribe"

        channels = [(b"foo", 1), (b"bar", 2), (b"baz", 3)]
        assert r.pubsub_shardnumsub("foo", "bar", "baz", target_nodes="all") == channels

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_ssubscribe_multiple_channels_different_nodes(self, r):
        """
        Test subscribing to multiple sharded channels on different nodes.
        Validates that the generator properly handles multiple node_pubsub_mapping entries.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"

        # Subscribe to first channel
        pubsub.ssubscribe(channel1)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Subscribe to second channel (likely different node)
        pubsub.ssubscribe(channel2)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Verify both channels are in shard_channels
        assert channel1.encode() in pubsub.shard_channels
        assert channel2.encode() in pubsub.shard_channels

        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_ssubscribe_multiple_channels_publish_and_read(self, r):
        """
        Test publishing to multiple sharded channels and reading messages.
        Validates that _sharded_message_generator properly cycles through
        multiple node_pubsub_mapping entries.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"
        msg1_data = "message-1"
        msg2_data = "message-2"

        # Subscribe to both channels
        pubsub.ssubscribe(channel1, channel2)

        # Read subscription confirmations
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            assert msg is not None
            assert msg["type"] == "ssubscribe"

        # Publish messages to both channels
        r.spublish(channel1, msg1_data)
        r.spublish(channel2, msg2_data)

        # Read messages - should get both messages
        messages = []
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            assert msg is not None
            assert msg["type"] == "smessage"
            messages.append(msg)

        # Verify we got messages from both channels
        channels_received = {msg["channel"] for msg in messages}
        assert channel1.encode() in channels_received
        assert channel2.encode() in channels_received

        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_generator_handles_concurrent_mapping_changes(self, r):
        """
        Test that the generator properly handles mapping changes during iteration.
        This validates the fix for the RuntimeError: dictionary changed size during iteration.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"

        # Subscribe to first channel
        pubsub.ssubscribe(channel1)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Get initial mapping size (cluster pubsub only)
        assert hasattr(pubsub, "node_pubsub_mapping"), "Test requires ClusterPubSub"
        initial_size = len(pubsub.node_pubsub_mapping)

        # Subscribe to second channel (modifies mapping during potential iteration)
        pubsub.ssubscribe(channel2)
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Verify mapping was updated
        assert len(pubsub.node_pubsub_mapping) >= initial_size

        # Publish and read messages - should not raise RuntimeError
        r.spublish(channel1, "msg1")
        r.spublish(channel2, "msg2")

        messages_received = 0
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            if msg and msg["type"] == "smessage":
                messages_received += 1

        assert messages_received == 2
        pubsub.close()


class TestPubSubPings:
    @skip_if_server_version_lt("3.0.0")
    def test_send_pubsub_ping(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("foo")
        p.ping()
        assert wait_for_message(p) == make_message(
            type="pong", channel=None, data="", pattern=None
        )

    @skip_if_server_version_lt("3.0.0")
    def test_send_pubsub_ping_message(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe("foo")
        p.ping(message="hello world")
        assert wait_for_message(p) == make_message(
            type="pong", channel=None, data="hello world", pattern=None
        )


@pytest.mark.onlynoncluster
class TestPubSubHealthCheckResponse:
    """Tests for health check response validation with different decode_responses settings"""

    def test_is_health_check_response_decode_false_list_format(self, r):
        """Test is_health_check_response recognizes list format with decode_responses=False"""
        p = r.pubsub()
        # List format: [b"pong", b"redis-py-health-check"]
        assert p.is_health_check_response([b"pong", b"redis-py-health-check"])

    def test_is_health_check_response_decode_false_bytes_format(self, r):
        """Test is_health_check_response recognizes bytes format with decode_responses=False"""
        p = r.pubsub()
        # Bytes format: b"redis-py-health-check"
        assert p.is_health_check_response(b"redis-py-health-check")

    def test_is_health_check_response_decode_false_rejects_string(self, r):
        """Test is_health_check_response rejects string format with decode_responses=False"""
        p = r.pubsub()
        # String format should NOT be recognized when decode_responses=False
        assert not p.is_health_check_response("redis-py-health-check")

    def test_is_health_check_response_decode_true_list_format(self, request):
        """Test is_health_check_response recognizes list format with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # List format: ["pong", "redis-py-health-check"]
        assert p.is_health_check_response(["pong", "redis-py-health-check"])

    def test_is_health_check_response_decode_true_string_format(self, request):
        """Test is_health_check_response recognizes string format with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # String format: "redis-py-health-check" (THE FIX!)
        assert p.is_health_check_response("redis-py-health-check")

    def test_is_health_check_response_decode_true_rejects_bytes(self, request):
        """Test is_health_check_response rejects bytes format with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # Bytes format should NOT be recognized when decode_responses=True
        assert not p.is_health_check_response(b"redis-py-health-check")

    def test_is_health_check_response_decode_true_rejects_invalid(self, request):
        """Test is_health_check_response rejects invalid responses with decode_responses=True"""
        r = _get_client(redis.Redis, request, decode_responses=True)
        p = r.pubsub()
        # Invalid responses should be rejected
        assert not p.is_health_check_response("invalid-response")
        assert not p.is_health_check_response(["pong", "invalid-response"])
        assert not p.is_health_check_response(None)

    def test_is_health_check_response_decode_false_rejects_invalid(self, r):
        """Test is_health_check_response rejects invalid responses with decode_responses=False"""
        p = r.pubsub()
        # Invalid responses should be rejected
        assert not p.is_health_check_response(b"invalid-response")
        assert not p.is_health_check_response([b"pong", b"invalid-response"])
        assert not p.is_health_check_response(None)


@pytest.mark.onlynoncluster
class TestPubSubConnectionKilled:
    @skip_if_server_version_lt("3.0.0")
    @skip_if_redis_enterprise()
    def test_connection_error_raised_when_connection_dies(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        for client in r.client_list():
            if client["cmd"] == "subscribe":
                r.client_kill_filter(_id=client["id"])
        with pytest.raises(ConnectionError):
            wait_for_message(p)


class TestPubSubTimeouts:
    def test_get_message_with_timeout_returns_none(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert wait_for_message(p) == make_message("subscribe", "foo", 1)
        assert p.get_message(timeout=0.01) is None

    def test_get_message_not_subscribed_return_none(self, r):
        p = r.pubsub()
        assert p.subscribed is False
        assert p.get_message() is None
        assert p.get_message(timeout=0.1) is None
        with patch.object(threading.Event, "wait") as mock:
            mock.return_value = False
            assert p.get_message(timeout=0.01) is None
            assert mock.called

    def test_get_message_subscribe_during_waiting(self, r):
        p = r.pubsub()

        def poll(ps, expected_res):
            assert ps.get_message() is None
            message = ps.get_message(timeout=1)
            assert message == expected_res

        subscribe_response = make_message("subscribe", "foo", 1)
        poller = threading.Thread(target=poll, args=(p, subscribe_response))
        poller.start()
        time.sleep(0.2)
        p.subscribe("foo")
        poller.join()

    def test_get_message_wait_for_subscription_not_being_called(self, r):
        p = r.pubsub()
        p.subscribe("foo")
        assert p.subscribed is True

        # Ensure p has the event attribute your wait_for_message would call:
        ev = getattr(p, "subscribed_event", None)

        assert ev is not None, (
            "PubSub event attribute not found (check redis-py version)"
        )

        with patch.object(ev, "wait") as mock:
            assert wait_for_message(p) == make_message("subscribe", "foo", 1)
            assert mock.called is False


class TestPubSubWorkerThread:
    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy", reason="Pypy threading issue"
    )
    def test_pubsub_worker_thread_exception_handler(self, r):
        event = threading.Event()

        def exception_handler(ex, pubsub, thread):
            thread.stop()
            event.set()

        p = r.pubsub()
        p.subscribe(**{"foo": lambda m: m})
        pubsub_thread = None
        try:
            with mock.patch.object(p, "get_message", side_effect=Exception("error")):
                pubsub_thread = p.run_in_thread(
                    daemon=True, exception_handler=exception_handler
                )

                assert event.wait(timeout=1.0)
        finally:
            if pubsub_thread is not None:
                pubsub_thread.stop()
                pubsub_thread.join(timeout=1.0)
        assert not pubsub_thread.is_alive()


class TestPubSubDeadlock:
    @pytest.mark.timeout(30, method="thread")
    def test_pubsub_deadlock(self, master_host):
        pool = redis.ConnectionPool(host=master_host[0], port=master_host[1])
        r = redis.Redis(connection_pool=pool)

        for i in range(60):
            p = r.pubsub()
            p.subscribe("my-channel-1", "my-channel-2")
            pool.reset()


@pytest.mark.timeout(5, method="thread")
@pytest.mark.parametrize("method", ["get_message", "listen"])
@pytest.mark.onlynoncluster
class TestPubSubAutoReconnect:
    def mysetup(self, r, method):
        self.messages = queue.Queue()
        self.pubsub = r.pubsub()
        self.state = 0
        self.cond = threading.Condition()
        if method == "get_message":
            self.get_message = self.loop_step_get_message
        else:
            self.get_message = self.loop_step_listen

        self.thread = threading.Thread(target=self.loop)
        self.thread.daemon = True
        self.thread.start()
        # get the initial connect message
        message = self.messages.get(timeout=1)
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    def wait_for_reconnect(self):
        self.cond.wait_for(lambda: self.pubsub.connection._sock is not None, timeout=2)
        assert self.pubsub.connection._sock is not None  # we didn't time out
        assert self.state == 3

        message = self.messages.get(timeout=1)
        assert message == {
            "channel": b"foo",
            "data": 1,
            "pattern": None,
            "type": "subscribe",
        }

    def mycleanup(self):
        # kill thread
        with self.cond:
            self.state = 4  # quit
            self.cond.notify()
        self.thread.join()

    def test_reconnect_socket_error(self, r: redis.Redis, method):
        """
        Test that a socket error will cause reconnect
        """
        self.mysetup(r, method)
        try:
            # now, disconnect the connection, and wait for it to be re-established
            with self.cond:
                self.state = 1
                with mock.patch.object(self.pubsub.connection, "_parser") as mockobj:
                    mockobj.read_response.side_effect = socket.error
                    mockobj.can_read.side_effect = socket.error
                    # wait until thread notices the disconnect until we undo the patch
                    self.cond.wait_for(lambda: self.state >= 2)
                    assert (
                        self.pubsub.connection._sock is None
                    )  # it is in a disconnected state
                self.wait_for_reconnect()

        finally:
            self.mycleanup()

    def test_reconnect_disconnect(self, r: redis.Redis, method):
        """
        Test that a manual disconnect() will cause reconnect
        """
        self.mysetup(r, method)
        try:
            # now, disconnect the connection, and wait for it to be re-established
            with self.cond:
                self.state = 1
                self.pubsub.connection.disconnect()
                assert self.pubsub.connection._sock is None
                # wait for reconnect
                self.wait_for_reconnect()
        finally:
            self.mycleanup()

    def loop(self):
        # reader loop, performing state transitions as it
        # discovers disconnects and reconnects
        self.pubsub.subscribe("foo")
        while True:
            time.sleep(0.01)  # give main thread chance to get lock
            with self.cond:
                old_state = self.state
                try:
                    if self.state == 4:
                        break
                    # print ('state, %s, sock %s' % (state, pubsub.connection._sock))
                    got_msg = self.get_message()
                    assert got_msg
                    if self.state in (1, 2):
                        self.state = 3  # successful reconnect
                except redis.ConnectionError:
                    assert self.state in (1, 2)
                    self.state = 2
                finally:
                    self.cond.notify()
                # assert that we noticed a connect error, or automatically
                # reconnected without error
                if old_state == 1:
                    assert self.state in (2, 3)

    def loop_step_get_message(self):
        # get a single message via listen()
        message = self.pubsub.get_message(timeout=0.1)
        if message is not None:
            self.messages.put(message)
            return True
        return False

    def loop_step_listen(self):
        # get a single message via listen()
        for message in self.pubsub.listen():
            self.messages.put(message)
            return True


@pytest.mark.onlynoncluster
class TestBaseException:
    def test_base_exception(self, r: redis.Redis):
        """
        Manually trigger a BaseException inside the parser's .read_response method
        and verify that it isn't caught
        """
        pubsub = r.pubsub()
        pubsub.subscribe("foo")

        def is_connected():
            return pubsub.connection._sock is not None

        assert is_connected()

        def get_msg():
            # blocking method to return messages
            while True:
                response = pubsub.parse_response(block=True)
                message = pubsub.handle_message(
                    response, ignore_subscribe_messages=False
                )
                if message is not None:
                    return message

        # get subscribe message
        msg = get_msg()
        assert msg is not None
        # timeout waiting for another message which never arrives
        assert is_connected()
        with (
            patch("redis._parsers._RESP2Parser.read_response") as mock1,
            patch("redis._parsers._HiredisParser.read_response") as mock2,
            patch("redis._parsers._RESP3Parser.read_response") as mock3,
        ):
            mock1.side_effect = BaseException("boom")
            mock2.side_effect = BaseException("boom")
            mock3.side_effect = BaseException("boom")

            with pytest.raises(BaseException):
                get_msg()

        # the timeout on the read should not cause disconnect
        assert is_connected()


class TestPubSubMetricsRecording:
    """
    Unit tests that verify metrics are properly recorded from PubSub operations
    through the direct record_* function calls.

    These tests use fully mocked connection and connection pool - no real Redis
    or OTel integration is used.
    """

    @pytest.fixture
    def mock_connection(self):
        """Create a mock connection with required attributes."""
        conn = mock.MagicMock()
        conn.host = "localhost"
        conn.port = 6379
        conn.db = 0
        conn.should_reconnect.return_value = False

        # Mock retry to just execute the function directly
        def mock_call_with_retry(do, fail, is_retryable=None, with_failure_count=False):
            return do()

        conn.retry.call_with_retry = mock_call_with_retry
        conn.retry.get_retries.return_value = 0

        return conn

    @pytest.fixture
    def mock_connection_pool(self, mock_connection):
        """Create a mock connection pool."""
        pool = mock.MagicMock()
        pool.get_connection.return_value = mock_connection
        pool.get_encoder.return_value = mock.MagicMock()
        return pool

    @pytest.fixture
    def mock_meter(self):
        """Create a mock Meter that tracks all instrument calls."""
        meter = mock.MagicMock()

        # Create mock histogram for operation duration
        self.operation_duration = mock.MagicMock()
        # Create mock counter for client errors
        self.client_errors = mock.MagicMock()

        def create_histogram_side_effect(name, **kwargs):
            if name == "db.client.operation.duration":
                return self.operation_duration
            return mock.MagicMock()

        def create_counter_side_effect(name, **kwargs):
            if name == "redis.client.errors":
                return self.client_errors
            return mock.MagicMock()

        meter.create_counter.side_effect = create_counter_side_effect
        meter.create_up_down_counter.return_value = mock.MagicMock()
        meter.create_histogram.side_effect = create_histogram_side_effect

        return meter

    @pytest.fixture
    def setup_pubsub_with_otel(self, mock_connection_pool, mock_connection, mock_meter):
        """
        Setup a PubSub with mocked connection and OTel collector.
        Returns tuple of (pubsub, operation_duration_mock).
        """
        from redis.client import PubSub
        from redis.event import EventDispatcher
        from redis.observability import recorder
        from redis.observability.config import OTelConfig, MetricGroup
        from redis.observability.metrics import RedisMetricsCollector

        # Reset any existing collector state
        recorder.reset_collector()

        # Create config with COMMAND group enabled
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        # Create collector with mocked meter
        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        # Patch the recorder to use our collector
        with mock.patch.object(
            recorder, "_get_or_create_collector", return_value=collector
        ):
            # Create event dispatcher (real one, to test the full chain)
            event_dispatcher = EventDispatcher()

            # Create PubSub with mocked connection pool
            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )

            # Set the connection directly to avoid subscribe flow
            pubsub.connection = mock_connection

            yield pubsub, self.operation_duration

        # Cleanup
        recorder.reset_collector()

    def test_pubsub_execute_records_metric(self, setup_pubsub_with_otel):
        """
        Test that executing a PubSub command records operation duration metric
        which is delivered to the Meter's histogram.record() method.
        """

        pubsub, operation_duration_mock = setup_pubsub_with_otel

        # Mock the command to return successfully
        mock_command = mock.MagicMock(return_value=True)

        # Execute a command through _execute
        pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

        # Verify the Meter's histogram.record() was called
        operation_duration_mock.record.assert_called_once()

        # Get the call arguments
        call_args = operation_duration_mock.record.call_args

        # Verify duration was recorded (first positional arg)
        duration = call_args[0][0]
        assert isinstance(duration, float)
        assert duration >= 0

        # Verify attributes
        attrs = call_args[1]["attributes"]
        assert attrs["db.operation.name"] == "SUBSCRIBE"
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    def test_pubsub_error_records_error_count(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that when a PubSub command raises an exception,
        error count is recorded via record_error_count.

        Note: record_operation_duration is NOT called for final errors -
        only record_error_count is called. record_operation_duration is
        only called during retries (in _close_connection) and on success.
        """

        recorder.reset_collector()
        # Enable RESILIENCY metric group for error counting
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.RESILIENCY])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, "_get_or_create_collector", return_value=collector
        ):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            # Make command raise an exception
            test_error = redis.ConnectionError("Connection failed")
            mock_command = mock.MagicMock(side_effect=test_error)

            # Execute should raise the error
            with pytest.raises(redis.ConnectionError):
                pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

            # Verify record_error_count was called (via client_errors counter)
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in attributes
            call_args = self.client_errors.add.call_args
            attrs = call_args[1]["attributes"]
            assert "error.type" in attrs

            # Verify operation_duration was NOT called (no retries, direct failure)
            self.operation_duration.record.assert_not_called()

        recorder.reset_collector()

    def test_pubsub_server_attributes_recorded(self, setup_pubsub_with_otel):
        """
        Test that server address, port, and db namespace are correctly recorded.
        """
        pubsub, operation_duration_mock = setup_pubsub_with_otel

        mock_command = mock.MagicMock(return_value=True)
        pubsub._execute(pubsub.connection, mock_command, "PING")

        call_args = operation_duration_mock.record.call_args
        attrs = call_args[1]["attributes"]

        # Verify server attributes match mock connection
        assert attrs["server.address"] == "localhost"
        assert attrs["server.port"] == 6379
        assert attrs["db.namespace"] == "0"

    def test_pubsub_retry_records_metric_on_each_attempt(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when a PubSub command is retried, operation duration metric
        is recorded for each retry attempt with retry_attempts attribute.
        """

        # Create connection with retry behavior
        mock_connection = mock.MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_connection.should_reconnect.return_value = False

        max_retries = 2

        def call_with_retry_impl(
            func, error_handler, is_retryable=None, with_failure_count=False
        ):
            """Simulate retry behavior - fail twice, then succeed."""
            for attempt in range(max_retries + 1):
                try:
                    return func()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            error_handler(e, attempt + 1)
                        else:
                            error_handler(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, "_get_or_create_collector", return_value=collector
        ):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            # Make command fail twice then succeed
            call_count = [0]

            def command_impl(*args, **kwargs):
                call_count[0] += 1
                if call_count[0] <= 2:
                    raise redis.ConnectionError("Connection failed")
                return True

            mock_command = mock.MagicMock(side_effect=command_impl)

            # Execute command - should retry twice then succeed
            pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

            # Verify histogram.record() was called 3 times:
            # 2 retry attempts + 1 final success
            assert self.operation_duration.record.call_count == 3

            calls = self.operation_duration.record.call_args_list

            # First two calls should have error.type (retry attempts)
            assert "error.type" in calls[0][1]["attributes"]
            assert "error.type" in calls[1][1]["attributes"]

            # Last call should be success (no error.type)
            assert "error.type" not in calls[2][1]["attributes"]

        recorder.reset_collector()

    def test_pubsub_retry_exhausted_records_final_error_metric(
        self, mock_connection_pool, mock_meter
    ):
        """
        Test that when all retries are exhausted, operation duration metrics
        are recorded for each retry attempt, and error count is recorded for
        the final error.

        Note: record_operation_duration is called during retries (in _close_connection),
        but record_error_count is called for the final error (not record_operation_duration).
        """

        mock_connection = mock.MagicMock()
        mock_connection.host = "localhost"
        mock_connection.port = 6379
        mock_connection.db = 0
        mock_connection.should_reconnect.return_value = False

        max_retries = 2

        def call_with_retry_impl(
            func, error_handler, is_retryable=None, with_failure_count=False
        ):
            """Simulate retry behavior - always fail."""
            for attempt in range(max_retries + 1):
                try:
                    return func()
                except redis.ConnectionError as e:
                    if attempt < max_retries:
                        if with_failure_count:
                            error_handler(e, attempt + 1)
                        else:
                            error_handler(e)
                    else:
                        raise

        mock_connection.retry.call_with_retry = call_with_retry_impl
        mock_connection.retry.get_retries.return_value = max_retries

        mock_connection_pool.get_connection.return_value = mock_connection

        recorder.reset_collector()
        # Enable both COMMAND and RESILIENCY metric groups
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND, MetricGroup.RESILIENCY])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, "_get_or_create_collector", return_value=collector
        ):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            # Make command always fail
            mock_command = mock.MagicMock(
                side_effect=redis.ConnectionError("Connection failed")
            )

            # Execute command - should fail after all retries
            with pytest.raises(redis.ConnectionError):
                pubsub._execute(pubsub.connection, mock_command, "SUBSCRIBE", "foo")

            # Verify histogram.record() was called 2 times (for retry attempts only)
            # The final error uses record_error_count, not record_operation_duration
            assert self.operation_duration.record.call_count == 2

            calls = self.operation_duration.record.call_args_list

            # All retry calls should have error.type
            for call in calls:
                assert "error.type" in call[1]["attributes"]
                assert call[1]["attributes"]["db.operation.name"] == "SUBSCRIBE"

            # Verify record_error_count was called once for the final error
            self.client_errors.add.assert_called_once()

            # Verify error type is recorded in the final error
            error_call_args = self.client_errors.add.call_args
            error_attrs = error_call_args[1]["attributes"]
            assert "error.type" in error_attrs

        recorder.reset_collector()

    def test_pubsub_no_metric_when_no_command_name(self, setup_pubsub_with_otel):
        """
        Test that no metric is recorded when command_name is None.
        """
        pubsub, operation_duration_mock = setup_pubsub_with_otel

        mock_command = mock.MagicMock(return_value=True)

        # Execute without command name (no args)
        pubsub._execute(pubsub.connection, mock_command)

        # Verify no event was emitted
        operation_duration_mock.record.assert_not_called()

    def test_pubsub_different_commands_record_correct_names(
        self, mock_connection_pool, mock_connection, mock_meter
    ):
        """
        Test that different PubSub commands record metrics with correct command names.
        """

        recorder.reset_collector()
        config = OTelConfig(metric_groups=[MetricGroup.COMMAND])

        with mock.patch("redis.observability.metrics.OTEL_AVAILABLE", True):
            collector = RedisMetricsCollector(mock_meter, config)

        with mock.patch.object(
            recorder, "_get_or_create_collector", return_value=collector
        ):
            event_dispatcher = EventDispatcher()

            pubsub = PubSub(
                connection_pool=mock_connection_pool,
                event_dispatcher=event_dispatcher,
            )
            pubsub.connection = mock_connection

            mock_command = mock.MagicMock(return_value=True)

            commands = [
                "SUBSCRIBE",
                "UNSUBSCRIBE",
                "PSUBSCRIBE",
                "PUNSUBSCRIBE",
                "PING",
            ]

            for cmd in commands:
                pubsub._execute(pubsub.connection, mock_command, cmd, "channel")

            # Verify all commands were recorded
            assert self.operation_duration.record.call_count == len(commands)

            calls = self.operation_duration.record.call_args_list
            recorded_commands = [
                call[1]["attributes"]["db.operation.name"] for call in calls
            ]

            assert recorded_commands == commands

        recorder.reset_collector()


class TestPubSubTimeoutPropagation:
    """
    Tests for timeout propagation through the entire pubsub read chain.
    Ensures that timeouts are properly passed from get_message() through
    parse_response() to the parser and socket buffer layers.
    """

    def test_get_message_timeout_is_respected(self, r):
        """
        Test that get_message() with timeout parameter respects the timeout
        and returns None when no message arrives within the timeout period.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "subscribe"

        # Call get_message with a short timeout - should return None
        # since no message is published
        start = time.monotonic()
        msg = p.get_message(timeout=0.1)
        elapsed = time.monotonic() - start
        assert msg is None
        # Verify timeout was actually respected (within reasonable bounds)
        assert elapsed < 0.5

    def test_get_message_timeout_with_published_message(self, r):
        """
        Test that get_message() with timeout returns a message if one
        arrives before the timeout expires.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message
        r.publish("foo", "hello")

        # get_message with timeout should return the message
        msg = p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"hello"

    def test_parse_response_timeout_propagation(self, r):
        """
        Test that parse_response() properly propagates timeout to read_response().
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Call parse_response with timeout - should respect it
        start = time.monotonic()
        response = p.parse_response(block=False, timeout=0.1)
        elapsed = time.monotonic() - start
        assert response is None
        assert elapsed < 0.5

    def test_get_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_message(timeout=0) returns immediately without blocking.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout=0 should return immediately
        start = time.monotonic()
        msg = p.get_message(timeout=0)
        elapsed = time.monotonic() - start
        assert msg is None
        assert elapsed < 0.1

    def test_get_message_timeout_none_blocks(self, r):
        """
        Test that get_message(timeout=None) blocks indefinitely.
        We test this by using a thread to publish a message after a delay.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish a message after a short delay in a thread
        def publish_after_delay():
            time.sleep(0.2)
            r.publish("foo", "delayed_message")

        thread = threading.Thread(target=publish_after_delay, daemon=True)
        thread.start()

        # get_message with timeout=None should block until message arrives
        start = time.monotonic()
        msg = p.get_message(timeout=None)
        elapsed = time.monotonic() - start
        assert msg is not None
        assert msg["type"] == "message"
        assert msg["data"] == b"delayed_message"
        # Should have waited at least 0.2 seconds
        assert elapsed >= 0.15
        thread.join(timeout=1.0)

    def test_multiple_messages_with_timeout(self, r):
        """
        Test that timeout is properly handled when reading multiple messages.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # Publish multiple messages
        r.publish("foo", "msg1")
        r.publish("foo", "msg2")
        r.publish("foo", "msg3")

        # Read all messages with timeout
        messages = []
        for _ in range(3):
            msg = wait_for_message(p, timeout=1.0, func=p.get_message)
            if msg:
                messages.append(msg)

        assert len(messages) == 3
        assert messages[0]["data"] == b"msg1"
        assert messages[1]["data"] == b"msg2"
        assert messages[2]["data"] == b"msg3"

    def test_timeout_with_pattern_subscribe(self, r):
        """
        Test that timeout works correctly with pattern subscriptions.
        """
        p = r.pubsub()
        p.psubscribe("foo*")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None
        assert msg["type"] == "psubscribe"

        # Publish a message matching the pattern
        r.publish("foobar", "hello")

        # get_message with timeout should return the message
        msg = p.get_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "pmessage"
        assert msg["data"] == b"hello"

    def test_timeout_with_no_subscription(self, r):
        """
        Test that get_message with timeout returns None when subscribed but no messages.
        """
        p = r.pubsub()
        p.subscribe("foo")
        # Read subscription message
        msg = wait_for_message(p, timeout=1.0)
        assert msg is not None

        # get_message with timeout should return None when no messages
        msg = p.get_message(timeout=0.1)
        assert msg is None


class TestClusterPubSubTimeoutPropagation:
    """
    Tests for timeout propagation in ClusterPubSub for sharded pubsub.
    """

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_timeout_is_respected(self, r):
        """
        Test that get_sharded_message() with timeout parameter respects the timeout
        and returns None when no message arrives within the timeout period.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"
        pubsub.ssubscribe(channel)
        # Read subscription message
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None
        assert msg["type"] == "ssubscribe"

        # Call get_sharded_message with a short timeout - should return None
        start = time.monotonic()
        msg = pubsub.get_sharded_message(timeout=0.1)
        elapsed = time.monotonic() - start
        assert msg is None
        # Verify timeout was actually respected
        assert elapsed < 0.5
        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_timeout_with_published_message(self, r):
        """
        Test that get_sharded_message() with timeout returns a message if one
        arrives before the timeout expires.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"
        pubsub.ssubscribe(channel)
        # Read subscription message
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None

        # Publish a message
        r.spublish(channel, "hello")

        # get_sharded_message with timeout should return the message
        msg = pubsub.get_sharded_message(timeout=1.0)
        assert msg is not None
        assert msg["type"] == "smessage"
        assert msg["data"] == b"hello"
        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_timeout_zero_returns_immediately(self, r):
        """
        Test that get_sharded_message(timeout=0) returns immediately without blocking.
        """
        pubsub = r.pubsub()
        channel = "test-channel:{0}"
        pubsub.ssubscribe(channel)
        # Read subscription message
        msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
        assert msg is not None

        # get_sharded_message with timeout=0 should return immediately
        start = time.monotonic()
        msg = pubsub.get_sharded_message(timeout=0)
        elapsed = time.monotonic() - start
        assert msg is None
        assert elapsed < 0.1
        pubsub.close()

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_get_sharded_message_multiple_channels_with_timeout(self, r):
        """
        Test that timeout is properly handled when reading from multiple sharded channels.
        """
        pubsub = r.pubsub()
        channel1 = "test-channel:{0}"
        channel2 = "test-channel:{6}"
        pubsub.ssubscribe(channel1, channel2)
        # Read subscription messages
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            assert msg is not None
            assert msg["type"] == "ssubscribe"

        # Publish messages to both channels
        r.spublish(channel1, "msg1")
        r.spublish(channel2, "msg2")

        # Read messages with timeout
        messages = []
        for _ in range(2):
            msg = wait_for_message(pubsub, timeout=1.0, func=pubsub.get_sharded_message)
            if msg and msg["type"] == "smessage":
                messages.append(msg)

        assert len(messages) == 2
        pubsub.close()


class TestClusterPubSubResubscribe:
    """
    Cluster-only tests verifying that sharded pubsub resubscription after a
    reconnect groups channels by slot and does not trigger CROSSSLOT errors
    on nodes that own multiple slots.
    """

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("7.0.0")
    def test_resubscribe_shard_channels_different_slots_same_node(self, r):
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
            p.ssubscribe(ch_a)
            assert (
                wait_for_message(p, timeout=2.0, func=p.get_sharded_message) is not None
            )
            p.ssubscribe(ch_b)
            assert (
                wait_for_message(p, timeout=2.0, func=p.get_sharded_message) is not None
            )

            # Force reconnect of the per-node subscriber connection.
            node = r.get_node_from_key(ch_a)
            per_node_pubsub = p.node_pubsub_mapping[node.name]
            assert per_node_pubsub.connection is not None
            per_node_pubsub.connection.disconnect()

            # Trigger reconnect + resubscription. With batched resubscription
            # this would raise a CROSSSLOT ResponseError; with per-slot
            # grouping it must succeed and deliver fresh ssubscribe acks.
            acks = {}
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and len(acks) < 2:
                msg = p.get_sharded_message(timeout=0.5)
                if msg is None:
                    continue
                if msg["type"] == "ssubscribe":
                    acks[msg["channel"]] = msg

            assert ch_a.encode() in acks
            assert ch_b.encode() in acks

            # Both channels must still be tracked in the aggregate state.
            assert ch_a.encode() in p.shard_channels
            assert ch_b.encode() in p.shard_channels
        finally:
            p.close()


@pytest.mark.fixed_client
class TestClusterPubSubSlotMigration:
    """
    Deterministic unit tests for ClusterPubSub shard-channel slot migration
    handling. These tests bypass all I/O by mocking the per-node PubSub
    instances and the cluster's node-resolution layer, verifying the
    reconciler logic and the reverse-index routing in isolation (no live
    cluster required).
    """

    def _make_cluster_pubsub(self):
        from redis._parsers.encoders import Encoder
        from redis.cluster import ClusterPubSub

        pubsub = ClusterPubSub.__new__(ClusterPubSub)
        # Base PubSub state normally wired up by PubSub.__init__.
        pubsub.encoder = Encoder("utf-8", "strict", False)
        pubsub._lock = threading.RLock()
        pubsub.subscribed_event = threading.Event()
        pubsub.health_check_response_counter = 0
        pubsub.connection = None
        pubsub.channels = {}
        pubsub.pending_unsubscribe_channels = set()
        pubsub.shard_channels = {}
        pubsub.pending_unsubscribe_shard_channels = set()
        pubsub.patterns = {}
        pubsub.pending_unsubscribe_patterns = set()
        pubsub.ignore_subscribe_messages = False
        # ClusterPubSub-specific state.
        pubsub.cluster = mock.MagicMock()
        pubsub.node_pubsub_mapping = {}
        pubsub._shard_channel_to_node = {}
        pubsub._shard_state_lock = threading.RLock()
        pubsub.push_handler_func = None
        # Reconciliation worker state (lazy-created by on_slots_changed).
        pubsub._reconcile_executor = None
        pubsub._reconcile_futures = set()
        return pubsub

    def _make_node(self, name):
        node = mock.MagicMock()
        node.name = name
        return node

    def _make_node_pubsub(self, shard_channels=None):
        p = mock.MagicMock()
        p.shard_channels = dict(shard_channels or {})
        p.pending_unsubscribe_shard_channels = set()
        p.subscribed = True
        return p

    def test_reinitialize_moves_channel_to_new_owner(self):
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

        pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name

    def test_reinitialize_noop_when_owner_unchanged(self):
        pubsub = self._make_cluster_pubsub()
        owner = self._make_node("127.0.0.1:7000")
        channel = b"foo"
        owner_ps = self._make_node_pubsub({channel: None})
        pubsub.node_pubsub_mapping[owner.name] = owner_ps
        pubsub.shard_channels = {channel: None}
        pubsub._shard_channel_to_node = {channel: owner.name}
        pubsub.cluster.get_node_from_key.return_value = owner

        pubsub.reinitialize_shard_subscriptions()

        owner_ps.sunsubscribe.assert_not_called()
        owner_ps.ssubscribe.assert_not_called()
        assert pubsub._shard_channel_to_node[channel] == owner.name

    def test_reinitialize_tolerates_old_node_disconnect(self):
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

        pubsub.reinitialize_shard_subscriptions()

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(channel)
        assert pubsub._shard_channel_to_node[channel] == new_node.name
        # Node still in topology → keep the pubsub so auto-reconnect can
        # recover it; the dedicated socket was already torn down by
        # Connection.disconnect() before the exception surfaced.
        old_ps.reset.assert_not_called()
        assert old_node.name in pubsub.node_pubsub_mapping

    def test_reinitialize_drops_old_pubsub_when_node_removed_from_topology(self):
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

        pubsub.reinitialize_shard_subscriptions()

        assert pubsub._shard_channel_to_node[channel] == new_node.name
        old_ps.reset.assert_called_once()
        assert old_node.name not in pubsub.node_pubsub_mapping

    def test_sunsubscribe_routes_via_reverse_index_after_migration(self):
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

        pubsub.sunsubscribe(channel)

        holding_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.sunsubscribe.assert_not_called()

    def test_ssubscribe_migration_applies_newly_supplied_handler(self):
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

        new_handler = mock.Mock()
        pubsub.ssubscribe(foo=new_handler)

        old_ps.sunsubscribe.assert_called_once_with(channel)
        new_ps.ssubscribe.assert_called_once_with(foo=new_handler)

    def test_ssubscribe_skips_channel_when_node_resolution_returns_none(self):
        """
        Regression: cluster.get_node_from_key may return None for channels
        whose slot is transiently uncovered. ssubscribe must skip such
        channels rather than dereference None (which would crash on
        ``node.name`` either in the reverse-index comparison or inside
        ``_get_node_pubsub``). Mirrors the async counterpart's guard.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.cluster.get_node_from_key.return_value = None
        # Stale reverse-index entry also present to exercise both crash paths:
        # the old_name != node.name comparison and the downstream fallthrough.
        pubsub._shard_channel_to_node = {b"foo": "127.0.0.1:7000"}

        pubsub.ssubscribe(b"foo")

        # No migration, no per-node pubsub creation, no mapping mutation.
        assert pubsub.node_pubsub_mapping == {}
        assert pubsub._shard_channel_to_node == {b"foo": "127.0.0.1:7000"}

    def test_migration_driven_sunsubscribe_drops_empty_pubsub(self):
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

        message = pubsub.get_sharded_message(target_node=old_node)

        assert message is not None
        assert old_node.name not in pubsub.node_pubsub_mapping
        # The per-node pubsub's dedicated connection must be released back
        # to its pool before the mapping drop; otherwise the instance is
        # GC-eligible with no path that closes it (PubSub.__del__ does not
        # release connections), leaking a pool slot per migration.
        old_ps.reset.assert_called_once()

    def test_reinitialize_partial_progress_when_slot_uncovered(self):
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
            pubsub.reinitialize_shard_subscriptions()

        # covered_channel was migrated before the error surfaced;
        # uncovered_channel is left in place for the next reconcile pass.
        new_ps.ssubscribe.assert_called_once_with(covered_channel)
        old_ps.sunsubscribe.assert_called_once_with(covered_channel)
        assert pubsub._shard_channel_to_node[covered_channel] == new_node.name
        assert pubsub._shard_channel_to_node[uncovered_channel] == old_node.name

    def test_on_slots_changed_schedules_reconcile_off_caller_thread(self):
        """
        on_slots_changed must offload reinitialize_shard_subscriptions to a
        worker thread so the dispatch call site (MovedError handling in
        _execute_command / topology refresh in initialize) is not blocked on
        per-channel network I/O. Mirrors the async path's create_task model.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        caller_thread = threading.get_ident()
        worker_thread = {}
        release = threading.Event()

        def _record_and_wait():
            worker_thread["id"] = threading.get_ident()
            # Block so the test can assert caller-thread non-blocking
            # behavior while reconciliation is still in flight.
            release.wait(timeout=5.0)

        with mock.patch.object(
            pubsub, "reinitialize_shard_subscriptions", side_effect=_record_and_wait
        ):
            try:
                pubsub.on_slots_changed()
                # Submit returned without executing the work on our thread.
                assert len(pubsub._reconcile_futures) == 1
                (future,) = list(pubsub._reconcile_futures)
                assert not future.done()
                release.set()
                future.result(timeout=5.0)
                assert worker_thread["id"] != caller_thread
                # Done-callback cleared the tracking set.
                assert pubsub._reconcile_futures == set()
            finally:
                release.set()
                pubsub.reset()

    def test_on_slots_changed_consumes_and_logs_future_exception(self, caplog):
        """
        Regression: on_slots_changed schedules reinitialize_shard_subscriptions
        on a worker thread; when that raises (e.g. SlotNotCoveredError for a
        transient uncovered slot), the exception must be consumed and logged
        via the library logger so it is not silently lost.
        """
        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}
        boom = SlotNotCoveredError("simulated transient uncovered slot")
        # Hold the worker inside the side-effect until the test has captured
        # the scheduled future, so the done-callback cannot discard it before
        # we observe it.
        release = threading.Event()

        def _raise():
            release.wait(timeout=5.0)
            raise boom

        with mock.patch.object(
            pubsub, "reinitialize_shard_subscriptions", side_effect=_raise
        ):
            with caplog.at_level(logging.ERROR, logger="redis.cluster"):
                try:
                    pubsub.on_slots_changed()
                    (future,) = list(pubsub._reconcile_futures)
                    # Register a sentinel done-callback after the two set by
                    # on_slots_changed. Since callbacks run in registration
                    # order, waiting on this Event guarantees both the
                    # discard-from-set and log-exception callbacks have run
                    # before the test assertions execute (future.result()
                    # alone can return before callbacks complete).
                    callbacks_done = threading.Event()
                    future.add_done_callback(lambda _f: callbacks_done.set())
                    release.set()
                    try:
                        future.result(timeout=5.0)
                    except SlotNotCoveredError:
                        pass
                    assert callbacks_done.wait(timeout=5.0)
                finally:
                    release.set()
                    pubsub.reset()

        assert pubsub._reconcile_futures == set()
        assert any(
            "shard subscription reconciliation failed" in rec.message
            and rec.levelno == logging.ERROR
            for rec in caplog.records
        )

    def test_ssubscribe_serializes_against_reconcile_lock(self):
        """
        Regression: ssubscribe / sunsubscribe / get_sharded_message must
        acquire self._shard_state_lock so they are mutually exclusive with
        the worker thread running reinitialize_shard_subscriptions. Without
        this serialization, the reverse index, shard_channels, and
        node_pubsub_mapping can be mutated concurrently and the user's
        subscription intent can be overwritten by the reconciliation pass.
        The lock is dedicated to shard-state bookkeeping (separate from
        PubSub.self._lock) so reconciliation does not starve aclose /
        send_command on the cluster-level connection.
        """
        pubsub = self._make_cluster_pubsub()
        node = self._make_node("127.0.0.1:7000")
        node_ps = self._make_node_pubsub()
        pubsub.node_pubsub_mapping[node.name] = node_ps
        pubsub.cluster.get_node_from_key.return_value = node

        holder_acquired = threading.Event()
        release_holder = threading.Event()
        user_done = threading.Event()

        def _hold_lock():
            with pubsub._shard_state_lock:
                holder_acquired.set()
                release_holder.wait(timeout=5.0)

        holder = threading.Thread(target=_hold_lock)
        holder.start()
        try:
            assert holder_acquired.wait(timeout=2.0)

            def _user_ssubscribe():
                pubsub.ssubscribe(b"foo")
                user_done.set()

            user = threading.Thread(target=_user_ssubscribe)
            user.start()
            try:
                # Holder still owns the lock: ssubscribe must be blocked and
                # cannot have produced any side effect on the per-node pubsub.
                assert not user_done.wait(timeout=0.1)
                node_ps.ssubscribe.assert_not_called()

                release_holder.set()
                assert user_done.wait(timeout=2.0)
                node_ps.ssubscribe.assert_called_once_with(b"foo")
            finally:
                user.join(timeout=2.0)
        finally:
            release_holder.set()
            holder.join(timeout=2.0)

    def test_on_slots_changed_concurrent_calls_create_single_executor(self):
        """
        Regression: EventDispatcher.dispatch() releases its lock before
        invoking listeners, so concurrent MovedError-handling threads can
        all reach on_slots_changed at the same time. The lazy creation of
        _reconcile_executor must be serialized so exactly one executor is
        constructed; otherwise orphaned ThreadPoolExecutors leak their
        worker threads.
        """
        from redis import cluster as cluster_module

        pubsub = self._make_cluster_pubsub()
        pubsub.shard_channels = {b"foo": None}

        constructed: list = []
        real_executor_cls = cluster_module.ThreadPoolExecutor
        # Block the constructor briefly so multiple threads pile up on the
        # _shard_state_lock; without serialization each waiting thread would
        # also see _reconcile_executor is None and construct its own.
        construct_gate = threading.Event()

        def _counting_executor(*args, **kwargs):
            construct_gate.wait(timeout=2.0)
            instance = real_executor_cls(*args, **kwargs)
            constructed.append(instance)
            return instance

        start_barrier = threading.Barrier(8)

        def _worker():
            start_barrier.wait(timeout=2.0)
            pubsub.on_slots_changed()

        with mock.patch.object(
            cluster_module, "ThreadPoolExecutor", side_effect=_counting_executor
        ):
            with mock.patch.object(
                pubsub, "reinitialize_shard_subscriptions", return_value=None
            ):
                threads = [threading.Thread(target=_worker) for _ in range(8)]
                try:
                    for t in threads:
                        t.start()
                    construct_gate.set()
                    for t in threads:
                        t.join(timeout=5.0)
                        assert not t.is_alive()
                finally:
                    construct_gate.set()
                    pubsub.reset()

        assert len(constructed) == 1
        # All callers observe the same executor instance.
        assert pubsub._reconcile_executor is None  # cleared by reset() above

    def test_reset_tears_down_per_node_pubsubs(self):
        """
        Regression: reset() must close every per-node pubsub so its dedicated
        connection is released and its (now stale) shard_channels cannot be
        replayed by PubSub.on_connect on a subsequent reconnect. Mirrors the
        async aclose() path which calls aclose() on each per-node pubsub
        before clearing cluster-level state.
        """
        pubsub = self._make_cluster_pubsub()
        ps_a = self._make_node_pubsub({b"foo": None})
        ps_b = self._make_node_pubsub({b"bar": None})
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_a,
            "127.0.0.1:7001": ps_b,
        }
        pubsub.shard_channels = {b"foo": None, b"bar": None}
        pubsub._shard_channel_to_node = {
            b"foo": "127.0.0.1:7000",
            b"bar": "127.0.0.1:7001",
        }

        pubsub.reset()

        ps_a.reset.assert_called_once()
        ps_b.reset.assert_called_once()
        # Cluster-level shard state cleared by super().reset() and our hook.
        assert pubsub.shard_channels == {}
        assert pubsub._shard_channel_to_node == {}
        # Mapping itself must also be cleared so the round-robin in
        # _pubsubs_generator can't yield dead per-node pubsubs between
        # teardown and re-subscription.
        assert pubsub.node_pubsub_mapping == {}

    def test_reset_swallows_per_node_teardown_errors(self):
        """
        reset() is also a fallback path from __del__; one buggy per-node
        pubsub raising must not mask teardown of the others or of the
        cluster-level state.
        """
        pubsub = self._make_cluster_pubsub()
        ps_bad = self._make_node_pubsub()
        ps_bad.reset.side_effect = RuntimeError("boom")
        ps_good = self._make_node_pubsub()
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_bad,
            "127.0.0.1:7001": ps_good,
        }

        pubsub.reset()

        ps_bad.reset.assert_called_once()
        ps_good.reset.assert_called_once()
        assert pubsub._shard_channel_to_node == {}
        assert pubsub.node_pubsub_mapping == {}

    def test_get_sharded_message_with_missing_target_node_returns_none(self):
        """
        Regression: get_sharded_message(target_node=...) must not raise
        KeyError when the per-node pubsub has been removed from
        node_pubsub_mapping. Migration-driven cleanup in the sunsubscribe
        branch and reset() both remove entries, so a caller polling with
        target_node can race the cleanup. Mirrors the async counterpart's
        .get()/None-fallthrough behavior.
        """
        pubsub = self._make_cluster_pubsub()
        # node_pubsub_mapping is empty - the entry was already cleaned up
        # (e.g. by a migration-driven sunsubscribe pop or by reset()).
        evicted_node = self._make_node("127.0.0.1:7000")

        message = pubsub.get_sharded_message(target_node=evicted_node)

        assert message is None

    def test_reset_recreates_pubsubs_generator(self):
        """
        Regression: _pubsubs_generator captures node_pubsub_mapping.values()
        into a local list inside ``yield from``; clearing the mapping does
        not reach references already held by that captured snapshot. A
        generator suspended mid-yield-from would surface stale (now reset())
        per-node pubsubs after re-subscription unless reset() also rebinds
        _pubsubs_generator to a fresh generator instance.
        """
        from redis.cluster import ClusterPubSub

        pubsub = self._make_cluster_pubsub()
        pubsub._pubsubs_generator = ClusterPubSub._pubsubs_generator(pubsub)
        ps_a = self._make_node_pubsub({b"foo": None})
        ps_b = self._make_node_pubsub({b"bar": None})
        pubsub.node_pubsub_mapping = {
            "127.0.0.1:7000": ps_a,
            "127.0.0.1:7001": ps_b,
        }
        # Advance into yield-from so the generator's frame holds a
        # captured list referencing both stale pubsubs.
        first = next(pubsub._pubsubs_generator)
        assert first in (ps_a, ps_b)
        old_generator = pubsub._pubsubs_generator

        pubsub.reset()

        assert pubsub._pubsubs_generator is not old_generator
        # After re-subscription the fresh generator must yield the new
        # entry rather than draining the old captured list first.
        fresh_ps = self._make_node_pubsub({b"baz": None})
        pubsub.node_pubsub_mapping["127.0.0.1:7002"] = fresh_ps
        assert next(pubsub._pubsubs_generator) is fresh_ps
