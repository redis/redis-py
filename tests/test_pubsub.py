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
from redis.exceptions import ConnectionError
from redis.utils import HIREDIS_AVAILABLE

from .conftest import (
    _get_client,
    is_resp2_connection,
    skip_if_redis_enterprise,
    skip_if_server_version_lt,
)


def wait_for_message(
    pubsub, timeout=0.5, ignore_subscribe_messages=False, node=None, func=None
):
    now = time.time()
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
        now = time.time()
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
    # see: https://redis-py-cluster.readthedocs.io/en/stable/pubsub.html
    # #known-limitations-with-pubsub
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

    @pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
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

    @pytest.mark.skipif(HIREDIS_AVAILABLE, reason="PythonParser only")
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
        with patch.object(threading.Event, "wait") as mock:
            assert p.subscribed is True
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
        with mock.patch.object(p, "get_message", side_effect=Exception("error")):
            pubsub_thread = p.run_in_thread(
                daemon=True, exception_handler=exception_handler
            )

        assert event.wait(timeout=1.0)
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
        with patch("redis._parsers._RESP2Parser.read_response") as mock1, patch(
            "redis._parsers._HiredisParser.read_response"
        ) as mock2, patch("redis._parsers._RESP3Parser.read_response") as mock3:
            mock1.side_effect = BaseException("boom")
            mock2.side_effect = BaseException("boom")
            mock3.side_effect = BaseException("boom")

            with pytest.raises(BaseException):
                get_msg()

        # the timeout on the read should not cause disconnect
        assert is_connected()
