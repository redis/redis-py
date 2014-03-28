from __future__ import with_statement
import pytest
import time

import redis
from redis.exceptions import ConnectionError


def wait_for_message(pubsub, timeout=0.1, ignore_subscribe_messages=False):
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        message = pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages)
        if message is not None:
            return message
        time.sleep(0.01)
        now = time.time()
    return None


def make_message(type, channel, data, pattern=None):
    return {
        'type': type,
        'pattern': pattern,
        'channel': channel,
        'data': data
    }


class TestPubSubSubscribeUnsubscribe(object):

    def test_subscribe_unsubscribe(self, r):
        p = r.pubsub()

        assert p.subscribe('foo', 'bar') is None

        # should be 2 messages indicating that we've subscribed
        assert wait_for_message(p) == make_message('subscribe', 'foo', 1)
        assert wait_for_message(p) == make_message('subscribe', 'bar', 2)

        assert p.unsubscribe('foo', 'bar') is None

        # should be 2 messages indicating that we've unsubscribed
        assert wait_for_message(p) == make_message('unsubscribe', 'foo', 1)
        assert wait_for_message(p) == make_message('unsubscribe', 'bar', 0)

    def test_pattern_subscribe_unsubscribe(self, r):
        p = r.pubsub()

        assert p.psubscribe('f*', 'b*') is None

        # should be 2 messages indicating that we've subscribed
        assert wait_for_message(p) == make_message('psubscribe', 'f*', 1)
        assert wait_for_message(p) == make_message('psubscribe', 'b*', 2)

        assert p.punsubscribe('f*', 'b*') is None

        # should be 2 messages indicating that we've unsubscribed
        assert wait_for_message(p) == make_message('punsubscribe', 'f*', 1)
        assert wait_for_message(p) == make_message('punsubscribe', 'b*', 0)

    def test_resubscribe_to_channels_on_reconnection(self, r):
        channels = ['foo', 'bar']
        p = r.pubsub()

        assert p.subscribe(*channels) is None

        for i, channel in enumerate(channels):
            i += 1  # enumerate is 0 index, but we want 1 based indexing
            assert wait_for_message(p) == make_message('subscribe', channel, i)

        # manually disconnect
        p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        for i, channel in enumerate(channels):
            i += 1  # enumerate is 0 index, but we want 1 based indexing
            assert wait_for_message(p) == make_message('subscribe', channel, i)

    def test_resubscribe_to_patterns_on_reconnection(self, r):
        patterns = ['f*', 'b*']
        p = r.pubsub()

        assert p.psubscribe(*patterns) is None

        for i, pattern in enumerate(patterns):
            i += 1  # enumerate is 0 index, but we want 1 based indexing
            assert wait_for_message(p) == make_message(
                'psubscribe', pattern, i)

        # manually disconnect
        p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        for i, pattern in enumerate(patterns):
            i += 1  # enumerate is 0 index, but we want 1 based indexing
            assert wait_for_message(p) == make_message(
                'psubscribe', pattern, i)

    def test_ignore_all_subscribe_messages(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)

        checks = (
            (p.subscribe, 'foo'),
            (p.unsubscribe, 'foo'),
            (p.psubscribe, 'f*'),
            (p.unsubscribe, 'f*'),
        )

        for func, channel in checks:
            assert func(channel) is None
            assert wait_for_message(p) is None

    def test_ignore_individual_subscribe_messages(self, r):
        p = r.pubsub()

        checks = (
            (p.subscribe, 'foo'),
            (p.unsubscribe, 'foo'),
            (p.psubscribe, 'f*'),
            (p.unsubscribe, 'f*'),
        )

        for func, channel in checks:
            assert func(channel) is None
            message = wait_for_message(p, ignore_subscribe_messages=True)
            assert message is None


class TestPubSubRedisDown(object):

    def test_channel_subscribe(self, r):
        r = redis.Redis(host='localhost', port=6390)
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            p.subscribe('foo')
