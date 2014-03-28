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


def make_subscribe_test_data(pubsub, type):
    if type == 'channel':
        return {
            'p': pubsub,
            'sub_type': 'subscribe',
            'unsub_type': 'unsubscribe',
            'sub_func': pubsub.subscribe,
            'unsub_func': pubsub.unsubscribe,
            'keys': ['foo', 'bar']
        }
    elif type == 'pattern':
        return {
            'p': pubsub,
            'sub_type': 'psubscribe',
            'unsub_type': 'punsubscribe',
            'sub_func': pubsub.psubscribe,
            'unsub_func': pubsub.punsubscribe,
            'keys': ['f*', 'b*']
        }
    assert False, 'invalid subscribe type: %s' % type


class TestPubSubSubscribeUnsubscribe(object):

    def _test_subscribe_unsubscribe(self, p, sub_type, unsub_type, sub_func,
                                    unsub_func, keys):
        assert sub_func(*keys) is None

        # should be 2 messages indicating that we've subscribed
        assert wait_for_message(p) == make_message(sub_type, keys[0], 1)
        assert wait_for_message(p) == make_message(sub_type, keys[1], 2)

        assert unsub_func(*keys) is None

        # should be 2 messages indicating that we've unsubscribed
        assert wait_for_message(p) == make_message(unsub_type, keys[0], 1)
        assert wait_for_message(p) == make_message(unsub_type, keys[1], 0)

    def test_channel_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        self._test_subscribe_unsubscribe(**kwargs)

    def test_pattern_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        self._test_subscribe_unsubscribe(**kwargs)

    def _test_resubscribe_on_reconnection(self, p, sub_type, unsub_type,
                                          sub_func, unsub_func, keys):
        assert sub_func(*keys) is None

        for i, key in enumerate(keys):
            i += 1  # enumerate is 0 index, but we want 1 based indexing
            assert wait_for_message(p) == make_message(sub_type, key, i)

        # manually disconnect
        p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        for i, key in enumerate(keys):
            i += 1  # enumerate is 0 index, but we want 1 based indexing
            assert wait_for_message(p) == make_message(sub_type, key, i)

    def test_resubscribe_to_channels_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        self._test_resubscribe_on_reconnection(**kwargs)

    def test_resubscribe_to_patterns_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        self._test_resubscribe_on_reconnection(**kwargs)

    def _test_subscribed_property(self, p, sub_type, unsub_type, sub_func,
                                  unsub_func, keys):

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
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        self._test_subscribed_property(**kwargs)

    def test_subscribe_property_with_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        self._test_subscribed_property(**kwargs)

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
