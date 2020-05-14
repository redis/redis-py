from __future__ import unicode_literals
import pytest
import time

import redis
from redis.exceptions import ConnectionError
from redis._compat import basestring, unichr

from .conftest import _get_client
from .conftest import skip_if_server_version_lt


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
        'pattern': pattern and pattern.encode('utf-8') or None,
        'channel': channel and channel.encode('utf-8') or None,
        'data': data.encode('utf-8') if isinstance(data, basestring) else data
    }


def make_subscribe_test_data(pubsub, type):
    if type == 'channel':
        return {
            'p': pubsub,
            'sub_type': 'subscribe',
            'unsub_type': 'unsubscribe',
            'sub_func': pubsub.subscribe,
            'unsub_func': pubsub.unsubscribe,
            'keys': ['foo', 'bar', 'uni' + unichr(4456) + 'code']
        }
    elif type == 'pattern':
        return {
            'p': pubsub,
            'sub_type': 'psubscribe',
            'unsub_type': 'punsubscribe',
            'sub_func': pubsub.psubscribe,
            'unsub_func': pubsub.punsubscribe,
            'keys': ['f*', 'b*', 'uni' + unichr(4456) + '*']
        }
    assert False, 'invalid subscribe type: %s' % type


class TestPubSubSubscribeUnsubscribe(object):

    def _test_subscribe_unsubscribe(self, p, sub_type, unsub_type, sub_func,
                                    unsub_func, keys):
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
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        self._test_subscribe_unsubscribe(**kwargs)

    def test_pattern_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        self._test_subscribe_unsubscribe(**kwargs)

    def _test_resubscribe_on_reconnection(self, p, sub_type, unsub_type,
                                          sub_func, unsub_func, keys):

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
            assert message['type'] == sub_type
            assert message['data'] == i + 1
            assert isinstance(message['channel'], bytes)
            channel = message['channel'].decode('utf-8')
            unique_channels.add(channel)

        assert len(unique_channels) == len(keys)
        for channel in unique_channels:
            assert channel in keys

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
            (p.punsubscribe, 'f*'),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert func(channel) is None
            assert p.subscribed is True
            assert wait_for_message(p) is None
        assert p.subscribed is False

    def test_ignore_individual_subscribe_messages(self, r):
        p = r.pubsub()

        checks = (
            (p.subscribe, 'foo'),
            (p.unsubscribe, 'foo'),
            (p.psubscribe, 'f*'),
            (p.punsubscribe, 'f*'),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert func(channel) is None
            assert p.subscribed is True
            message = wait_for_message(p, ignore_subscribe_messages=True)
            assert message is None
        assert p.subscribed is False

    def test_sub_unsub_resub_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        self._test_sub_unsub_resub(**kwargs)

    def test_sub_unsub_resub_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        self._test_sub_unsub_resub(**kwargs)

    def _test_sub_unsub_resub(self, p, sub_type, unsub_type, sub_func,
                              unsub_func, keys):
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

    def test_sub_unsub_all_resub_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        self._test_sub_unsub_all_resub(**kwargs)

    def test_sub_unsub_all_resub_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        self._test_sub_unsub_all_resub(**kwargs)

    def _test_sub_unsub_all_resub(self, p, sub_type, unsub_type, sub_func,
                                  unsub_func, keys):
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


class TestPubSubMessages(object):
    def setup_method(self, method):
        self.message = None

    def message_handler(self, message):
        self.message = message

    def test_published_message_to_channel(self, r):
        p = r.pubsub()
        p.subscribe('foo')
        assert wait_for_message(p) == make_message('subscribe', 'foo', 1)
        assert r.publish('foo', 'test message') == 1

        message = wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message('message', 'foo', 'test message')

    def test_published_message_to_pattern(self, r):
        p = r.pubsub()
        p.subscribe('foo')
        p.psubscribe('f*')
        assert wait_for_message(p) == make_message('subscribe', 'foo', 1)
        assert wait_for_message(p) == make_message('psubscribe', 'f*', 2)
        # 1 to pattern, 1 to channel
        assert r.publish('foo', 'test message') == 2

        message1 = wait_for_message(p)
        message2 = wait_for_message(p)
        assert isinstance(message1, dict)
        assert isinstance(message2, dict)

        expected = [
            make_message('message', 'foo', 'test message'),
            make_message('pmessage', 'foo', 'test message', pattern='f*')
        ]

        assert message1 in expected
        assert message2 in expected
        assert message1 != message2

    def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(foo=self.message_handler)
        assert wait_for_message(p) is None
        assert r.publish('foo', 'test message') == 1
        assert wait_for_message(p) is None
        assert self.message == make_message('message', 'foo', 'test message')

    def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(**{'f*': self.message_handler})
        assert wait_for_message(p) is None
        assert r.publish('foo', 'test message') == 1
        assert wait_for_message(p) is None
        assert self.message == make_message('pmessage', 'foo', 'test message',
                                            pattern='f*')

    def test_unicode_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = 'uni' + unichr(4456) + 'code'
        channels = {channel: self.message_handler}
        p.subscribe(**channels)
        assert wait_for_message(p) is None
        assert r.publish(channel, 'test message') == 1
        assert wait_for_message(p) is None
        assert self.message == make_message('message', channel, 'test message')

    def test_unicode_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = 'uni' + unichr(4456) + '*'
        channel = 'uni' + unichr(4456) + 'code'
        p.psubscribe(**{pattern: self.message_handler})
        assert wait_for_message(p) is None
        assert r.publish(channel, 'test message') == 1
        assert wait_for_message(p) is None
        assert self.message == make_message('pmessage', channel,
                                            'test message', pattern=pattern)

    def test_get_message_without_subscribe(self, r):
        p = r.pubsub()
        with pytest.raises(RuntimeError) as info:
            p.get_message()
        expect = ('connection not set: '
                  'did you forget to call subscribe() or psubscribe()?')
        assert expect in info.exconly()


class TestPubSubAutoDecoding(object):
    "These tests only validate that we get unicode values back"

    channel = 'uni' + unichr(4456) + 'code'
    pattern = 'uni' + unichr(4456) + '*'
    data = 'abc' + unichr(4458) + '123'

    def make_message(self, type, channel, data, pattern=None):
        return {
            'type': type,
            'channel': channel,
            'pattern': pattern,
            'data': data
        }

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
        assert wait_for_message(p) == self.make_message('subscribe',
                                                        self.channel, 1)

        p.unsubscribe(self.channel)
        assert wait_for_message(p) == self.make_message('unsubscribe',
                                                        self.channel, 0)

    def test_pattern_subscribe_unsubscribe(self, r):
        p = r.pubsub()
        p.psubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message('psubscribe',
                                                        self.pattern, 1)

        p.punsubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message('punsubscribe',
                                                        self.pattern, 0)

    def test_channel_publish(self, r):
        p = r.pubsub()
        p.subscribe(self.channel)
        assert wait_for_message(p) == self.make_message('subscribe',
                                                        self.channel, 1)
        r.publish(self.channel, self.data)
        assert wait_for_message(p) == self.make_message('message',
                                                        self.channel,
                                                        self.data)

    def test_pattern_publish(self, r):
        p = r.pubsub()
        p.psubscribe(self.pattern)
        assert wait_for_message(p) == self.make_message('psubscribe',
                                                        self.pattern, 1)
        r.publish(self.channel, self.data)
        assert wait_for_message(p) == self.make_message('pmessage',
                                                        self.channel,
                                                        self.data,
                                                        pattern=self.pattern)

    def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(**{self.channel: self.message_handler})
        assert wait_for_message(p) is None
        r.publish(self.channel, self.data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message('message', self.channel,
                                                 self.data)

        # test that we reconnected to the correct channel
        self.message = None
        p.connection.disconnect()
        assert wait_for_message(p) is None  # should reconnect
        new_data = self.data + 'new data'
        r.publish(self.channel, new_data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message('message', self.channel,
                                                 new_data)

    def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.psubscribe(**{self.pattern: self.message_handler})
        assert wait_for_message(p) is None
        r.publish(self.channel, self.data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message('pmessage', self.channel,
                                                 self.data,
                                                 pattern=self.pattern)

        # test that we reconnected to the correct pattern
        self.message = None
        p.connection.disconnect()
        assert wait_for_message(p) is None  # should reconnect
        new_data = self.data + 'new data'
        r.publish(self.channel, new_data)
        assert wait_for_message(p) is None
        assert self.message == self.make_message('pmessage', self.channel,
                                                 new_data,
                                                 pattern=self.pattern)

    def test_context_manager(self, r):
        with r.pubsub() as pubsub:
            pubsub.subscribe('foo')
            assert pubsub.connection is not None

        assert pubsub.connection is None
        assert pubsub.channels == {}
        assert pubsub.patterns == {}


class TestPubSubRedisDown(object):

    def test_channel_subscribe(self, r):
        r = redis.Redis(host='localhost', port=6390)
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            p.subscribe('foo')


class TestPubSubSubcommands(object):

    @skip_if_server_version_lt('2.8.0')
    def test_pubsub_channels(self, r):
        p = r.pubsub()
        p.subscribe('foo', 'bar', 'baz', 'quux')
        for i in range(4):
            assert wait_for_message(p)['type'] == 'subscribe'
        channels = sorted(r.pubsub_channels())
        # assert channels == [b'bar', b'baz', b'foo', b'quux']
        if channels != [b'bar', b'baz', b'foo', b'quux']:
            import pdb
            pdb.set_trace()

    @skip_if_server_version_lt('2.8.0')
    def test_pubsub_numsub(self, r):
        p1 = r.pubsub()
        p1.subscribe('foo', 'bar', 'baz')
        for i in range(3):
            assert wait_for_message(p1)['type'] == 'subscribe'
        p2 = r.pubsub()
        p2.subscribe('bar', 'baz')
        for i in range(2):
            assert wait_for_message(p2)['type'] == 'subscribe'
        p3 = r.pubsub()
        p3.subscribe('baz')
        assert wait_for_message(p3)['type'] == 'subscribe'

        channels = [(b'foo', 1), (b'bar', 2), (b'baz', 3)]
        assert channels == r.pubsub_numsub('foo', 'bar', 'baz')

    @skip_if_server_version_lt('2.8.0')
    def test_pubsub_numpat(self, r):
        p = r.pubsub()
        p.psubscribe('*oo', '*ar', 'b*z')
        for i in range(3):
            assert wait_for_message(p)['type'] == 'psubscribe'
        assert r.pubsub_numpat() == 3


class TestPubSubPings(object):

    @skip_if_server_version_lt('3.0.0')
    def test_send_pubsub_ping(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe('foo')
        p.ping()
        assert wait_for_message(p) == make_message(type='pong', channel=None,
                                                   data='',
                                                   pattern=None)

    @skip_if_server_version_lt('3.0.0')
    def test_send_pubsub_ping_message(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe('foo')
        p.ping(message='hello world')
        assert wait_for_message(p) == make_message(type='pong', channel=None,
                                                   data='hello world',
                                                   pattern=None)


class TestPubSubConnectionKilled(object):

    @skip_if_server_version_lt('3.0.0')
    def test_connection_error_raised_when_connection_dies(self, r):
        p = r.pubsub()
        p.subscribe('foo')
        assert wait_for_message(p) == make_message('subscribe', 'foo', 1)
        for client in r.client_list():
            if client['cmd'] == 'subscribe':
                r.client_kill_filter(_id=client['id'])
        with pytest.raises(ConnectionError):
            wait_for_message(p)


class TestPubSubTimeouts(object):
    def test_get_message_with_timeout_returns_none(self, r):
        p = r.pubsub()
        p.subscribe('foo')
        assert wait_for_message(p) == make_message('subscribe', 'foo', 1)
        assert p.get_message(timeout=0.01) is None
