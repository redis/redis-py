from __future__ import with_statement
import binascii
import datetime
import pytest
import redis
import time

from redis._compat import (unichr, u, b, ascii_letters, iteritems, iterkeys,
                           itervalues)
from redis.client import parse_info
from redis import exceptions

from .conftest import skip_if_server_version_lt, add_namespace


@pytest.fixture()
def slowlog(request, nr):
    current_config = nr.config_get()
    old_slower_than_value = current_config['slowlog-log-slower-than']
    old_max_legnth_value = current_config['slowlog-max-len']

    def cleanup():
        nr.config_set('slowlog-log-slower-than', old_slower_than_value)
        nr.config_set('slowlog-max-len', old_max_legnth_value)
    request.addfinalizer(cleanup)

    nr.config_set('slowlog-log-slower-than', 0)
    nr.config_set('slowlog-max-len', 128)


def redis_server_time(client):
    seconds, milliseconds = client.time()
    timestamp = float('%s.%s' % (seconds, milliseconds))
    return datetime.datetime.fromtimestamp(timestamp)


# RESPONSE CALLBACKS
class TestResponseCallbacks(object):
    "Tests for the response callback system"

    def test_response_callbacks(self, nr):
        assert nr.response_callbacks == redis.Redis.RESPONSE_CALLBACKS
        assert id(nr.response_callbacks) != id(redis.Redis.RESPONSE_CALLBACKS)
        nr.set_response_callback('GET', lambda x: 'static')
        nr['a'] = 'foo'
        assert nr['a'] == 'static'


class TestRedisCommands(object):

    def test_command_on_invalid_key_type(self, nr):
        nr.lpush('a', '1')
        with pytest.raises(redis.ResponseError):
            nr['a']

    # SERVER INFORMATION
    def test_client_list(self, nr):
        clients = nr.client_list()
        assert isinstance(clients[0], dict)
        assert 'addr' in clients[0]

    @skip_if_server_version_lt('2.6.9')
    def test_client_getname(self, nr):
        assert nr.client_getname() is None

    @skip_if_server_version_lt('2.6.9')
    def test_client_setname(self, nr):
        assert nr.client_setname('redis_py_test')
        assert nr.client_getname() == 'redis_py_test'

    def test_config_get(self, nr):
        data = nr.config_get()
        assert 'maxmemory' in data
        assert data['maxmemory'].isdigit()

    def test_config_resetstat(self, nr):
        nr.ping()
        prior_commands_processed = int(nr.info()['total_commands_processed'])
        assert prior_commands_processed >= 1
        nr.config_resetstat()
        reset_commands_processed = int(nr.info()['total_commands_processed'])
        assert reset_commands_processed < prior_commands_processed

    def test_config_set(self, nr):
        data = nr.config_get()
        rdbname = data['dbfilename']
        try:
            assert nr.config_set('dbfilename', 'redis_py_test.rdb')
            assert nr.config_get()['dbfilename'] == 'redis_py_test.rdb'
        finally:
            assert nr.config_set('dbfilename', rdbname)

    def test_dbsize(self, nr):
        nr['a'] = 'foo'
        nr['b'] = 'bar'
        assert nr.dbsize() == 2

    def test_echo(self, nr):
        assert nr.echo('foo bar') == b('foo bar')

    def test_info(self, nr):
        nr['a'] = 'foo'
        nr['b'] = 'bar'
        info = nr.info()
        assert isinstance(info, dict)
        assert info['db9']['keys'] == 2

    def test_lastsave(self, nr):
        assert isinstance(nr.lastsave(), datetime.datetime)

    def test_object(self, nr):
        nr['a'] = 'foo'
        assert isinstance(nr.object('refcount', 'a'), int)
        assert isinstance(nr.object('idletime', 'a'), int)
        assert nr.object('encoding', 'a') in (b('raw'), b('embstr'))
        assert nr.object('idletime', 'invalid-key') is None

    def test_ping(self, nr):
        assert nr.ping()

    def test_slowlog_get(self, nr, slowlog):
        assert nr.slowlog_reset()
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        nr.get(unicode_string)
        slowlog = nr.slowlog_get()
        assert isinstance(slowlog, list)
        commands = [log['command'] for log in slowlog]

        get_command = b(' ').join((
            b('GET'),
            add_namespace(unicode_string.encode('utf-8'))
        ))
        assert get_command in commands
        assert b('SLOWLOG RESET') in commands
        # the order should be ['GET <uni string>', 'SLOWLOG RESET'],
        # but if other clients are executing commands at the same time, there
        # could be commands, before, between, or after, so just check that
        # the two we care about are in the appropriate ordenr.
        assert commands.index(get_command) < commands.index(b('SLOWLOG RESET'))

        # make sure other attributes are typed correctly
        assert isinstance(slowlog[0]['start_time'], int)
        assert isinstance(slowlog[0]['duration'], int)

    def test_slowlog_get_limit(self, nr, slowlog):
        assert nr.slowlog_reset()
        nr.get('foo')
        nr.get('bar')
        slowlog = nr.slowlog_get(1)
        assert isinstance(slowlog, list)
        commands = [log['command'] for log in slowlog]
        assert b('GET {0}'.format(add_namespace('foo'))) not in commands
        assert b('GET {0}'.format(add_namespace('bar'))) in commands

    def test_slowlog_length(self, nr, slowlog):
        nr.get('foo')
        assert isinstance(nr.slowlog_len(), int)

    @skip_if_server_version_lt('2.6.0')
    def test_time(self, nr):
        t = nr.time()
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)

    # BASIC KEY COMMANDS
    def test_append(self, nr):
        assert nr.append('a', 'a1') == 2
        assert nr['a'] == b('a1')
        assert nr.append('a', 'a2') == 4
        assert nr['a'] == b('a1a2')

    @skip_if_server_version_lt('2.6.0')
    def test_bitcount(self, nr):
        nr.setbit('a', 5, True)
        assert nr.bitcount('a') == 1
        nr.setbit('a', 6, True)
        assert nr.bitcount('a') == 2
        nr.setbit('a', 5, False)
        assert nr.bitcount('a') == 1
        nr.setbit('a', 9, True)
        nr.setbit('a', 17, True)
        nr.setbit('a', 25, True)
        nr.setbit('a', 33, True)
        assert nr.bitcount('a') == 5
        assert nr.bitcount('a', 0, -1) == 5
        assert nr.bitcount('a', 2, 3) == 2
        assert nr.bitcount('a', 2, -1) == 3
        assert nr.bitcount('a', -2, -1) == 2
        assert nr.bitcount('a', 1, 1) == 1

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_not_empty_string(self, nr):
        nr['a'] = ''
        nr.bitop('not', 'r', 'a')
        assert nr.get('r') is None

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_not(self, nr):
        test_str = b('\xAA\x00\xFF\x55')
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        nr['a'] = test_str
        nr.bitop('not', 'r', 'a')
        assert int(binascii.hexlify(nr['r']), 16) == correct

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_not_in_place(self, nr):
        test_str = b('\xAA\x00\xFF\x55')
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        nr['a'] = test_str
        nr.bitop('not', 'a', 'a')
        assert int(binascii.hexlify(nr['a']), 16) == correct

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_single_string(self, nr):
        test_str = b('\x01\x02\xFF')
        nr['a'] = test_str
        nr.bitop('and', 'res1', 'a')
        nr.bitop('or', 'res2', 'a')
        nr.bitop('xor', 'res3', 'a')
        assert nr['res1'] == test_str
        assert nr['res2'] == test_str
        assert nr['res3'] == test_str

    @skip_if_server_version_lt('2.6.0')
    def test_bitop_string_operands(self, nr):
        nr['a'] = b('\x01\x02\xFF\xFF')
        nr['b'] = b('\x01\x02\xFF')
        nr.bitop('and', 'res1', 'a', 'b')
        nr.bitop('or', 'res2', 'a', 'b')
        nr.bitop('xor', 'res3', 'a', 'b')
        assert int(binascii.hexlify(nr['res1']), 16) == 0x0102FF00
        assert int(binascii.hexlify(nr['res2']), 16) == 0x0102FFFF
        assert int(binascii.hexlify(nr['res3']), 16) == 0x000000FF

    @skip_if_server_version_lt('2.8.7')
    def test_bitpos(self, nr):
        key = 'key:bitpos'
        nr.set(key, b('\xff\xf0\x00'))
        assert nr.bitpos(key, 0) == 12
        assert nr.bitpos(key, 0, 2, -1) == 16
        assert nr.bitpos(key, 0, -2, -1) == 12
        nr.set(key, b('\x00\xff\xf0'))
        assert nr.bitpos(key, 1, 0) == 8
        assert nr.bitpos(key, 1, 1) == 8
        nr.set(key, b('\x00\x00\x00'))
        assert nr.bitpos(key, 1) == -1

    @skip_if_server_version_lt('2.8.7')
    def test_bitpos_wrong_arguments(self, nr):
        key = 'key:bitpos:wrong:args'
        nr.set(key, b('\xff\xf0\x00'))
        with pytest.raises(exceptions.RedisError):
            nr.bitpos(key, 0, end=1) == 12
        with pytest.raises(exceptions.RedisError):
            nr.bitpos(key, 7) == 12

    def test_decr(self, nr):
        assert nr.decr('a') == -1
        assert nr['a'] == b('-1')
        assert nr.decr('a') == -2
        assert nr['a'] == b('-2')
        assert nr.decr('a', amount=5) == -7
        assert nr['a'] == b('-7')

    def test_delete(self, nr):
        assert nr.delete('a') == 0
        nr['a'] = 'foo'
        assert nr.delete('a') == 1

    def test_delete_with_multiple_keys(self, nr):
        nr['a'] = 'foo'
        nr['b'] = 'bar'
        assert nr.delete('a', 'b') == 2
        assert nr.get('a') is None
        assert nr.get('b') is None

    def test_delitem(self, nr):
        nr['a'] = 'foo'
        del nr['a']
        assert nr.get('a') is None

    @skip_if_server_version_lt('2.6.0')
    def test_dump_and_restore(self, nr):
        nr['a'] = 'foo'
        dumped = nr.dump('a')
        del nr['a']
        nr.restore('a', 0, dumped)
        assert nr['a'] == b('foo')

    @skip_if_server_version_lt('3.0.0')
    def test_dump_and_restore_and_replace(self, nr):
        nr['a'] = 'bar'
        dumped = nr.dump('a')
        with pytest.raises(redis.ResponseError):
            nr.restore('a', 0, dumped)

        nr.restore('a', 0, dumped, replace=True)
        assert nr['a'] == b('bar')

    def test_exists(self, nr):
        assert not nr.exists('a')
        nr['a'] = 'foo'
        assert nr.exists('a')

    def test_exists_contains(self, nr):
        assert 'a' not in nr
        nr['a'] = 'foo'
        assert 'a' in nr

    def test_expire(self, nr):
        assert not nr.expire('a', 10)
        nr['a'] = 'foo'
        assert nr.expire('a', 10)
        assert 0 < nr.ttl('a') <= 10
        assert nr.persist('a')
        assert not nr.ttl('a')

    def test_expireat_datetime(self, nr):
        expire_at = redis_server_time(nr) + datetime.timedelta(minutes=1)
        nr['a'] = 'foo'
        assert nr.expireat('a', expire_at)
        assert 0 < nr.ttl('a') <= 61

    def test_expireat_no_key(self, nr):
        expire_at = redis_server_time(nr) + datetime.timedelta(minutes=1)
        assert not nr.expireat('a', expire_at)

    def test_expireat_unixtime(self, nr):
        expire_at = redis_server_time(nr) + datetime.timedelta(minutes=1)
        nr['a'] = 'foo'
        expire_at_seconds = int(time.mktime(expire_at.timetuple()))
        assert nr.expireat('a', expire_at_seconds)
        assert 0 < nr.ttl('a') <= 61

    def test_get_and_set(self, nr):
        # get and set can't be tested independently of each other
        assert nr.get('a') is None
        byte_string = b('value')
        integer = 5
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        assert nr.set('byte_string', byte_string)
        assert nr.set('integer', 5)
        assert nr.set('unicode_string', unicode_string)
        assert nr.get('byte_string') == byte_string
        assert nr.get('integer') == b(str(integer))
        assert nr.get('unicode_string').decode('utf-8') == unicode_string

    def test_getitem_and_setitem(self, nr):
        nr['a'] = 'bar'
        assert nr['a'] == b('bar')

    def test_getitem_raises_keyerror_for_missing_key(self, nr):
        with pytest.raises(KeyError):
            nr['a']

    def test_getitem_does_not_raise_keyerror_for_empty_string(self, nr):
        nr['a'] = b("")
        assert nr['a'] == b("")

    def test_get_set_bit(self, nr):
        # no value
        assert not nr.getbit('a', 5)
        # set bit 5
        assert not nr.setbit('a', 5, True)
        assert nr.getbit('a', 5)
        # unset bit 4
        assert not nr.setbit('a', 4, False)
        assert not nr.getbit('a', 4)
        # set bit 4
        assert not nr.setbit('a', 4, True)
        assert nr.getbit('a', 4)
        # set bit 5 again
        assert nr.setbit('a', 5, True)
        assert nr.getbit('a', 5)

    def test_getrange(self, nr):
        nr['a'] = 'foo'
        assert nr.getrange('a', 0, 0) == b('f')
        assert nr.getrange('a', 0, 2) == b('foo')
        assert nr.getrange('a', 3, 4) == b('')

    def test_getset(self, nr):
        assert nr.getset('a', 'foo') is None
        assert nr.getset('a', 'bar') == b('foo')
        assert nr.get('a') == b('bar')

    def test_incr(self, nr):
        assert nr.incr('a') == 1
        assert nr['a'] == b('1')
        assert nr.incr('a') == 2
        assert nr['a'] == b('2')
        assert nr.incr('a', amount=5) == 7
        assert nr['a'] == b('7')

    def test_incrby(self, nr):
        assert nr.incrby('a') == 1
        assert nr.incrby('a', 4) == 5
        assert nr['a'] == b('5')

    @skip_if_server_version_lt('2.6.0')
    def test_incrbyfloat(self, nr):
        assert nr.incrbyfloat('a') == 1.0
        assert nr['a'] == b('1')
        assert nr.incrbyfloat('a', 1.1) == 2.1
        assert float(nr['a']) == float(2.1)

    def test_keys(self, nr):
        assert nr.keys() == []
        keys_with_underscores = set([b('test_a'), b('test_b')])
        keys = keys_with_underscores.union(set([b('testc')]))
        for key in keys:
            nr[key] = 1
        assert set(nr.keys(pattern='test_*')) == keys_with_underscores
        assert set(nr.keys(pattern='test*')) == keys

    def test_mget(self, nr):
        assert nr.mget(['a', 'b']) == [None, None]
        nr['a'] = '1'
        nr['b'] = '2'
        nr['c'] = '3'
        assert nr.mget('a', 'other', 'b', 'c') == [b('1'), None, b('2'), b('3')]

    def test_mset(self, nr):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert nr.mset(d)
        for k, v in iteritems(d):
            assert nr[k] == v

    def test_mset_kwargs(self, nr):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert nr.mset(**d)
        for k, v in iteritems(d):
            assert nr[k] == v

    def test_msetnx(self, nr):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert nr.msetnx(d)
        d2 = {'a': b('x'), 'd': b('4')}
        assert not nr.msetnx(d2)
        for k, v in iteritems(d):
            assert nr[k] == v
        assert nr.get('d') is None

    def test_msetnx_kwargs(self, nr):
        d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
        assert nr.msetnx(**d)
        d2 = {'a': b('x'), 'd': b('4')}
        assert not nr.msetnx(**d2)
        for k, v in iteritems(d):
            assert nr[k] == v
        assert nr.get('d') is None

    @skip_if_server_version_lt('2.6.0')
    def test_pexpire(self, nr):
        assert not nr.pexpire('a', 60000)
        nr['a'] = 'foo'
        assert nr.pexpire('a', 60000)
        assert 0 < nr.pttl('a') <= 60000
        assert nr.persist('a')
        assert nr.pttl('a') is None

    @skip_if_server_version_lt('2.6.0')
    def test_pexpireat_datetime(self, nr):
        expire_at = redis_server_time(nr) + datetime.timedelta(minutes=1)
        nr['a'] = 'foo'
        assert nr.pexpireat('a', expire_at)
        assert 0 < nr.pttl('a') <= 61000

    @skip_if_server_version_lt('2.6.0')
    def test_pexpireat_no_key(self, nr):
        expire_at = redis_server_time(nr) + datetime.timedelta(minutes=1)
        assert not nr.pexpireat('a', expire_at)

    @skip_if_server_version_lt('2.6.0')
    def test_pexpireat_unixtime(self, nr):
        expire_at = redis_server_time(nr) + datetime.timedelta(minutes=1)
        nr['a'] = 'foo'
        expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
        assert nr.pexpireat('a', expire_at_seconds)
        assert 0 < nr.pttl('a') <= 61000

    @skip_if_server_version_lt('2.6.0')
    def test_psetex(self, nr):
        assert nr.psetex('a', 1000, 'value')
        assert nr['a'] == b('value')
        assert 0 < nr.pttl('a') <= 1000

    @skip_if_server_version_lt('2.6.0')
    def test_psetex_timedelta(self, nr):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert nr.psetex('a', expire_at, 'value')
        assert nr['a'] == b('value')
        assert 0 < nr.pttl('a') <= 1000

    def test_randomkey(self, nr):
        assert nr.randomkey() is None
        for key in ('a', 'b', 'c'):
            nr[key] = 1
        assert nr.randomkey() in (
            b(add_namespace('a')),
            b(add_namespace('b')),
            b(add_namespace('c'))
        )

    def test_rename(self, nr):
        nr['a'] = '1'
        assert nr.rename('a', 'b')
        assert nr.get('a') is None
        assert nr['b'] == b('1')

    def test_renamenx(self, nr):
        nr['a'] = '1'
        nr['b'] = '2'
        assert not nr.renamenx('a', 'b')
        assert nr['a'] == b('1')
        assert nr['b'] == b('2')

    @skip_if_server_version_lt('2.6.0')
    def test_set_nx(self, nr):
        assert nr.set('a', '1', nx=True)
        assert not nr.set('a', '2', nx=True)
        assert nr['a'] == b('1')

    @skip_if_server_version_lt('2.6.0')
    def test_set_xx(self, nr):
        assert not nr.set('a', '1', xx=True)
        assert nr.get('a') is None
        nr['a'] = 'bar'
        assert nr.set('a', '2', xx=True)
        assert nr.get('a') == b('2')

    @skip_if_server_version_lt('2.6.0')
    def test_set_px(self, nr):
        assert nr.set('a', '1', px=10000)
        assert nr['a'] == b('1')
        assert 0 < nr.pttl('a') <= 10000
        assert 0 < nr.ttl('a') <= 10

    @skip_if_server_version_lt('2.6.0')
    def test_set_px_timedelta(self, nr):
        expire_at = datetime.timedelta(milliseconds=1000)
        assert nr.set('a', '1', px=expire_at)
        assert 0 < nr.pttl('a') <= 1000
        assert 0 < nr.ttl('a') <= 1

    @skip_if_server_version_lt('2.6.0')
    def test_set_ex(self, nr):
        assert nr.set('a', '1', ex=10)
        assert 0 < nr.ttl('a') <= 10

    @skip_if_server_version_lt('2.6.0')
    def test_set_ex_timedelta(self, nr):
        expire_at = datetime.timedelta(seconds=60)
        assert nr.set('a', '1', ex=expire_at)
        assert 0 < nr.ttl('a') <= 60

    @skip_if_server_version_lt('2.6.0')
    def test_set_multipleoptions(self, nr):
        nr['a'] = 'val'
        assert nr.set('a', '1', xx=True, px=10000)
        assert 0 < nr.ttl('a') <= 10

    def test_setex(self, nr):
        assert nr.setex('a', '1', 60)
        assert nr['a'] == b('1')
        assert 0 < nr.ttl('a') <= 60

    def test_setnx(self, nr):
        assert nr.setnx('a', '1')
        assert nr['a'] == b('1')
        assert not nr.setnx('a', '2')
        assert nr['a'] == b('1')

    def test_setrange(self, nr):
        assert nr.setrange('a', 5, 'foo') == 8
        assert nr['a'] == b('\0\0\0\0\0foo')
        nr['a'] = 'abcdefghijh'
        assert nr.setrange('a', 6, '12345') == 11
        assert nr['a'] == b('abcdef12345')

    def test_strlen(self, nr):
        nr['a'] = 'foo'
        assert nr.strlen('a') == 3

    def test_substr(self, nr):
        nr['a'] = '0123456789'
        assert nr.substr('a', 0) == b('0123456789')
        assert nr.substr('a', 2) == b('23456789')
        assert nr.substr('a', 3, 5) == b('345')
        assert nr.substr('a', 3, -2) == b('345678')

    def test_type(self, nr):
        assert nr.type('a') == b('none')
        nr['a'] = '1'
        assert nr.type('a') == b('string')
        del nr['a']
        nr.lpush('a', '1')
        assert nr.type('a') == b('list')
        del nr['a']
        nr.sadd('a', '1')
        assert nr.type('a') == b('set')
        del nr['a']
        nr.zadd('a', **{'1': 1})
        assert nr.type('a') == b('zset')

    # LIST COMMANDS
    def test_blpop(self, nr):
        nr.rpush('a', '1', '2')
        nr.rpush('b', '3', '4')
        assert nr.blpop(['b', 'a'], timeout=1) == (b('b'), b('3'))
        assert nr.blpop(['b', 'a'], timeout=1) == (b('b'), b('4'))
        assert nr.blpop(['b', 'a'], timeout=1) == (b('a'), b('1'))
        assert nr.blpop(['b', 'a'], timeout=1) == (b('a'), b('2'))
        assert nr.blpop(['b', 'a'], timeout=1) is None
        nr.rpush('c', '1')
        assert nr.blpop('c', timeout=1) == (b('c'), b('1'))

    def test_brpop(self, nr):
        nr.rpush('a', '1', '2')
        nr.rpush('b', '3', '4')
        assert nr.brpop(['b', 'a'], timeout=1) == (b('b'), b('4'))
        assert nr.brpop(['b', 'a'], timeout=1) == (b('b'), b('3'))
        assert nr.brpop(['b', 'a'], timeout=1) == (b('a'), b('2'))
        assert nr.brpop(['b', 'a'], timeout=1) == (b('a'), b('1'))
        assert nr.brpop(['b', 'a'], timeout=1) is None
        nr.rpush('c', '1')
        assert nr.brpop('c', timeout=1) == (b('c'), b('1'))

    def test_brpoplpush(self, nr):
        nr.rpush('a', '1', '2')
        nr.rpush('b', '3', '4')
        assert nr.brpoplpush('a', 'b') == b('2')
        assert nr.brpoplpush('a', 'b') == b('1')
        assert nr.brpoplpush('a', 'b', timeout=1) is None
        assert nr.lrange('a', 0, -1) == []
        assert nr.lrange('b', 0, -1) == [b('1'), b('2'), b('3'), b('4')]

    def test_brpoplpush_empty_string(self, nr):
        nr.rpush('a', '')
        assert nr.brpoplpush('a', 'b') == b('')

    def test_lindex(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.lindex('a', '0') == b('1')
        assert nr.lindex('a', '1') == b('2')
        assert nr.lindex('a', '2') == b('3')

    def test_linsert(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.linsert('a', 'after', '2', '2.5') == 4
        assert nr.lrange('a', 0, -1) == [b('1'), b('2'), b('2.5'), b('3')]
        assert nr.linsert('a', 'before', '2', '1.5') == 5
        assert nr.lrange('a', 0, -1) == \
            [b('1'), b('1.5'), b('2'), b('2.5'), b('3')]

    def test_llen(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.llen('a') == 3

    def test_lpop(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.lpop('a') == b('1')
        assert nr.lpop('a') == b('2')
        assert nr.lpop('a') == b('3')
        assert nr.lpop('a') is None

    def test_lpush(self, nr):
        assert nr.lpush('a', '1') == 1
        assert nr.lpush('a', '2') == 2
        assert nr.lpush('a', '3', '4') == 4
        assert nr.lrange('a', 0, -1) == [b('4'), b('3'), b('2'), b('1')]

    def test_lpushx(self, nr):
        assert nr.lpushx('a', '1') == 0
        assert nr.lrange('a', 0, -1) == []
        nr.rpush('a', '1', '2', '3')
        assert nr.lpushx('a', '4') == 4
        assert nr.lrange('a', 0, -1) == [b('4'), b('1'), b('2'), b('3')]

    def test_lrange(self, nr):
        nr.rpush('a', '1', '2', '3', '4', '5')
        assert nr.lrange('a', 0, 2) == [b('1'), b('2'), b('3')]
        assert nr.lrange('a', 2, 10) == [b('3'), b('4'), b('5')]
        assert nr.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4'), b('5')]

    def test_lrem(self, nr):
        nr.rpush('a', '1', '1', '1', '1')
        assert nr.lrem('a', '1', 1) == 1
        assert nr.lrange('a', 0, -1) == [b('1'), b('1'), b('1')]
        assert nr.lrem('a', '1') == 3
        assert nr.lrange('a', 0, -1) == []

    def test_lset(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.lrange('a', 0, -1) == [b('1'), b('2'), b('3')]
        assert nr.lset('a', 1, '4')
        assert nr.lrange('a', 0, 2) == [b('1'), b('4'), b('3')]

    def test_ltrim(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.ltrim('a', 0, 1)
        assert nr.lrange('a', 0, -1) == [b('1'), b('2')]

    def test_rpop(self, nr):
        nr.rpush('a', '1', '2', '3')
        assert nr.rpop('a') == b('3')
        assert nr.rpop('a') == b('2')
        assert nr.rpop('a') == b('1')
        assert nr.rpop('a') is None

    def test_rpoplpush(self, nr):
        nr.rpush('a', 'a1', 'a2', 'a3')
        nr.rpush('b', 'b1', 'b2', 'b3')
        assert nr.rpoplpush('a', 'b') == b('a3')
        assert nr.lrange('a', 0, -1) == [b('a1'), b('a2')]
        assert nr.lrange('b', 0, -1) == [b('a3'), b('b1'), b('b2'), b('b3')]

    def test_rpush(self, nr):
        assert nr.rpush('a', '1') == 1
        assert nr.rpush('a', '2') == 2
        assert nr.rpush('a', '3', '4') == 4
        assert nr.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4')]

    def test_rpushx(self, nr):
        assert nr.rpushx('a', 'b') == 0
        assert nr.lrange('a', 0, -1) == []
        nr.rpush('a', '1', '2', '3')
        assert nr.rpushx('a', '4') == 4
        assert nr.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4')]

    # SCAN COMMANDS
    @skip_if_server_version_lt('2.8.0')
    def test_scan(self, nr):
        nr.set('a', 1)
        nr.set('b', 2)
        nr.set('c', 3)
        cursor, keys = nr.scan()
        assert cursor == 0
        assert set(keys) == set([b('a'), b('b'), b('c')])
        _, keys = nr.scan(match='a')
        assert set(keys) == set([b('a')])

    @skip_if_server_version_lt('2.8.0')
    def test_scan_iter(self, nr):
        nr.set('a', 1)
        nr.set('b', 2)
        nr.set('c', 3)
        keys = list(nr.scan_iter())
        assert set(keys) == set([b('a'), b('b'), b('c')])
        keys = list(nr.scan_iter(match='a'))
        assert set(keys) == set([b('a')])

    @skip_if_server_version_lt('2.8.0')
    def test_sscan(self, nr):
        nr.sadd('a', 1, 2, 3)
        cursor, members = nr.sscan('a')
        assert cursor == 0
        assert set(members) == set([b('1'), b('2'), b('3')])
        _, members = nr.sscan('a', match=b('1'))
        assert set(members) == set([b('1')])

    @skip_if_server_version_lt('2.8.0')
    def test_sscan_iter(self, nr):
        nr.sadd('a', 1, 2, 3)
        members = list(nr.sscan_iter('a'))
        assert set(members) == set([b('1'), b('2'), b('3')])
        members = list(nr.sscan_iter('a', match=b('1')))
        assert set(members) == set([b('1')])

    @skip_if_server_version_lt('2.8.0')
    def test_hscan(self, nr):
        nr.hmset('a', {'a': 1, 'b': 2, 'c': 3})
        cursor, dic = nr.hscan('a')
        assert cursor == 0
        assert dic == {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        _, dic = nr.hscan('a', match='a')
        assert dic == {b('a'): b('1')}

    @skip_if_server_version_lt('2.8.0')
    def test_hscan_iter(self, nr):
        nr.hmset('a', {'a': 1, 'b': 2, 'c': 3})
        dic = dict(nr.hscan_iter('a'))
        assert dic == {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        dic = dict(nr.hscan_iter('a', match='a'))
        assert dic == {b('a'): b('1')}

    @skip_if_server_version_lt('2.8.0')
    def test_zscan(self, nr):
        nr.zadd('a', 'a', 1, 'b', 2, 'c', 3)
        cursor, pairs = nr.zscan('a')
        assert cursor == 0
        assert set(pairs) == set([(b('a'), 1), (b('b'), 2), (b('c'), 3)])
        _, pairs = nr.zscan('a', match='a')
        assert set(pairs) == set([(b('a'), 1)])

    @skip_if_server_version_lt('2.8.0')
    def test_zscan_iter(self, nr):
        nr.zadd('a', 'a', 1, 'b', 2, 'c', 3)
        pairs = list(nr.zscan_iter('a'))
        assert set(pairs) == set([(b('a'), 1), (b('b'), 2), (b('c'), 3)])
        pairs = list(nr.zscan_iter('a', match='a'))
        assert set(pairs) == set([(b('a'), 1)])

    # SET COMMANDS
    def test_sadd(self, nr):
        members = set([b('1'), b('2'), b('3')])
        nr.sadd('a', *members)
        assert nr.smembers('a') == members

    def test_scard(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.scard('a') == 3

    def test_sdiff(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.sdiff('a', 'b') == set([b('1'), b('2'), b('3')])
        nr.sadd('b', '2', '3')
        assert nr.sdiff('a', 'b') == set([b('1')])

    def test_sdiffstore(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.sdiffstore('c', 'a', 'b') == 3
        assert nr.smembers('c') == set([b('1'), b('2'), b('3')])
        nr.sadd('b', '2', '3')
        assert nr.sdiffstore('c', 'a', 'b') == 1
        assert nr.smembers('c') == set([b('1')])

    def test_sinter(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.sinter('a', 'b') == set()
        nr.sadd('b', '2', '3')
        assert nr.sinter('a', 'b') == set([b('2'), b('3')])

    def test_sinterstore(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.sinterstore('c', 'a', 'b') == 0
        assert nr.smembers('c') == set()
        nr.sadd('b', '2', '3')
        assert nr.sinterstore('c', 'a', 'b') == 2
        assert nr.smembers('c') == set([b('2'), b('3')])

    def test_sismember(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.sismember('a', '1')
        assert nr.sismember('a', '2')
        assert nr.sismember('a', '3')
        assert not nr.sismember('a', '4')

    def test_smembers(self, nr):
        nr.sadd('a', '1', '2', '3')
        assert nr.smembers('a') == set([b('1'), b('2'), b('3')])

    def test_smove(self, nr):
        nr.sadd('a', 'a1', 'a2')
        nr.sadd('b', 'b1', 'b2')
        assert nr.smove('a', 'b', 'a1')
        assert nr.smembers('a') == set([b('a2')])
        assert nr.smembers('b') == set([b('b1'), b('b2'), b('a1')])

    def test_spop(self, nr):
        s = [b('1'), b('2'), b('3')]
        nr.sadd('a', *s)
        value = nr.spop('a')
        assert value in s
        assert nr.smembers('a') == set(s) - set([value])

    def test_srandmember(self, nr):
        s = [b('1'), b('2'), b('3')]
        nr.sadd('a', *s)
        assert nr.srandmember('a') in s

    @skip_if_server_version_lt('2.6.0')
    def test_srandmember_multi_value(self, nr):
        s = [b('1'), b('2'), b('3')]
        nr.sadd('a', *s)
        randoms = nr.srandmember('a', number=2)
        assert len(randoms) == 2
        assert set(randoms).intersection(s) == set(randoms)

    def test_srem(self, nr):
        nr.sadd('a', '1', '2', '3', '4')
        assert nr.srem('a', '5') == 0
        assert nr.srem('a', '2', '4') == 2
        assert nr.smembers('a') == set([b('1'), b('3')])

    def test_sunion(self, nr):
        nr.sadd('a', '1', '2')
        nr.sadd('b', '2', '3')
        assert nr.sunion('a', 'b') == set([b('1'), b('2'), b('3')])

    def test_sunionstore(self, nr):
        nr.sadd('a', '1', '2')
        nr.sadd('b', '2', '3')
        assert nr.sunionstore('c', 'a', 'b') == 3
        assert nr.smembers('c') == set([b('1'), b('2'), b('3')])

    # SORTED SET COMMANDS
    def test_zadd(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zrange('a', 0, -1) == [b('a1'), b('a2'), b('a3')]

    def test_zcard(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zcard('a') == 3

    def test_zcount(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zcount('a', '-inf', '+inf') == 3
        assert nr.zcount('a', 1, 2) == 2
        assert nr.zcount('a', 10, 20) == 0

    def test_zincrby(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zincrby('a', 'a2') == 3.0
        assert nr.zincrby('a', 'a3', amount=5) == 8.0
        assert nr.zscore('a', 'a2') == 3.0
        assert nr.zscore('a', 'a3') == 8.0

    @skip_if_server_version_lt('2.8.9')
    def test_zlexcount(self, nr):
        nr.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert nr.zlexcount('a', '-', '+') == 7
        assert nr.zlexcount('a', '[b', '[f') == 5

    def test_zinterstore_sum(self, nr):
        nr.zadd('a', a1=1, a2=1, a3=1)
        nr.zadd('b', a1=2, a2=2, a3=2)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zinterstore('d', ['a', 'b', 'c']) == 2
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a3'), 8), (b('a1'), 9)]

    def test_zinterstore_max(self, nr):
        nr.zadd('a', a1=1, a2=1, a3=1)
        nr.zadd('b', a1=2, a2=2, a3=2)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zinterstore('d', ['a', 'b', 'c'], aggregate='MAX') == 2
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a3'), 5), (b('a1'), 6)]

    def test_zinterstore_min(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        nr.zadd('b', a1=2, a2=3, a3=5)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zinterstore('d', ['a', 'b', 'c'], aggregate='MIN') == 2
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a1'), 1), (b('a3'), 3)]

    def test_zinterstore_with_weight(self, nr):
        nr.zadd('a', a1=1, a2=1, a3=1)
        nr.zadd('b', a1=2, a2=2, a3=2)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zinterstore('d', {'a': 1, 'b': 2, 'c': 3}) == 2
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a3'), 20), (b('a1'), 23)]

    def test_zrange(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zrange('a', 0, 1) == [b('a1'), b('a2')]
        assert nr.zrange('a', 1, 2) == [b('a2'), b('a3')]

        # withscores
        assert nr.zrange('a', 0, 1, withscores=True) == \
            [(b('a1'), 1.0), (b('a2'), 2.0)]
        assert nr.zrange('a', 1, 2, withscores=True) == \
            [(b('a2'), 2.0), (b('a3'), 3.0)]

        # custom score function
        assert nr.zrange('a', 0, 1, withscores=True, score_cast_func=int) == \
            [(b('a1'), 1), (b('a2'), 2)]

    @skip_if_server_version_lt('2.8.9')
    def test_zrangebylex(self, nr):
        nr.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert nr.zrangebylex('a', '-', '[c') == [b('a'), b('b'), b('c')]
        assert nr.zrangebylex('a', '-', '(c') == [b('a'), b('b')]
        assert nr.zrangebylex('a', '[aaa', '(g') == \
            [b('b'), b('c'), b('d'), b('e'), b('f')]
        assert nr.zrangebylex('a', '[f', '+') == [b('f'), b('g')]
        assert nr.zrangebylex('a', '-', '+', start=3, num=2) == [b('d'), b('e')]

    @skip_if_server_version_lt('2.9.9')
    def test_zrevrangebylex(self, nr):
        nr.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert nr.zrevrangebylex('a', '[c', '-') == [b('c'), b('b'), b('a')]
        assert nr.zrevrangebylex('a', '(c', '-') == [b('b'), b('a')]
        assert nr.zrevrangebylex('a', '(g', '[aaa') == \
            [b('f'), b('e'), b('d'), b('c'), b('b')]
        assert nr.zrevrangebylex('a', '+', '[f') == [b('g'), b('f')]
        assert nr.zrevrangebylex('a', '+', '-', start=3, num=2) == \
            [b('d'), b('c')]

    def test_zrangebyscore(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert nr.zrangebyscore('a', 2, 4) == [b('a2'), b('a3'), b('a4')]

        # slicing with start/num
        assert nr.zrangebyscore('a', 2, 4, start=1, num=2) == \
            [b('a3'), b('a4')]

        # withscores
        assert nr.zrangebyscore('a', 2, 4, withscores=True) == \
            [(b('a2'), 2.0), (b('a3'), 3.0), (b('a4'), 4.0)]

        # custom score function
        assert nr.zrangebyscore('a', 2, 4, withscores=True,
                               score_cast_func=int) == \
            [(b('a2'), 2), (b('a3'), 3), (b('a4'), 4)]

    def test_zrank(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert nr.zrank('a', 'a1') == 0
        assert nr.zrank('a', 'a2') == 1
        assert nr.zrank('a', 'a6') is None

    def test_zrem(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zrem('a', 'a2') == 1
        assert nr.zrange('a', 0, -1) == [b('a1'), b('a3')]
        assert nr.zrem('a', 'b') == 0
        assert nr.zrange('a', 0, -1) == [b('a1'), b('a3')]

    def test_zrem_multiple_keys(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zrem('a', 'a1', 'a2') == 2
        assert nr.zrange('a', 0, 5) == [b('a3')]

    @skip_if_server_version_lt('2.8.9')
    def test_zremrangebylex(self, nr):
        nr.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
        assert nr.zremrangebylex('a', '-', '[c') == 3
        assert nr.zrange('a', 0, -1) == [b('d'), b('e'), b('f'), b('g')]
        assert nr.zremrangebylex('a', '[f', '+') == 2
        assert nr.zrange('a', 0, -1) == [b('d'), b('e')]
        assert nr.zremrangebylex('a', '[h', '+') == 0
        assert nr.zrange('a', 0, -1) == [b('d'), b('e')]

    def test_zremrangebyrank(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert nr.zremrangebyrank('a', 1, 3) == 3
        assert nr.zrange('a', 0, 5) == [b('a1'), b('a5')]

    def test_zremrangebyscore(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert nr.zremrangebyscore('a', 2, 4) == 3
        assert nr.zrange('a', 0, -1) == [b('a1'), b('a5')]
        assert nr.zremrangebyscore('a', 2, 4) == 0
        assert nr.zrange('a', 0, -1) == [b('a1'), b('a5')]

    def test_zrevrange(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zrevrange('a', 0, 1) == [b('a3'), b('a2')]
        assert nr.zrevrange('a', 1, 2) == [b('a2'), b('a1')]

        # withscores
        assert nr.zrevrange('a', 0, 1, withscores=True) == \
            [(b('a3'), 3.0), (b('a2'), 2.0)]
        assert nr.zrevrange('a', 1, 2, withscores=True) == \
            [(b('a2'), 2.0), (b('a1'), 1.0)]

        # custom score function
        assert nr.zrevrange('a', 0, 1, withscores=True,
                           score_cast_func=int) == \
            [(b('a3'), 3.0), (b('a2'), 2.0)]

    def test_zrevrangebyscore(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert nr.zrevrangebyscore('a', 4, 2) == [b('a4'), b('a3'), b('a2')]

        # slicing with start/num
        assert nr.zrevrangebyscore('a', 4, 2, start=1, num=2) == \
            [b('a3'), b('a2')]

        # withscores
        assert nr.zrevrangebyscore('a', 4, 2, withscores=True) == \
            [(b('a4'), 4.0), (b('a3'), 3.0), (b('a2'), 2.0)]

        # custom score function
        assert nr.zrevrangebyscore('a', 4, 2, withscores=True,
                                  score_cast_func=int) == \
            [(b('a4'), 4), (b('a3'), 3), (b('a2'), 2)]

    def test_zrevrank(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
        assert nr.zrevrank('a', 'a1') == 4
        assert nr.zrevrank('a', 'a2') == 3
        assert nr.zrevrank('a', 'a6') is None

    def test_zscore(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        assert nr.zscore('a', 'a1') == 1.0
        assert nr.zscore('a', 'a2') == 2.0
        assert nr.zscore('a', 'a4') is None

    def test_zunionstore_sum(self, nr):
        nr.zadd('a', a1=1, a2=1, a3=1)
        nr.zadd('b', a1=2, a2=2, a3=2)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zunionstore('d', ['a', 'b', 'c']) == 4
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a2'), 3), (b('a4'), 4), (b('a3'), 8), (b('a1'), 9)]

    def test_zunionstore_max(self, nr):
        nr.zadd('a', a1=1, a2=1, a3=1)
        nr.zadd('b', a1=2, a2=2, a3=2)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zunionstore('d', ['a', 'b', 'c'], aggregate='MAX') == 4
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a2'), 2), (b('a4'), 4), (b('a3'), 5), (b('a1'), 6)]

    def test_zunionstore_min(self, nr):
        nr.zadd('a', a1=1, a2=2, a3=3)
        nr.zadd('b', a1=2, a2=2, a3=4)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zunionstore('d', ['a', 'b', 'c'], aggregate='MIN') == 4
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a1'), 1), (b('a2'), 2), (b('a3'), 3), (b('a4'), 4)]

    def test_zunionstore_with_weight(self, nr):
        nr.zadd('a', a1=1, a2=1, a3=1)
        nr.zadd('b', a1=2, a2=2, a3=2)
        nr.zadd('c', a1=6, a3=5, a4=4)
        assert nr.zunionstore('d', {'a': 1, 'b': 2, 'c': 3}) == 4
        assert nr.zrange('d', 0, -1, withscores=True) == \
            [(b('a2'), 5), (b('a4'), 12), (b('a3'), 20), (b('a1'), 23)]

    # HYPERLOGLOG TESTS
    @skip_if_server_version_lt('2.8.9')
    def test_pfadd(self, nr):
        members = set([b('1'), b('2'), b('3')])
        assert nr.pfadd('a', *members) == 1
        assert nr.pfadd('a', *members) == 0
        assert nr.pfcount('a') == len(members)

    @skip_if_server_version_lt('2.8.9')
    def test_pfcount(self, nr):
        members = set([b('1'), b('2'), b('3')])
        nr.pfadd('a', *members)
        assert nr.pfcount('a') == len(members)
        members_b = set([b('2'), b('3'), b('4')])
        nr.pfadd('b', *members_b)
        assert nr.pfcount('b') == len(members_b)
        assert nr.pfcount('a', 'b') == len(members_b.union(members))

    @skip_if_server_version_lt('2.8.9')
    def test_pfmerge(self, nr):
        mema = set([b('1'), b('2'), b('3')])
        memb = set([b('2'), b('3'), b('4')])
        memc = set([b('5'), b('6'), b('7')])
        nr.pfadd('a', *mema)
        nr.pfadd('b', *memb)
        nr.pfadd('c', *memc)
        nr.pfmerge('d', 'c', 'a')
        assert nr.pfcount('d') == 6
        nr.pfmerge('d', 'b')
        assert nr.pfcount('d') == 7

    # HASH COMMANDS
    def test_hget_and_hset(self, nr):
        nr.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert nr.hget('a', '1') == b('1')
        assert nr.hget('a', '2') == b('2')
        assert nr.hget('a', '3') == b('3')

        # field was updated, redis returns 0
        assert nr.hset('a', '2', 5) == 0
        assert nr.hget('a', '2') == b('5')

        # field is new, redis returns 1
        assert nr.hset('a', '4', 4) == 1
        assert nr.hget('a', '4') == b('4')

        # key inside of hash that doesn't exist returns null value
        assert nr.hget('a', 'b') is None

    def test_hdel(self, nr):
        nr.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert nr.hdel('a', '2') == 1
        assert nr.hget('a', '2') is None
        assert nr.hdel('a', '1', '3') == 2
        assert nr.hlen('a') == 0

    def test_hexists(self, nr):
        nr.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert nr.hexists('a', '1')
        assert not nr.hexists('a', '4')

    def test_hgetall(self, nr):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        nr.hmset('a', h)
        assert nr.hgetall('a') == h

    def test_hincrby(self, nr):
        assert nr.hincrby('a', '1') == 1
        assert nr.hincrby('a', '1', amount=2) == 3
        assert nr.hincrby('a', '1', amount=-2) == 1

    @skip_if_server_version_lt('2.6.0')
    def test_hincrbyfloat(self, nr):
        assert nr.hincrbyfloat('a', '1') == 1.0
        assert nr.hincrbyfloat('a', '1') == 2.0
        assert nr.hincrbyfloat('a', '1', 1.2) == 3.2

    def test_hkeys(self, nr):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        nr.hmset('a', h)
        local_keys = list(iterkeys(h))
        remote_keys = nr.hkeys('a')
        assert (sorted(local_keys) == sorted(remote_keys))

    def test_hlen(self, nr):
        nr.hmset('a', {'1': 1, '2': 2, '3': 3})
        assert nr.hlen('a') == 3

    def test_hmget(self, nr):
        assert nr.hmset('a', {'a': 1, 'b': 2, 'c': 3})
        assert nr.hmget('a', 'a', 'b', 'c') == [b('1'), b('2'), b('3')]

    def test_hmset(self, nr):
        h = {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        assert nr.hmset('a', h)
        assert nr.hgetall('a') == h

    def test_hsetnx(self, nr):
        # Initially set the hash field
        assert nr.hsetnx('a', '1', 1)
        assert nr.hget('a', '1') == b('1')
        assert not nr.hsetnx('a', '1', 2)
        assert nr.hget('a', '1') == b('1')

    def test_hvals(self, nr):
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        nr.hmset('a', h)
        local_vals = list(itervalues(h))
        remote_vals = nr.hvals('a')
        assert sorted(local_vals) == sorted(remote_vals)

    # SORT
    def test_sort_basic(self, nr):
        nr.rpush('a', '3', '2', '1', '4')
        assert nr.sort('a') == [b('1'), b('2'), b('3'), b('4')]

    def test_sort_limited(self, nr):
        nr.rpush('a', '3', '2', '1', '4')
        assert nr.sort('a', start=1, num=2) == [b('2'), b('3')]

    def test_sort_by(self, nr):
        nr['score:1'] = 8
        nr['score:2'] = 3
        nr['score:3'] = 5
        nr.rpush('a', '3', '2', '1')
        assert nr.sort('a', by='score:*') == [b('2'), b('3'), b('1')]

    def test_sort_get(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr.rpush('a', '2', '3', '1')
        assert nr.sort('a', get='user:*') == [b('u1'), b('u2'), b('u3')]

    def test_sort_get_multi(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr.rpush('a', '2', '3', '1')
        assert nr.sort('a', get=('user:*', '#')) == \
            [b('u1'), b('1'), b('u2'), b('2'), b('u3'), b('3')]

    def test_sort_get_groups_two(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr.rpush('a', '2', '3', '1')
        assert nr.sort('a', get=('user:*', '#'), groups=True) == \
            [(b('u1'), b('1')), (b('u2'), b('2')), (b('u3'), b('3'))]

    def test_sort_groups_string_get(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr.rpush('a', '2', '3', '1')
        with pytest.raises(exceptions.DataError):
            nr.sort('a', get='user:*', groups=True)

    def test_sort_groups_just_one_get(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr.rpush('a', '2', '3', '1')
        with pytest.raises(exceptions.DataError):
            nr.sort('a', get=['user:*'], groups=True)

    def test_sort_groups_no_get(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr.rpush('a', '2', '3', '1')
        with pytest.raises(exceptions.DataError):
            nr.sort('a', groups=True)

    def test_sort_groups_three_gets(self, nr):
        nr['user:1'] = 'u1'
        nr['user:2'] = 'u2'
        nr['user:3'] = 'u3'
        nr['door:1'] = 'd1'
        nr['door:2'] = 'd2'
        nr['door:3'] = 'd3'
        nr.rpush('a', '2', '3', '1')
        assert nr.sort('a', get=('user:*', 'door:*', '#'), groups=True) == \
            [
                (b('u1'), b('d1'), b('1')),
                (b('u2'), b('d2'), b('2')),
                (b('u3'), b('d3'), b('3'))
        ]

    def test_sort_desc(self, nr):
        nr.rpush('a', '2', '3', '1')
        assert nr.sort('a', desc=True) == [b('3'), b('2'), b('1')]

    def test_sort_alpha(self, nr):
        nr.rpush('a', 'e', 'c', 'b', 'd', 'a')
        assert nr.sort('a', alpha=True) == \
            [b('a'), b('b'), b('c'), b('d'), b('e')]

    def test_sort_store(self, nr):
        nr.rpush('a', '2', '3', '1')
        assert nr.sort('a', store='sorted_values') == 3
        assert nr.lrange('sorted_values', 0, -1) == [b('1'), b('2'), b('3')]

    def test_sort_all_options(self, nr):
        nr['user:1:username'] = 'zeus'
        nr['user:2:username'] = 'titan'
        nr['user:3:username'] = 'hermes'
        nr['user:4:username'] = 'hercules'
        nr['user:5:username'] = 'apollo'
        nr['user:6:username'] = 'athena'
        nr['user:7:username'] = 'hades'
        nr['user:8:username'] = 'dionysus'

        nr['user:1:favorite_drink'] = 'yuengling'
        nr['user:2:favorite_drink'] = 'rum'
        nr['user:3:favorite_drink'] = 'vodka'
        nr['user:4:favorite_drink'] = 'milk'
        nr['user:5:favorite_drink'] = 'pinot noir'
        nr['user:6:favorite_drink'] = 'water'
        nr['user:7:favorite_drink'] = 'gin'
        nr['user:8:favorite_drink'] = 'apple juice'

        nr.rpush('gods', '5', '8', '3', '1', '2', '7', '6', '4')
        num = nr.sort('gods', start=2, num=4, by='user:*:username',
                     get='user:*:favorite_drink', desc=True, alpha=True,
                     store='sorted')
        assert num == 4
        assert nr.lrange('sorted', 0, 10) == \
            [b('vodka'), b('milk'), b('gin'), b('apple juice')]

    def test_cluster_addslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('ADDSLOTS', 1) is True

    def test_cluster_count_failure_reports(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster(
            'COUNT-FAILURE-REPORTS', 'node'), int)

    def test_cluster_countkeysinslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster(
            'COUNTKEYSINSLOT', 2), int)

    def test_cluster_delslots(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('DELSLOTS', 1) is True

    def test_cluster_failover(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('FAILOVER', 1) is True

    def test_cluster_forget(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('FORGET', 1) is True

    def test_cluster_info(self, mock_cluster_resp_info):
        assert isinstance(mock_cluster_resp_info.cluster('info'), dict)

    def test_cluster_keyslot(self, mock_cluster_resp_int):
        assert isinstance(mock_cluster_resp_int.cluster(
            'keyslot', 'asdf'), int)

    def test_cluster_meet(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('meet', 'ip', 'port', 1) is True

    def test_cluster_nodes(self, mock_cluster_resp_nodes):
        assert isinstance(mock_cluster_resp_nodes.cluster('nodes'), dict)

    def test_cluster_replicate(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('replicate', 'nodeid') is True

    def test_cluster_reset(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('reset', 'hard') is True

    def test_cluster_saveconfig(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('saveconfig') is True

    def test_cluster_setslot(self, mock_cluster_resp_ok):
        assert mock_cluster_resp_ok.cluster('setslot', 1,
                                            'IMPORTING', 'nodeid') is True

    def test_cluster_slaves(self, mock_cluster_resp_slaves):
        assert isinstance(mock_cluster_resp_slaves.cluster(
            'slaves', 'nodeid'), dict)

    # GEO COMMANDS
    @skip_if_server_version_lt('3.2.0')
    def test_geoadd(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        assert nr.geoadd('barcelona', *values) == 2
        assert nr.zcard('barcelona') == 2

    @skip_if_server_version_lt('3.2.0')
    def test_geoadd_invalid_params(self, nr):
        with pytest.raises(exceptions.RedisError):
            nr.geoadd('barcelona', *(1, 2))

    @skip_if_server_version_lt('3.2.0')
    def test_geodist(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        assert nr.geoadd('barcelona', *values) == 2
        assert nr.geodist('barcelona', 'place1', 'place2') == 3067.4157

    @skip_if_server_version_lt('3.2.0')
    def test_geodist_units(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.geodist('barcelona', 'place1', 'place2', 'km') == 3.0674

    @skip_if_server_version_lt('3.2.0')
    def test_geodist_invalid_units(self, nr):
        with pytest.raises(exceptions.RedisError):
            assert nr.geodist('x', 'y', 'z', 'inches')

    @skip_if_server_version_lt('3.2.0')
    def test_geohash(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.geohash('barcelona', 'place1', 'place2') ==\
            ['sp3e9yg3kd0', 'sp3e9cbc3t0']

    @skip_if_server_version_lt('3.2.0')
    def test_geopos(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        # redis uses 52 bits precision, hereby small errors may be introduced.
        assert nr.geopos('barcelona', 'place1', 'place2') ==\
            [(2.19093829393386841, 41.43379028184083523),
             (2.18737632036209106, 41.40634178640635099)]

    @skip_if_server_version_lt('3.2.0')
    def test_georadius(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.georadius('barcelona', 2.191, 41.433, 1000) == ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_no_values(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.georadius('barcelona', 1, 2, 1000) == []

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_units(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.georadius('barcelona', 2.191, 41.433, 1, unit='km') ==\
            ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_with(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)

        # test a bunch of combinations to test the parse response
        # function.
        assert nr.georadius('barcelona', 2.191, 41.433, 1, unit='km',
                           withdist=True, withcoord=True, withhash=True) ==\
            [['place1', 0.0881, 3471609698139488,
              (2.19093829393386841, 41.43379028184083523)]]

        assert nr.georadius('barcelona', 2.191, 41.433, 1, unit='km',
                           withdist=True, withcoord=True) ==\
            [['place1', 0.0881,
              (2.19093829393386841, 41.43379028184083523)]]

        assert nr.georadius('barcelona', 2.191, 41.433, 1, unit='km',
                           withhash=True, withcoord=True) ==\
            [['place1', 3471609698139488,
              (2.19093829393386841, 41.43379028184083523)]]

        # test no values.
        assert nr.georadius('barcelona', 2, 1, 1, unit='km',
                           withdist=True, withcoord=True, withhash=True) == []

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_count(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.georadius('barcelona', 2.191, 41.433, 3000, count=1) ==\
            ['place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_sort(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.georadius('barcelona', 2.191, 41.433, 3000, sort='ASC') ==\
            ['place1', 'place2']
        assert nr.georadius('barcelona', 2.191, 41.433, 3000, sort='DESC') ==\
            ['place2', 'place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_store(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        nr.georadius('barcelona', 2.191, 41.433, 1000, store='places_barcelona')
        assert nr.zrange('places_barcelona', 0, -1) == [b'place1']

    @skip_if_server_version_lt('3.2.0')
    def test_georadius_store_dist(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        nr.georadius('barcelona', 2.191, 41.433, 1000,
                    store_dist='places_barcelona')
        # instead of save the geo score, the distance is saved.
        assert nr.zscore('places_barcelona', 'place1') == 88.05060698409301

    @skip_if_server_version_lt('3.2.0')
    def test_georadiusmember(self, nr):
        values = (2.1909389952632, 41.433791470673, 'place1') +\
                 (2.1873744593677, 41.406342043777, 'place2')

        nr.geoadd('barcelona', *values)
        assert nr.georadiusbymember('barcelona', 'place1', 4000) ==\
            ['place2', 'place1']
        assert nr.georadiusbymember('barcelona', 'place1', 10) == ['place1']

        assert nr.georadiusbymember('barcelona', 'place1', 4000,
                                   withdist=True, withcoord=True,
                                   withhash=True) ==\
            [['place2', 3067.4157, 3471609625421029,
                (2.187376320362091, 41.40634178640635)],
             ['place1', 0.0, 3471609698139488,
                 (2.1909382939338684, 41.433790281840835)]]


class TestStrictCommands(object):

    def test_strict_zadd(self, nsr):
        nsr.zadd('a', 1.0, 'a1', 2.0, 'a2', a3=3.0)
        assert nsr.zrange('a', 0, -1, withscores=True) == \
            [(b('a1'), 1.0), (b('a2'), 2.0), (b('a3'), 3.0)]

    def test_strict_lrem(self, nsr):
        nsr.rpush('a', 'a1', 'a2', 'a3', 'a1')
        nsr.lrem('a', 0, 'a1')
        assert nsr.lrange('a', 0, -1) == [b('a2'), b('a3')]

    def test_strict_setex(self, nsr):
        assert nsr.setex('a', 60, '1')
        assert nsr['a'] == b('1')
        assert 0 < nsr.ttl('a') <= 60

    def test_strict_ttl(self, nsr):
        assert not nsr.expire('a', 10)
        nsr['a'] = '1'
        assert nsr.expire('a', 10)
        assert 0 < nsr.ttl('a') <= 10
        assert nsr.persist('a')
        assert nsr.ttl('a') == -1

    @skip_if_server_version_lt('2.6.0')
    def test_strict_pttl(self, nsr):
        assert not nsr.pexpire('a', 10000)
        nsr['a'] = '1'
        assert nsr.pexpire('a', 10000)
        assert 0 < nsr.pttl('a') <= 10000
        assert nsr.persist('a')
        assert nsr.pttl('a') == -1


class TestBinarySave(object):

    def test_binary_get_set(self, nr):
        assert nr.set(' foo bar ', '123')
        assert nr.get(' foo bar ') == b('123')

        assert nr.set(' foo\r\nbar\r\n ', '456')
        assert nr.get(' foo\r\nbar\r\n ') == b('456')

        assert nr.set(' \r\n\t\x07\x13 ', '789')
        assert nr.get(' \r\n\t\x07\x13 ') == b('789')

        assert sorted(nr.keys('*')) == \
            [b(' \r\n\t\x07\x13 '), b(' foo\r\nbar\r\n '), b(' foo bar ')]

        assert nr.delete(' foo bar ')
        assert nr.delete(' foo\r\nbar\r\n ')
        assert nr.delete(' \r\n\t\x07\x13 ')

    def test_binary_lists(self, nr):
        mapping = {
            b('foo bar'): [b('1'), b('2'), b('3')],
            b('foo\r\nbar\r\n'): [b('4'), b('5'), b('6')],
            b('foo\tbar\x07'): [b('7'), b('8'), b('9')],
        }
        # fill in lists
        for key, value in iteritems(mapping):
            nr.rpush(key, *value)

        # check that KEYS returns all the keys as they are
        assert sorted(nr.keys('*')) == sorted(list(iterkeys(mapping)))

        # check that it is possible to get list content by key name
        for key, value in iteritems(mapping):
            assert nr.lrange(key, 0, -1) == value

    def test_22_info(self, nr):
        """
        Older Redis versions contained 'allocation_stats' in INFO that
        was the cause of a number of bugs when parsing.
        """
        info = "allocation_stats:6=1,7=1,8=7141,9=180,10=92,11=116,12=5330," \
               "13=123,14=3091,15=11048,16=225842,17=1784,18=814,19=12020," \
               "20=2530,21=645,22=15113,23=8695,24=142860,25=318,26=3303," \
               "27=20561,28=54042,29=37390,30=1884,31=18071,32=31367,33=160," \
               "34=169,35=201,36=10155,37=1045,38=15078,39=22985,40=12523," \
               "41=15588,42=265,43=1287,44=142,45=382,46=945,47=426,48=171," \
               "49=56,50=516,51=43,52=41,53=46,54=54,55=75,56=647,57=332," \
               "58=32,59=39,60=48,61=35,62=62,63=32,64=221,65=26,66=30," \
               "67=36,68=41,69=44,70=26,71=144,72=169,73=24,74=37,75=25," \
               "76=42,77=21,78=126,79=374,80=27,81=40,82=43,83=47,84=46," \
               "85=114,86=34,87=37,88=7240,89=34,90=38,91=18,92=99,93=20," \
               "94=18,95=17,96=15,97=22,98=18,99=69,100=17,101=22,102=15," \
               "103=29,104=39,105=30,106=70,107=22,108=21,109=26,110=52," \
               "111=45,112=33,113=67,114=41,115=44,116=48,117=53,118=54," \
               "119=51,120=75,121=44,122=57,123=44,124=66,125=56,126=52," \
               "127=81,128=108,129=70,130=50,131=51,132=53,133=45,134=62," \
               "135=12,136=13,137=7,138=15,139=21,140=11,141=20,142=6,143=7," \
               "144=11,145=6,146=16,147=19,148=1112,149=1,151=83,154=1," \
               "155=1,156=1,157=1,160=1,161=1,162=2,166=1,169=1,170=1,171=2," \
               "172=1,174=1,176=2,177=9,178=34,179=73,180=30,181=1,185=3," \
               "187=1,188=1,189=1,192=1,196=1,198=1,200=1,201=1,204=1,205=1," \
               "207=1,208=1,209=1,214=2,215=31,216=78,217=28,218=5,219=2," \
               "220=1,222=1,225=1,227=1,234=1,242=1,250=1,252=1,253=1," \
               ">=256=203"
        parsed = parse_info(info)
        assert 'allocation_stats' in parsed
        assert '6' in parsed['allocation_stats']
        assert '>=256' in parsed['allocation_stats']

    def test_large_responses(self, nr):
        "The PythonParser has some special cases for return values > 1MB"
        # load up 5MB of data into a key
        data = ''.join([ascii_letters] * (5000000 // len(ascii_letters)))
        nr['a'] = data
        assert nr['a'] == b(data)

    def test_floating_point_encoding(self, nr):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        nr.zadd('a', 'a1', timestamp)
        assert nr.zscore('a', 'a1') == timestamp
