from distutils.version import StrictVersion
import unittest
import datetime
import time
import binascii

from redis._compat import (unichr, u, b, ascii_letters, iteritems, dictkeys,
                           dictvalues)
from redis.client import parse_info
import redis

class ServerCommandsTestCase(unittest.TestCase):
    def get_client(self, cls=redis.Redis):
        return cls(host='localhost', port=6379, db=9)

    def setUp(self):
        self.client = self.get_client()
        self.client.flushdb()

    def tearDown(self):
        self.client.flushdb()
        self.client.connection_pool.disconnect()

    def test_response_callbacks(self):
        self.assertEquals(
            self.client.response_callbacks,
            redis.Redis.RESPONSE_CALLBACKS)
        self.assertNotEquals(
            id(self.client.response_callbacks),
            id(redis.Redis.RESPONSE_CALLBACKS))
        self.client.set_response_callback('GET', lambda x: 'static')
        self.client.set('a', 'foo')
        self.assertEquals(self.client.get('a'), 'static')

    # GENERAL SERVER COMMANDS
    def test_dbsize(self):
        self.client['a'] = 'foo'
        self.client['b'] = 'bar'
        self.assertEquals(self.client.dbsize(), 2)

    def test_get_and_set(self):
        # get and set can't be tested independently of each other
        client = redis.Redis(host='localhost', port=6379, db=9)
        self.assertEquals(client.get('a'), None)
        byte_string = b('value')
        integer = 5
        unicode_string = unichr(3456) + u('abcd') + unichr(3421)
        self.assert_(client.set('byte_string', byte_string))
        self.assert_(client.set('integer', 5))
        self.assert_(client.set('unicode_string', unicode_string))
        self.assertEquals(client.get('byte_string'), byte_string)
        self.assertEquals(client.get('integer'), b(str(integer)))
        self.assertEquals(
            client.get('unicode_string').decode('utf-8'),
            unicode_string)

    def test_getitem_and_setitem(self):
        self.client['a'] = 'bar'
        self.assertEquals(self.client['a'], b('bar'))
        self.assertRaises(KeyError, self.client.__getitem__, 'b')

    def test_delete(self):
        self.assertEquals(self.client.delete('a'), False)
        self.client['a'] = 'foo'
        self.assertEquals(self.client.delete('a'), True)

    def test_delitem(self):
        self.client['a'] = 'foo'
        del self.client['a']
        self.assertEquals(self.client.get('a'), None)

    def test_client_list(self):
        clients = self.client.client_list()
        self.assert_(isinstance(clients[0], dict))
        self.assert_('addr' in clients[0])

    def test_config_get(self):
        data = self.client.config_get()
        self.assert_('maxmemory' in data)
        self.assert_(data['maxmemory'].isdigit())

    def test_config_set(self):
        data = self.client.config_get()
        rdbname = data['dbfilename']
        self.assert_(self.client.config_set('dbfilename', 'redis_py_test.rdb'))
        self.assertEquals(
            self.client.config_get()['dbfilename'],
            'redis_py_test.rdb'
        )
        self.assert_(self.client.config_set('dbfilename', rdbname))
        self.assertEquals(self.client.config_get()['dbfilename'], rdbname)

    def test_debug_object(self):
        self.client['a'] = 'foo'
        debug_info = self.client.debug_object('a')
        self.assert_(len(debug_info) > 0)
        self.assertEquals(debug_info['refcount'], 1)
        self.assert_(debug_info['serializedlength'] > 0)
        self.client.rpush('b', 'a1')
        debug_info = self.client.debug_object('a')

    def test_echo(self):
        self.assertEquals(self.client.echo('foo bar'), b('foo bar'))

    def test_info(self):
        self.client['a'] = 'foo'
        self.client['b'] = 'bar'
        info = self.client.info()
        self.assert_(isinstance(info, dict))
        self.assertEquals(info['db9']['keys'], 2)

    def test_lastsave(self):
        self.assert_(isinstance(self.client.lastsave(), datetime.datetime))

    def test_object(self):
        self.client['a'] = 'foo'
        self.assert_(isinstance(self.client.object('refcount', 'a'), int))
        self.assert_(isinstance(self.client.object('idletime', 'a'), int))
        self.assertEquals(self.client.object('encoding', 'a'), b('raw'))

    def test_ping(self):
        self.assertEquals(self.client.ping(), True)

    def test_time(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        t = self.client.time()
        self.assertEquals(len(t), 2)
        self.assert_(isinstance(t[0], int))
        self.assert_(isinstance(t[1], int))

    # KEYS
    def test_append(self):
        # invalid key type
        self.client.rpush('a', 'a1')
        self.assertRaises(redis.ResponseError, self.client.append, 'a', 'a1')
        del self.client['a']
        # real logic
        self.assertEquals(self.client.append('a', 'a1'), 2)
        self.assertEquals(self.client['a'], b('a1'))
        self.assert_(self.client.append('a', 'a2'), 4)
        self.assertEquals(self.client['a'], b('a1a2'))

    def test_getrange(self):
        self.client['a'] = 'foo'
        self.assertEquals(self.client.getrange('a', 0, 0), b('f'))
        self.assertEquals(self.client.getrange('a', 0, 2), b('foo'))
        self.assertEquals(self.client.getrange('a', 3, 4), b(''))

    def test_decr(self):
        self.assertEquals(self.client.decr('a'), -1)
        self.assertEquals(self.client['a'], b('-1'))
        self.assertEquals(self.client.decr('a'), -2)
        self.assertEquals(self.client['a'], b('-2'))
        self.assertEquals(self.client.decr('a', amount=5), -7)
        self.assertEquals(self.client['a'], b('-7'))

    def test_exists(self):
        self.assertEquals(self.client.exists('a'), False)
        self.client['a'] = 'foo'
        self.assertEquals(self.client.exists('a'), True)

    def test_expire(self):
        self.assertEquals(self.client.expire('a', 10), False)
        self.client['a'] = 'foo'
        self.assertEquals(self.client.expire('a', 10), True)
        self.assertEquals(self.client.ttl('a'), 10)
        self.assertEquals(self.client.persist('a'), True)
        self.assertEquals(self.client.ttl('a'), None)

    def test_expireat(self):
        expire_at = datetime.datetime.now() + datetime.timedelta(minutes=1)
        self.assertEquals(self.client.expireat('a', expire_at), False)
        self.client['a'] = 'foo'
        # expire at in unix time
        expire_at_seconds = int(time.mktime(expire_at.timetuple()))
        self.assertEquals(self.client.expireat('a', expire_at_seconds), True)
        self.assertEquals(self.client.ttl('a'), 60)
        # expire at given a datetime object
        self.client['b'] = 'bar'
        self.assertEquals(self.client.expireat('b', expire_at), True)
        self.assertEquals(self.client.ttl('b'), 60)

    def test_pexpire(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        self.assertEquals(self.client.pexpire('a', 10000), False)
        self.client['a'] = 'foo'
        self.assertEquals(self.client.pexpire('a', 10000), True)
        self.assert_(self.client.pttl('a') <= 10000)
        self.assertEquals(self.client.persist('a'), True)
        self.assertEquals(self.client.pttl('a'), None)

    def test_pexpireat(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        expire_at = datetime.datetime.now() + datetime.timedelta(minutes=1)
        self.assertEquals(self.client.pexpireat('a', expire_at), False)
        self.client['a'] = 'foo'
        # expire at in unix time (milliseconds)
        expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
        self.assertEquals(self.client.pexpireat('a', expire_at_seconds), True)
        self.assert_(self.client.ttl('a') <= 60)
        # expire at given a datetime object
        self.client['b'] = 'bar'
        self.assertEquals(self.client.pexpireat('b', expire_at), True)
        self.assert_(self.client.ttl('b') <= 60)

    def test_get_set_bit(self):
        self.assertEquals(self.client.getbit('a', 5), False)
        self.assertEquals(self.client.setbit('a', 5, True), False)
        self.assertEquals(self.client.getbit('a', 5), True)
        self.assertEquals(self.client.setbit('a', 4, False), False)
        self.assertEquals(self.client.getbit('a', 4), False)
        self.assertEquals(self.client.setbit('a', 4, True), False)
        self.assertEquals(self.client.setbit('a', 5, True), True)
        self.assertEquals(self.client.getbit('a', 4), True)
        self.assertEquals(self.client.getbit('a', 5), True)

    def test_bitcount(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        self.client.setbit('a', 5, True)
        self.assertEquals(self.client.bitcount('a'), 1)
        self.client.setbit('a', 6, True)
        self.assertEquals(self.client.bitcount('a'), 2)
        self.client.setbit('a', 5, False)
        self.assertEquals(self.client.bitcount('a'), 1)
        self.client.setbit('a', 9, True)
        self.client.setbit('a', 17, True)
        self.client.setbit('a', 25, True)
        self.client.setbit('a', 33, True)
        self.assertEquals(self.client.bitcount('a'), 5)
        self.assertEquals(self.client.bitcount('a', 2, 3), 2)
        self.assertEquals(self.client.bitcount('a', 2, -1), 3)
        self.assertEquals(self.client.bitcount('a', -2, -1), 2)
        self.assertEquals(self.client.bitcount('a', 1, 1), 1)

    def test_bitop_not_empty_string(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        self.client.set('a', '')
        self.client.bitop('not', 'r', 'a')
        self.assertEquals(self.client.get('r'), None)

    def test_bitop_not(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        test_str = b('\xAA\x00\xFF\x55')
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        self.client.set('a', test_str)
        self.client.bitop('not', 'r', 'a')
        self.assertEquals(
            int(binascii.hexlify(self.client.get('r')), 16),
            correct)

    def test_bitop_not_in_place(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        test_str = b('\xAA\x00\xFF\x55')
        correct = ~0xAA00FF55 & 0xFFFFFFFF
        self.client.set('a', test_str)
        self.client.bitop('not', 'a', 'a')
        self.assertEquals(
            int(binascii.hexlify(self.client.get('a')), 16),
            correct)

    def test_bitop_single_string(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        test_str = b('\x01\x02\xFF')
        self.client.set('a', test_str)
        self.client.bitop('and', 'res1', 'a')
        self.client.bitop('or', 'res2', 'a')
        self.client.bitop('xor', 'res3', 'a')
        self.assertEquals(self.client.get('res1'), test_str)
        self.assertEquals(self.client.get('res2'), test_str)
        self.assertEquals(self.client.get('res3'), test_str)

    def test_bitop_string_operands(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        self.client.set('a', b('\x01\x02\xFF\xFF'))
        self.client.set('b', b('\x01\x02\xFF'))
        self.client.bitop('and', 'res1', 'a', 'b')
        self.client.bitop('or', 'res2', 'a', 'b')
        self.client.bitop('xor', 'res3', 'a', 'b')
        self.assertEquals(
            int(binascii.hexlify(self.client.get('res1')), 16),
            0x0102FF00)
        self.assertEquals(
            int(binascii.hexlify(self.client.get('res2')), 16),
            0x0102FFFF)
        self.assertEquals(
            int(binascii.hexlify(self.client.get('res3')), 16),
            0x000000FF)

    def test_getset(self):
        self.assertEquals(self.client.getset('a', 'foo'), None)
        self.assertEquals(self.client.getset('a', 'bar'), b('foo'))

    def test_incr(self):
        self.assertEquals(self.client.incr('a'), 1)
        self.assertEquals(self.client['a'], b('1'))
        self.assertEquals(self.client.incr('a'), 2)
        self.assertEquals(self.client['a'], b('2'))
        self.assertEquals(self.client.incr('a', amount=5), 7)
        self.assertEquals(self.client['a'], b('7'))

    def test_incrbyfloat(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        self.assertEquals(self.client.incrbyfloat('a'), 1.0)
        self.assertEquals(self.client['a'], b('1'))
        self.assertEquals(self.client.incrbyfloat('a', 1.1), 2.1)
        self.assertEquals(float(self.client['a']), float(2.1))

    def test_keys(self):
        self.assertEquals(self.client.keys(), [])
        keys = set([b('test_a'), b('test_b'), b('testc')])
        for key in keys:
            self.client[key] = 1
        self.assertEquals(
            set(self.client.keys(pattern='test_*')),
            keys - set([b('testc')]))
        self.assertEquals(set(self.client.keys(pattern='test*')), keys)

    def test_mget(self):
        self.assertEquals(self.client.mget(['a', 'b']), [None, None])
        self.client['a'] = '1'
        self.client['b'] = '2'
        self.client['c'] = '3'
        self.assertEquals(
            self.client.mget(['a', 'other', 'b', 'c']),
            [b('1'), None, b('2'), b('3')])

    def test_mset(self):
        d = {'a': '1', 'b': '2', 'c': '3'}
        self.assert_(self.client.mset(d))
        for k, v in iteritems(d):
            self.assertEquals(self.client[k], b(v))

    def test_msetnx(self):
        d = {'a': '1', 'b': '2', 'c': '3'}
        self.assert_(self.client.msetnx(d))
        d2 = {'a': 'x', 'd': '4'}
        self.assert_(not self.client.msetnx(d2))
        for k, v in iteritems(d):
            self.assertEquals(self.client[k], b(v))
        self.assertEquals(self.client.get('d'), None)

    def test_randomkey(self):
        self.assertEquals(self.client.randomkey(), None)
        self.client['a'] = '1'
        self.client['b'] = '2'
        self.client['c'] = '3'
        self.assert_(self.client.randomkey() in (b('a'), b('b'), b('c')))

    def test_rename(self):
        self.client['a'] = '1'
        self.assert_(self.client.rename('a', 'b'))
        self.assertEquals(self.client.get('a'), None)
        self.assertEquals(self.client['b'], b('1'))

    def test_renamenx(self):
        self.client['a'] = '1'
        self.client['b'] = '2'
        self.assert_(not self.client.renamenx('a', 'b'))
        self.assertEquals(self.client['a'], b('1'))
        self.assertEquals(self.client['b'], b('2'))

    def test_setex(self):
        self.assertEquals(self.client.setex('a', '1', 60), True)
        self.assertEquals(self.client['a'], b('1'))
        self.assertEquals(self.client.ttl('a'), 60)

    def test_setnx(self):
        self.assert_(self.client.setnx('a', '1'))
        self.assertEquals(self.client['a'], b('1'))
        self.assert_(not self.client.setnx('a', '2'))
        self.assertEquals(self.client['a'], b('1'))

    def test_setrange(self):
        self.assertEquals(self.client.setrange('a', 5, 'abcdef'), 11)
        self.assertEquals(self.client['a'], b('\0\0\0\0\0abcdef'))
        self.client['a'] = 'Hello World'
        self.assertEquals(self.client.setrange('a', 6, 'Redis'), 11)
        self.assertEquals(self.client['a'], b('Hello Redis'))

    def test_strlen(self):
        self.client['a'] = 'abcdef'
        self.assertEquals(self.client.strlen('a'), 6)

    def test_substr(self):
        # invalid key type
        self.client.rpush('a', 'a1')
        self.assertRaises(redis.ResponseError, self.client.substr, 'a', 0)
        del self.client['a']
        # real logic
        self.client['a'] = 'abcdefghi'
        self.assertEquals(self.client.substr('a', 0), b('abcdefghi'))
        self.assertEquals(self.client.substr('a', 2), b('cdefghi'))
        self.assertEquals(self.client.substr('a', 3, 5), b('def'))
        self.assertEquals(self.client.substr('a', 3, -2), b('defgh'))
        self.client['a'] = 123456  # does substr work with ints?
        self.assertEquals(self.client.substr('a', 2, -2), b('345'))

    def test_type(self):
        self.assertEquals(self.client.type('a'), b('none'))
        self.client['a'] = '1'
        self.assertEquals(self.client.type('a'), b('string'))
        del self.client['a']
        self.client.lpush('a', '1')
        self.assertEquals(self.client.type('a'), b('list'))
        del self.client['a']
        self.client.sadd('a', '1')
        self.assertEquals(self.client.type('a'), b('set'))
        del self.client['a']
        self.client.zadd('a', **{'1': 1})
        self.assertEquals(self.client.type('a'), b('zset'))

    # LISTS
    def make_list(self, name, l):
        for i in l:
            self.client.rpush(name, i)

    def test_blpop(self):
        self.make_list('a', 'ab')
        self.make_list('b', 'cd')
        self.assertEquals(
            self.client.blpop(['b', 'a'], timeout=1),
            (b('b'), b('c')))
        self.assertEquals(
            self.client.blpop(['b', 'a'], timeout=1),
            (b('b'), b('d')))
        self.assertEquals(
            self.client.blpop(['b', 'a'], timeout=1),
            (b('a'), b('a')))
        self.assertEquals(
            self.client.blpop(['b', 'a'], timeout=1),
            (b('a'), b('b')))
        self.assertEquals(self.client.blpop(['b', 'a'], timeout=1), None)
        self.make_list('c', 'a')
        self.assertEquals(self.client.blpop('c', timeout=1), (b('c'), b('a')))

    def test_brpop(self):
        self.make_list('a', 'ab')
        self.make_list('b', 'cd')
        self.assertEquals(
            self.client.brpop(['b', 'a'], timeout=1),
            (b('b'), b('d')))
        self.assertEquals(
            self.client.brpop(['b', 'a'], timeout=1),
            (b('b'), b('c')))
        self.assertEquals(
            self.client.brpop(['b', 'a'], timeout=1),
            (b('a'), b('b')))
        self.assertEquals(
            self.client.brpop(['b', 'a'], timeout=1),
            (b('a'), b('a')))
        self.assertEquals(self.client.brpop(['b', 'a'], timeout=1), None)
        self.make_list('c', 'a')
        self.assertEquals(self.client.brpop('c', timeout=1), (b('c'), b('a')))

    def test_brpoplpush(self):
        self.make_list('a', '12')
        self.make_list('b', '34')
        self.assertEquals(self.client.brpoplpush('a', 'b'), b('2'))
        self.assertEquals(self.client.brpoplpush('a', 'b'), b('1'))
        self.assertEquals(self.client.brpoplpush('a', 'b', timeout=1), None)
        self.assertEquals(self.client.lrange('a', 0, -1), [])
        self.assertEquals(
            self.client.lrange('b', 0, -1),
            [b('1'), b('2'), b('3'), b('4')])

    def test_lindex(self):
        # no key
        self.assertEquals(self.client.lindex('a', '0'), None)
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lindex, 'a', '0')
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assertEquals(self.client.lindex('a', '0'), b('a'))
        self.assertEquals(self.client.lindex('a', '1'), b('b'))
        self.assertEquals(self.client.lindex('a', '2'), b('c'))

    def test_linsert(self):
        # no key
        self.assertEquals(self.client.linsert('a', 'after', 'x', 'y'), 0)
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(
            redis.ResponseError, self.client.linsert, 'a', 'after', 'x', 'y'
        )
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assertEquals(self.client.linsert('a', 'after', 'b', 'b1'), 4)
        self.assertEquals(
            self.client.lrange('a', 0, -1),
            [b('a'), b('b'), b('b1'), b('c')])
        self.assertEquals(self.client.linsert('a', 'before', 'b', 'a1'), 5)
        self.assertEquals(
            self.client.lrange('a', 0, -1),
            [b('a'), b('a1'), b('b'), b('b1'), b('c')])

    def test_llen(self):
        # no key
        self.assertEquals(self.client.llen('a'), 0)
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.llen, 'a')
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assertEquals(self.client.llen('a'), 3)

    def test_lpop(self):
        # no key
        self.assertEquals(self.client.lpop('a'), None)
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lpop, 'a')
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assertEquals(self.client.lpop('a'), b('a'))
        self.assertEquals(self.client.lpop('a'), b('b'))
        self.assertEquals(self.client.lpop('a'), b('c'))
        self.assertEquals(self.client.lpop('a'), None)

    def test_lpush(self):
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lpush, 'a', 'a')
        del self.client['a']
        # real logic
        version = self.client.info()['redis_version']
        if StrictVersion(version) >= StrictVersion('2.4.0'):
            self.assertEqual(1, self.client.lpush('a', 'b'))
            self.assertEqual(2, self.client.lpush('a', 'a'))
            self.assertEqual(4, self.client.lpush('a', 'b', 'a'))
        elif StrictVersion(version) >= StrictVersion('1.3.4'):
            self.assertEqual(1, self.client.lpush('a', 'b'))
            self.assertEqual(2, self.client.lpush('a', 'a'))
        else:
            self.assert_(self.client.lpush('a', 'b'))
            self.assert_(self.client.lpush('a', 'a'))
        self.assertEquals(self.client.lindex('a', 0), b('a'))
        self.assertEquals(self.client.lindex('a', 1), b('b'))

    def test_lpushx(self):
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lpushx, 'a', 'a')
        del self.client['a']
        # real logic
        self.assertEquals(self.client.lpushx('a', 'b'), 0)
        self.assertEquals(self.client.lrange('a', 0, -1), [])
        self.make_list('a', 'abc')
        self.assertEquals(self.client.lpushx('a', 'd'), 4)
        self.assertEquals(
            self.client.lrange('a', 0, -1),
            [b('d'), b('a'), b('b'), b('c')])

    def test_lrange(self):
        # no key
        self.assertEquals(self.client.lrange('a', 0, 1), [])
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lrange, 'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_list('a', 'abcde')
        self.assertEquals(
            self.client.lrange('a', 0, 2),
            [b('a'), b('b'), b('c')])
        self.assertEquals(
            self.client.lrange('a', 2, 10),
            [b('c'), b('d'), b('e')])

    def test_lrem(self):
        # no key
        self.assertEquals(self.client.lrem('a', 'foo'), 0)
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lrem, 'a', 'b')
        del self.client['a']
        # real logic
        self.make_list('a', 'aaaa')
        self.assertEquals(self.client.lrem('a', 'a', 1), 1)
        self.assertEquals(
            self.client.lrange('a', 0, 3),
            [b('a'), b('a'), b('a')])
        self.assertEquals(self.client.lrem('a', 'a'), 3)
        # remove all the elements in the list means the key is deleted
        self.assertEquals(self.client.lrange('a', 0, 1), [])

    def test_lset(self):
        # no key
        self.assertRaises(redis.ResponseError, self.client.lset, 'a', 1, 'b')
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.lset, 'a', 1, 'b')
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assertEquals(
            self.client.lrange('a', 0, 2),
            [b('a'), b('b'), b('c')])
        self.assert_(self.client.lset('a', 1, 'd'))
        self.assertEquals(
            self.client.lrange('a', 0, 2),
            [b('a'), b('d'), b('c')])

    def test_ltrim(self):
        # no key -- TODO: Not sure why this is actually true.
        self.assert_(self.client.ltrim('a', 0, 2))
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.ltrim, 'a', 0, 2)
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assert_(self.client.ltrim('a', 0, 1))
        self.assertEquals(self.client.lrange('a', 0, 5), [b('a'), b('b')])

    def test_rpop(self):
        # no key
        self.assertEquals(self.client.rpop('a'), None)
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.rpop, 'a')
        del self.client['a']
        # real logic
        self.make_list('a', 'abc')
        self.assertEquals(self.client.rpop('a'), b('c'))
        self.assertEquals(self.client.rpop('a'), b('b'))
        self.assertEquals(self.client.rpop('a'), b('a'))
        self.assertEquals(self.client.rpop('a'), None)

    def test_rpoplpush(self):
        # no src key
        self.make_list('b', ['b1'])
        self.assertEquals(self.client.rpoplpush('a', 'b'), None)
        # no dest key
        self.assertEquals(self.client.rpoplpush('b', 'a'), b('b1'))
        self.assertEquals(self.client.lindex('a', 0), b('b1'))
        del self.client['a']
        del self.client['b']
        # src key is not a list
        self.client['a'] = 'a1'
        self.assertRaises(redis.ResponseError, self.client.rpoplpush, 'a', 'b')
        del self.client['a']
        # dest key is not a list
        self.make_list('a', ['a1'])
        self.client['b'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.rpoplpush, 'a', 'b')
        del self.client['a']
        del self.client['b']
        # real logic
        self.make_list('a', ['a1', 'a2', 'a3'])
        self.make_list('b', ['b1', 'b2', 'b3'])
        self.assertEquals(self.client.rpoplpush('a', 'b'), b('a3'))
        self.assertEquals(self.client.lrange('a', 0, 2), [b('a1'), b('a2')])
        self.assertEquals(
            self.client.lrange('b', 0, 4),
            [b('a3'), b('b1'), b('b2'), b('b3')])

    def test_rpush(self):
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.rpush, 'a', 'a')
        del self.client['a']
        # real logic
        version = self.client.info()['redis_version']
        if StrictVersion(version) >= StrictVersion('2.4.0'):
            self.assertEqual(1, self.client.rpush('a', 'a'))
            self.assertEqual(2, self.client.rpush('a', 'b'))
            self.assertEqual(4, self.client.rpush('a', 'a', 'b'))
        elif StrictVersion(version) >= StrictVersion('1.3.4'):
            self.assertEqual(1, self.client.rpush('a', 'a'))
            self.assertEqual(2, self.client.rpush('a', 'b'))
        else:
            self.assert_(self.client.rpush('a', 'a'))
            self.assert_(self.client.rpush('a', 'b'))
        self.assertEquals(self.client.lindex('a', 0), b('a'))
        self.assertEquals(self.client.lindex('a', 1), b('b'))

    def test_rpushx(self):
        # key is not a list
        self.client['a'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.rpushx, 'a', 'a')
        del self.client['a']
        # real logic
        self.assertEquals(self.client.rpushx('a', 'b'), 0)
        self.assertEquals(self.client.lrange('a', 0, -1), [])
        self.make_list('a', 'abc')
        self.assertEquals(self.client.rpushx('a', 'd'), 4)
        self.assertEquals(
            self.client.lrange('a', 0, -1),
            [b('a'), b('b'), b('c'), b('d')])

    # Set commands
    def make_set(self, name, l):
        for i in l:
            self.client.sadd(name, i)

    def test_sadd(self):
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.sadd, 'a', 'a1')
        del self.client['a']
        # real logic
        members = set([b('a1'), b('a2'), b('a3')])
        self.make_set('a', members)
        self.assertEquals(self.client.smembers('a'), members)

    def test_scard(self):
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.scard, 'a')
        del self.client['a']
        # real logic
        self.make_set('a', 'abc')
        self.assertEquals(self.client.scard('a'), 3)

    def test_sdiff(self):
        # some key is not a set
        self.make_set('a', ['a1', 'a2', 'a3'])
        self.client['b'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.sdiff, ['a', 'b'])
        del self.client['b']
        # real logic
        self.make_set('b', ['b1', 'a2', 'b3'])
        self.assertEquals(
            self.client.sdiff(['a', 'b']),
            set([b('a1'), b('a3')]))

    def test_sdiffstore(self):
        # some key is not a set
        self.make_set('a', ['a1', 'a2', 'a3'])
        self.client['b'] = 'b'
        self.assertRaises(
            redis.ResponseError, self.client.sdiffstore,
            'c', ['a', 'b'])
        del self.client['b']
        self.make_set('b', ['b1', 'a2', 'b3'])
        # dest key always gets overwritten, even if it's not a set, so don't
        # test for that
        # real logic
        self.assertEquals(self.client.sdiffstore('c', ['a', 'b']), 2)
        self.assertEquals(self.client.smembers('c'), set([b('a1'), b('a3')]))

    def test_sinter(self):
        # some key is not a set
        self.make_set('a', ['a1', 'a2', 'a3'])
        self.client['b'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.sinter, ['a', 'b'])
        del self.client['b']
        # real logic
        self.make_set('b', ['a1', 'b2', 'a3'])
        self.assertEquals(
            self.client.sinter(['a', 'b']),
            set([b('a1'), b('a3')]))

    def test_sinterstore(self):
        # some key is not a set
        self.make_set('a', ['a1', 'a2', 'a3'])
        self.client['b'] = 'b'
        self.assertRaises(
            redis.ResponseError, self.client.sinterstore,
            'c', ['a', 'b'])
        del self.client['b']
        self.make_set('b', ['a1', 'b2', 'a3'])
        # dest key always gets overwritten, even if it's not a set, so don't
        # test for that
        # real logic
        self.assertEquals(self.client.sinterstore('c', ['a', 'b']), 2)
        self.assertEquals(self.client.smembers('c'), set([b('a1'), b('a3')]))

    def test_sismember(self):
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.sismember, 'a', 'a')
        del self.client['a']
        # real logic
        self.make_set('a', 'abc')
        self.assertEquals(self.client.sismember('a', 'a'), True)
        self.assertEquals(self.client.sismember('a', 'b'), True)
        self.assertEquals(self.client.sismember('a', 'c'), True)
        self.assertEquals(self.client.sismember('a', 'd'), False)

    def test_smembers(self):
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.smembers, 'a')
        del self.client['a']
        # set doesn't exist
        self.assertEquals(self.client.smembers('a'), set())
        # real logic
        self.make_set('a', 'abc')
        self.assertEquals(
            self.client.smembers('a'),
            set([b('a'), b('b'), b('c')]))

    def test_smove(self):
        # src key is not set
        self.make_set('b', ['b1', 'b2'])
        self.assertEquals(self.client.smove('a', 'b', 'a1'), 0)
        # src key is not a set
        self.client['a'] = 'a'
        self.assertRaises(
            redis.ResponseError, self.client.smove,
            'a', 'b', 'a1')
        del self.client['a']
        self.make_set('a', ['a1', 'a2'])
        # dest key is not a set
        del self.client['b']
        self.client['b'] = 'b'
        self.assertRaises(
            redis.ResponseError, self.client.smove,
            'a', 'b', 'a1')
        del self.client['b']
        self.make_set('b', ['b1', 'b2'])
        # real logic
        self.assert_(self.client.smove('a', 'b', 'a1'))
        self.assertEquals(self.client.smembers('a'), set([b('a2')]))
        self.assertEquals(
            self.client.smembers('b'),
            set([b('b1'), b('b2'), b('a1')]))

    def test_spop(self):
        # key is not set
        self.assertEquals(self.client.spop('a'), None)
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.spop, 'a')
        del self.client['a']
        # real logic
        s = [b('a'), b('b'), b('c')]
        self.make_set('a', s)
        value = self.client.spop('a')
        self.assert_(value in s)
        self.assertEquals(self.client.smembers('a'), set(s) - set([value]))

    def test_srandmember(self):
        # key is not set
        self.assertEquals(self.client.srandmember('a'), None)
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.srandmember, 'a')
        del self.client['a']
        # real logic
        self.make_set('a', 'abc')
        self.assert_(self.client.srandmember('a') in b('abc'))

        version = self.client.info()['redis_version']
        if StrictVersion(version) >= StrictVersion('2.6.0'):
            randoms = self.client.srandmember('a', number=2)
            self.assertEquals(len(randoms), 2)
            for r in randoms:
                self.assert_(r in b('abc'))

    def test_srem(self):
        # key is not set
        self.assertEquals(self.client.srem('a', 'a'), False)
        # key is not a set
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.srem, 'a', 'a')
        del self.client['a']
        # real logic
        self.make_set('a', 'abc')
        self.assertEquals(self.client.srem('a', 'd'), False)
        self.assertEquals(self.client.srem('a', 'b'), True)
        self.assertEquals(self.client.smembers('a'), set([b('a'), b('c')]))

    def test_sunion(self):
        # some key is not a set
        self.make_set('a', ['a1', 'a2', 'a3'])
        self.client['b'] = 'b'
        self.assertRaises(redis.ResponseError, self.client.sunion, ['a', 'b'])
        del self.client['b']
        # real logic
        self.make_set('b', ['a1', 'b2', 'a3'])
        self.assertEquals(
            self.client.sunion(['a', 'b']),
            set([b('a1'), b('a2'), b('a3'), b('b2')]))

    def test_sunionstore(self):
        # some key is not a set
        self.make_set('a', ['a1', 'a2', 'a3'])
        self.client['b'] = 'b'
        self.assertRaises(
            redis.ResponseError, self.client.sunionstore,
            'c', ['a', 'b'])
        del self.client['b']
        self.make_set('b', ['a1', 'b2', 'a3'])
        # dest key always gets overwritten, even if it's not a set, so don't
        # test for that
        # real logic
        self.assertEquals(self.client.sunionstore('c', ['a', 'b']), 4)
        self.assertEquals(
            self.client.smembers('c'),
            set([b('a1'), b('a2'), b('a3'), b('b2')]))

    # SORTED SETS
    def make_zset(self, name, d):
        for k, v in d.items():
            self.client.zadd(name, **{k: v})

    def test_zadd(self):
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(
            self.client.zrange('a', 0, 3),
            [b('a1'), b('a2'), b('a3')])

    def test_zcard(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zcard, 'a')
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.zcard('a'), 3)

    def test_zcount(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zcount, 'a', 0, 0)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.zcount('a', '-inf', '+inf'), 3)
        self.assertEquals(self.client.zcount('a', 1, 2), 2)
        self.assertEquals(self.client.zcount('a', 10, 20), 0)

    def test_zincrby(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zincrby, 'a', 'a1')
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.zincrby('a', 'a2'), 3.0)
        self.assertEquals(self.client.zincrby('a', 'a3', amount=5), 8.0)
        self.assertEquals(self.client.zscore('a', 'a2'), 3.0)
        self.assertEquals(self.client.zscore('a', 'a3'), 8.0)

    def test_zinterstore(self):
        self.make_zset('a', {'a1': 1, 'a2': 1, 'a3': 1})
        self.make_zset('b', {'a1': 2, 'a3': 2, 'a4': 2})
        self.make_zset('c', {'a1': 6, 'a3': 5, 'a4': 4})

        # sum, no weight
        self.assert_(self.client.zinterstore('z', ['a', 'b', 'c']))
        self.assertEquals(
            self.client.zrange('z', 0, -1, withscores=True),
            [(b('a3'), 8), (b('a1'), 9)]
        )

        # max, no weight
        self.assert_(
            self.client.zinterstore('z', ['a', 'b', 'c'], aggregate='MAX')
        )
        self.assertEquals(
            self.client.zrange('z', 0, -1, withscores=True),
            [(b('a3'), 5), (b('a1'), 6)]
        )

        # with weight
        self.assert_(self.client.zinterstore('z', {'a': 1, 'b': 2, 'c': 3}))
        self.assertEquals(
            self.client.zrange('z', 0, -1, withscores=True),
            [(b('a3'), 20), (b('a1'), 23)]
        )

    def test_zrange(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zrange, 'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.zrange('a', 0, 1), [b('a1'), b('a2')])
        self.assertEquals(self.client.zrange('a', 1, 2), [b('a2'), b('a3')])
        self.assertEquals(
            self.client.zrange('a', 0, 1, withscores=True),
            [(b('a1'), 1.0), (b('a2'), 2.0)])
        self.assertEquals(
            self.client.zrange('a', 1, 2, withscores=True),
            [(b('a2'), 2.0), (b('a3'), 3.0)])
        # test a custom score casting function returns the correct value
        self.assertEquals(
            self.client.zrange('a', 0, 1, withscores=True,
                               score_cast_func=int),
            [(b('a1'), 1), (b('a2'), 2)])
        # a non existant key should return empty list
        self.assertEquals(self.client.zrange('b', 0, 1, withscores=True), [])

    def test_zrangebyscore(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(
            redis.ResponseError, self.client.zrangebyscore,
            'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        self.assertEquals(
            self.client.zrangebyscore('a', 2, 4),
            [b('a2'), b('a3'), b('a4')])
        self.assertEquals(
            self.client.zrangebyscore('a', 2, 4, start=1, num=2),
            [b('a3'), b('a4')])
        self.assertEquals(
            self.client.zrangebyscore('a', 2, 4, withscores=True),
            [(b('a2'), 2.0), (b('a3'), 3.0), (b('a4'), 4.0)])
        # a non existant key should return empty list
        self.assertEquals(
            self.client.zrangebyscore('b', 0, 1, withscores=True), [])

    def test_zrank(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zrank, 'a', 'a4')
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        self.assertEquals(self.client.zrank('a', 'a1'), 0)
        self.assertEquals(self.client.zrank('a', 'a2'), 1)
        self.assertEquals(self.client.zrank('a', 'a3'), 2)
        self.assertEquals(self.client.zrank('a', 'a4'), 3)
        self.assertEquals(self.client.zrank('a', 'a5'), 4)
        # non-existent value in zset
        self.assertEquals(self.client.zrank('a', 'a6'), None)

    def test_zrem(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zrem, 'a', 'a1')
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.zrem('a', 'a2'), True)
        self.assertEquals(self.client.zrange('a', 0, 5), [b('a1'), b('a3')])
        self.assertEquals(self.client.zrem('a', 'b'), False)
        self.assertEquals(self.client.zrange('a', 0, 5), [b('a1'), b('a3')])

    def test_zremrangebyrank(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(
            redis.ResponseError, self.client.zremrangebyscore,
            'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        self.assertEquals(self.client.zremrangebyrank('a', 1, 3), 3)
        self.assertEquals(self.client.zrange('a', 0, 5), [b('a1'), b('a5')])

    def test_zremrangebyscore(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(
            redis.ResponseError, self.client.zremrangebyscore,
            'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        self.assertEquals(self.client.zremrangebyscore('a', 2, 4), 3)
        self.assertEquals(self.client.zrange('a', 0, 5), [b('a1'), b('a5')])
        self.assertEquals(self.client.zremrangebyscore('a', 2, 4), 0)
        self.assertEquals(self.client.zrange('a', 0, 5), [b('a1'), b('a5')])

    def test_zrevrange(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(
            redis.ResponseError, self.client.zrevrange,
            'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.zrevrange('a', 0, 1), [b('a3'), b('a2')])
        self.assertEquals(self.client.zrevrange('a', 1, 2), [b('a2'), b('a1')])
        self.assertEquals(
            self.client.zrevrange('a', 0, 1, withscores=True),
            [(b('a3'), 3.0), (b('a2'), 2.0)])
        self.assertEquals(
            self.client.zrevrange('a', 1, 2, withscores=True),
            [(b('a2'), 2.0), (b('a1'), 1.0)])
        # a non existant key should return empty list
        self.assertEquals(self.client.zrange('b', 0, 1, withscores=True), [])

    def test_zrevrangebyscore(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(
            redis.ResponseError, self.client.zrevrangebyscore,
            'a', 0, 1)
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 1, 'a2': 2, 'a3': 3, 'a4': 4, 'a5': 5})
        self.assertEquals(
            self.client.zrevrangebyscore('a', 4, 2),
            [b('a4'), b('a3'), b('a2')])
        self.assertEquals(
            self.client.zrevrangebyscore('a', 4, 2, start=1, num=2),
            [b('a3'), b('a2')])
        self.assertEquals(
            self.client.zrevrangebyscore('a', 4, 2, withscores=True),
            [(b('a4'), 4.0), (b('a3'), 3.0), (b('a2'), 2.0)])
        # a non existant key should return empty list
        self.assertEquals(
            self.client.zrevrangebyscore('b', 1, 0, withscores=True),
            [])

    def test_zrevrank(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zrevrank, 'a', 'a4')
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 5, 'a2': 4, 'a3': 3, 'a4': 2, 'a5': 1})
        self.assertEquals(self.client.zrevrank('a', 'a1'), 0)
        self.assertEquals(self.client.zrevrank('a', 'a2'), 1)
        self.assertEquals(self.client.zrevrank('a', 'a3'), 2)
        self.assertEquals(self.client.zrevrank('a', 'a4'), 3)
        self.assertEquals(self.client.zrevrank('a', 'a5'), 4)
        self.assertEquals(self.client.zrevrank('a', 'b'), None)

    def test_zscore(self):
        # key is not a zset
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.zscore, 'a', 'a1')
        del self.client['a']
        # real logic
        self.make_zset('a', {'a1': 0, 'a2': 1, 'a3': 2})
        self.assertEquals(self.client.zscore('a', 'a1'), 0.0)
        self.assertEquals(self.client.zscore('a', 'a2'), 1.0)
        # test a non-existant member
        self.assertEquals(self.client.zscore('a', 'a4'), None)

    def test_zunionstore(self):
        self.make_zset('a', {'a1': 1, 'a2': 1, 'a3': 1})
        self.make_zset('b', {'a1': 2, 'a3': 2, 'a4': 2})
        self.make_zset('c', {'a1': 6, 'a4': 5, 'a5': 4})

        # sum, no weight
        self.assert_(self.client.zunionstore('z', ['a', 'b', 'c']))
        self.assertEquals(
            self.client.zrange('z', 0, -1, withscores=True),
            [
                (b('a2'), 1),
                (b('a3'), 3),
                (b('a5'), 4),
                (b('a4'), 7),
                (b('a1'), 9)
            ]
        )

        # max, no weight
        self.assert_(
            self.client.zunionstore('z', ['a', 'b', 'c'], aggregate='MAX')
        )
        self.assertEquals(
            self.client.zrange('z', 0, -1, withscores=True),
            [
                (b('a2'), 1),
                (b('a3'), 2),
                (b('a5'), 4),
                (b('a4'), 5),
                (b('a1'), 6)
            ]
        )

        # with weight
        self.assert_(self.client.zunionstore('z', {'a': 1, 'b': 2, 'c': 3}))
        self.assertEquals(
            self.client.zrange('z', 0, -1, withscores=True),
            [
                (b('a2'), 1),
                (b('a3'), 5),
                (b('a5'), 12),
                (b('a4'), 19),
                (b('a1'), 23)
            ]
        )

    # HASHES
    def make_hash(self, key, d):
        for k, v in iteritems(d):
            self.client.hset(key, k, v)

    def test_hget_and_hset(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hget, 'a', 'a1')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hget('a', 'a1'), None)
        # real logic
        self.make_hash('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.hget('a', 'a1'), b('1'))
        self.assertEquals(self.client.hget('a', 'a2'), b('2'))
        self.assertEquals(self.client.hget('a', 'a3'), b('3'))
        # field was updated, redis returns 0
        self.assertEquals(self.client.hset('a', 'a2', 5), 0)
        self.assertEquals(self.client.hget('a', 'a2'), b('5'))
        # field is new, redis returns 1
        self.assertEquals(self.client.hset('a', 'a4', 4), 1)
        self.assertEquals(self.client.hget('a', 'a4'), b('4'))
        # key inside of hash that doesn't exist returns null value
        self.assertEquals(self.client.hget('a', 'b'), None)

    def test_hsetnx(self):
        # Initially set the hash field
        self.client.hsetnx('a', 'a1', 1)
        self.assertEqual(self.client.hget('a', 'a1'), b('1'))
        # Try and set the existing hash field to a different value
        self.client.hsetnx('a', 'a1', 2)
        self.assertEqual(self.client.hget('a', 'a1'), b('1'))

    def test_hmset(self):
        d = {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
        self.assert_(self.client.hmset('foo', d))
        self.assertEqual(self.client.hgetall('foo'), d)
        self.assertRaises(redis.DataError, self.client.hmset, 'foo', {})

    def test_hmset_empty_value(self):
        d = {b('a'): b('1'), b('b'): b('2'), b('c'): b('')}
        self.assert_(self.client.hmset('foo', d))
        self.assertEqual(self.client.hgetall('foo'), d)

    def test_hmget(self):
        d = {'a': 1, 'b': 2, 'c': 3}
        self.assert_(self.client.hmset('foo', d))
        self.assertEqual(
            self.client.hmget('foo', ['a', 'b', 'c']), [b('1'), b('2'), b('3')])
        self.assertEqual(self.client.hmget('foo', ['a', 'c']), [b('1'), b('3')])
        # using *args type args
        self.assertEquals(self.client.hmget('foo', 'a', 'c'), [b('1'), b('3')])

    def test_hmget_empty(self):
        self.assertEqual(self.client.hmget('foo', ['a', 'b']), [None, None])

    def test_hmget_no_keys(self):
        self.assertRaises(redis.ResponseError, self.client.hmget, 'foo', [])

    def test_hdel(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hdel, 'a', 'a1')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hdel('a', 'a1'), False)
        # real logic
        self.make_hash('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.hget('a', 'a2'), b('2'))
        self.assert_(self.client.hdel('a', 'a2'))
        self.assertEquals(self.client.hget('a', 'a2'), None)

    def test_hexists(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hexists, 'a', 'a1')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hexists('a', 'a1'), False)
        # real logic
        self.make_hash('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.hexists('a', 'a1'), True)
        self.assertEquals(self.client.hexists('a', 'a4'), False)
        self.client.hdel('a', 'a1')
        self.assertEquals(self.client.hexists('a', 'a1'), False)

    def test_hgetall(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hgetall, 'a')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hgetall('a'), {})
        # real logic
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        self.make_hash('a', h)
        remote_hash = self.client.hgetall('a')
        self.assertEquals(h, remote_hash)

    def test_hincrby(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hincrby, 'a', 'a1')
        del self.client['a']
        # no key should create the hash and incr the key's value to 1
        self.assertEquals(self.client.hincrby('a', 'a1'), 1)
        # real logic
        self.assertEquals(self.client.hincrby('a', 'a1'), 2)
        self.assertEquals(self.client.hincrby('a', 'a1', amount=2), 4)
        # negative values decrement
        self.assertEquals(self.client.hincrby('a', 'a1', amount=-3), 1)
        # hash that exists, but key that doesn't
        self.assertEquals(self.client.hincrby('a', 'a2', amount=3), 3)
        # finally a key that's not an int
        self.client.hset('a', 'a3', 'foo')
        self.assertRaises(redis.ResponseError, self.client.hincrby, 'a', 'a3')

    def test_hincrbyfloat(self):
        version = self.client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError,
                          self.client.hincrbyfloat, 'a', 'a1')
        del self.client['a']
        # no key should create the hash and incr the key's value to 1
        self.assertEquals(self.client.hincrbyfloat('a', 'a1'), 1.0)
        self.assertEquals(self.client.hincrbyfloat('a', 'a1'), 2.0)
        self.assertEquals(self.client.hincrbyfloat('a', 'a1', 1.2), 3.2)

    def test_hkeys(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hkeys, 'a')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hkeys('a'), [])
        # real logic
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        self.make_hash('a', h)
        keys = dictkeys(h)
        keys.sort()
        remote_keys = self.client.hkeys('a')
        remote_keys.sort()
        self.assertEquals(keys, remote_keys)

    def test_hlen(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hlen, 'a')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hlen('a'), 0)
        # real logic
        self.make_hash('a', {'a1': 1, 'a2': 2, 'a3': 3})
        self.assertEquals(self.client.hlen('a'), 3)
        self.client.hdel('a', 'a3')
        self.assertEquals(self.client.hlen('a'), 2)

    def test_hvals(self):
        # key is not a hash
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.hvals, 'a')
        del self.client['a']
        # no key
        self.assertEquals(self.client.hvals('a'), [])
        # real logic
        h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
        self.make_hash('a', h)
        vals = dictvalues(h)
        vals.sort()
        remote_vals = self.client.hvals('a')
        remote_vals.sort()
        self.assertEquals(vals, remote_vals)

    # SORT
    def test_sort_bad_key(self):
        # key is not set
        self.assertEquals(self.client.sort('a'), [])
        # key is a string value
        self.client['a'] = 'a'
        self.assertRaises(redis.ResponseError, self.client.sort, 'a')
        del self.client['a']

    def test_sort_basic(self):
        self.make_list('a', '3214')
        self.assertEquals(
            self.client.sort('a'),
            [b('1'), b('2'), b('3'), b('4')])

    def test_sort_limited(self):
        self.make_list('a', '3214')
        self.assertEquals(
            self.client.sort('a', start=1, num=2),
            [b('2'), b('3')])

    def test_sort_by(self):
        self.client['score:1'] = 8
        self.client['score:2'] = 3
        self.client['score:3'] = 5
        self.make_list('a_values', '123')
        self.assertEquals(
            self.client.sort('a_values', by='score:*'),
            [b('2'), b('3'), b('1')])

    def test_sort_get(self):
        self.client['user:1'] = 'u1'
        self.client['user:2'] = 'u2'
        self.client['user:3'] = 'u3'
        self.make_list('a', '231')
        self.assertEquals(
            self.client.sort('a', get='user:*'),
            [b('u1'), b('u2'), b('u3')])

    def test_sort_get_multi(self):
        self.client['user:1'] = 'u1'
        self.client['user:2'] = 'u2'
        self.client['user:3'] = 'u3'
        self.make_list('a', '231')
        self.assertEquals(
            self.client.sort('a', get=('user:*', '#')),
            [b('u1'), b('1'), b('u2'), b('2'), b('u3'), b('3')])

    def test_sort_desc(self):
        self.make_list('a', '231')
        self.assertEquals(
            self.client.sort('a', desc=True),
            [b('3'), b('2'), b('1')])

    def test_sort_alpha(self):
        self.make_list('a', 'ecbda')
        self.assertEquals(
            self.client.sort('a', alpha=True),
            [b('a'), b('b'), b('c'), b('d'), b('e')])

    def test_sort_store(self):
        self.make_list('a', '231')
        self.assertEquals(self.client.sort('a', store='sorted_values'), 3)
        self.assertEquals(
            self.client.lrange('sorted_values', 0, 5),
            [b('1'), b('2'), b('3')])

    def test_sort_all_options(self):
        self.client['user:1:username'] = 'zeus'
        self.client['user:2:username'] = 'titan'
        self.client['user:3:username'] = 'hermes'
        self.client['user:4:username'] = 'hercules'
        self.client['user:5:username'] = 'apollo'
        self.client['user:6:username'] = 'athena'
        self.client['user:7:username'] = 'hades'
        self.client['user:8:username'] = 'dionysus'

        self.client['user:1:favorite_drink'] = 'yuengling'
        self.client['user:2:favorite_drink'] = 'rum'
        self.client['user:3:favorite_drink'] = 'vodka'
        self.client['user:4:favorite_drink'] = 'milk'
        self.client['user:5:favorite_drink'] = 'pinot noir'
        self.client['user:6:favorite_drink'] = 'water'
        self.client['user:7:favorite_drink'] = 'gin'
        self.client['user:8:favorite_drink'] = 'apple juice'

        self.make_list('gods', '12345678')
        num = self.client.sort(
            'gods', start=2, num=4, by='user:*:username',
            get='user:*:favorite_drink', desc=True, alpha=True, store='sorted')
        self.assertEquals(num, 4)
        self.assertEquals(
            self.client.lrange('sorted', 0, 10),
            [b('vodka'), b('milk'), b('gin'), b('apple juice')])

    def test_strict_zadd(self):
        client = self.get_client(redis.StrictRedis)
        client.zadd('a', 1.0, 'a1', 2.0, 'a2', a3=3.0)
        self.assertEquals(client.zrange('a', 0, 3, withscores=True),
                          [(b('a1'), 1.0), (b('a2'), 2.0), (b('a3'), 3.0)])

    def test_strict_lrem(self):
        client = self.get_client(redis.StrictRedis)
        client.rpush('a', 'a1')
        client.rpush('a', 'a2')
        client.rpush('a', 'a3')
        client.rpush('a', 'a1')
        client.lrem('a', 0, 'a1')
        self.assertEquals(client.lrange('a', 0, -1), [b('a2'), b('a3')])

    def test_strict_setex(self):
        "SETEX swaps the order of the value and timeout"
        client = self.get_client(redis.StrictRedis)
        self.assertEquals(client.setex('a', 60, '1'), True)
        self.assertEquals(client['a'], b('1'))
        self.assertEquals(client.ttl('a'), 60)

    def test_strict_expire(self):
        "TTL is -1 by default in StrictRedis"
        client = self.get_client(redis.StrictRedis)
        self.assertEquals(client.expire('a', 10), False)
        self.client['a'] = 'foo'
        self.assertEquals(client.expire('a', 10), True)
        self.assertEquals(client.ttl('a'), 10)
        self.assertEquals(client.persist('a'), True)
        self.assertEquals(client.ttl('a'), -1)

    def test_strict_pexpire(self):
        client = self.get_client(redis.StrictRedis)
        version = client.info()['redis_version']
        if StrictVersion(version) < StrictVersion('2.6.0'):
            try:
                raise unittest.SkipTest()
            except AttributeError:
                return

        self.assertEquals(client.pexpire('a', 10000), False)
        self.client['a'] = 'foo'
        self.assertEquals(client.pexpire('a', 10000), True)
        self.assert_(client.pttl('a') <= 10000)
        self.assertEquals(client.persist('a'), True)
        self.assertEquals(client.pttl('a'), -1)

    ## BINARY SAFE
    # TODO add more tests
    def test_binary_get_set(self):
        self.assertTrue(self.client.set(' foo bar ', '123'))
        self.assertEqual(self.client.get(' foo bar '), b('123'))

        self.assertTrue(self.client.set(' foo\r\nbar\r\n ', '456'))
        self.assertEqual(self.client.get(' foo\r\nbar\r\n '), b('456'))

        self.assertTrue(self.client.set(' \r\n\t\x07\x13 ', '789'))
        self.assertEqual(self.client.get(' \r\n\t\x07\x13 '), b('789'))

        self.assertEqual(
            sorted(self.client.keys('*')),
            [b(' \r\n\t\x07\x13 '), b(' foo\r\nbar\r\n '), b(' foo bar ')])

        self.assertTrue(self.client.delete(' foo bar '))
        self.assertTrue(self.client.delete(' foo\r\nbar\r\n '))
        self.assertTrue(self.client.delete(' \r\n\t\x07\x13 '))

    def test_binary_lists(self):
        mapping = {
            b('foo bar'): [b('1'), b('2'), b('3')],
            b('foo\r\nbar\r\n'): [b('4'), b('5'), b('6')],
            b('foo\tbar\x07'): [b('7'), b('8'), b('9')],
        }
        # fill in lists
        for key, value in iteritems(mapping):
            for c in value:
                self.assertTrue(self.client.rpush(key, c))

        # check that KEYS returns all the keys as they are
        self.assertEqual(sorted(self.client.keys('*')),
                         sorted(dictkeys(mapping)))

        # check that it is possible to get list content by key name
        for key in dictkeys(mapping):
            self.assertEqual(self.client.lrange(key, 0, -1),
                             mapping[key])

    def test_22_info(self):
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
        self.assert_('allocation_stats' in parsed)
        self.assert_('6' in parsed['allocation_stats'])
        self.assert_('>=256' in parsed['allocation_stats'])

    def test_large_responses(self):
        "The PythonParser has some special cases for return values > 1MB"
        # load up 5MB of data into a key
        data = []
        for i in range(5000000 // len(ascii_letters)):
            data.append(ascii_letters)
        data = ''.join(data)
        self.client.set('a', data)
        self.assertEquals(self.client.get('a'), b(data))

    def test_floating_point_encoding(self):
        """
        High precision floating point values sent to the server should keep
        precision.
        """
        timestamp = 1349673917.939762
        self.client.zadd('a', 'aaa', timestamp)
        self.assertEquals(self.client.zscore('a', 'aaa'), timestamp)
