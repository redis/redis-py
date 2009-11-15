#!/usr/bin/env python

""" redis.py - A client for the Redis daemon.

History:
        - 20091115 implemented __getitem__, __setitem__, __delitem__ to make
          gets/sets/deletes easy and pythonic. better py 2.6 decimal support
          (thanks Brent Pedersen)
        - 20091106 added SPOP, SCARD, SRANDMEMBER, SDIFF, SDIFFSTORE
          and all sorted set (Z*) commands (Andy McCurdy)
        - 20091106 added connection retry handling when the client gets disconnected,
          perhaps via the server timing the client out.
        - 20090603 fix missing errno import, add sunion and sunionstore commands,
          generalize shebang (Jochen Kupperschmidt)

"""

__author__ = "Ludovico Magnocavallo <ludo\x40qix\x2eit>"
__copyright__ = "Copyright 2009, Ludovico Magnocavallo"
__license__ = "MIT"
__version__ = "0.5"
__revision__ = "$LastChangedRevision: 175 $"[22:-2]
__date__ = "$LastChangedDate: 2009-03-17 16:15:55 +0100 (Mar, 17 Mar 2009) $"[18:-2]


# TODO: Redis._get_multi_response


import socket
import decimal
import errno


class RedisError(Exception): pass
class ConnectionError(RedisError): pass
class ResponseError(RedisError): pass
class InvalidResponse(RedisError): pass
class InvalidData(RedisError): pass


class Redis(object):
    """The main Redis client.

    >>> r = Redis(db=9)
    >>> r['a'] = 24.0
    >>> r['a']
    Decimal("24.0")

    >>> r = Redis(db=9, float_fn=float)
    >>> r['a']
    24.0

    >>> del r['a']
    >>> print r.get('a') # r['a'] will raise KeyError
    None

    """
    
    def __init__(self, host=None, port=None, timeout=None,
            db=None, nodelay=None, charset='utf8', errors='strict',
            retry_connection=True, float_fn=decimal.Decimal):
        self.host = host or 'localhost'
        self.port = port or 6379
        if timeout:
            socket.setdefaulttimeout(timeout)
        self.nodelay = nodelay
        self.charset = charset
        self.errors = errors
        self.float_fn = float_fn
        if retry_connection:
            self.send_command = self._send_command_retry
        else:
            self.send_command = self._send_command
        self.retry_connection = retry_connection
        self._sock = None
        self._fp = None
        self.db = db
        
    def _encode(self, s):
        if isinstance(s, str):
            return s
        if isinstance(s, unicode):
            try:
                return s.encode(self.charset, self.errors)
            except UnicodeEncodeError, e:
                raise InvalidData("Error encoding unicode value '%s': %s" % (value.encode(self.charset, 'replace'), e))
        return str(s)
    
    def _send_command(self, s):
        """
        >>> r = Redis(db=9)
        >>> r.connect()
        >>> r._sock.close()
        >>> try:
        ...     r._send_command('pippo')
        ... except ConnectionError, e:
        ...     print e
        Error 9 while writing to socket. Bad file descriptor.
        >>>
        >>> 
        """
        self.connect()
        try:
            self._sock.sendall(s)
        except socket.error, e:
            if e.args[0] == 32:
                # broken pipe
                self.disconnect()
            raise ConnectionError("Error %s while writing to socket. %s." % tuple(e.args))
        return self._get_response()
            
    def _send_command_retry(self, s):
        try:
            return self._send_command(s)
        except ConnectionError:
            self.disconnect()
            return self._send_command(s)
            
            
    def _read(self):
        try:
            return self._fp.readline()
        except socket.error, e:
            if e.args and e.args[0] == errno.EAGAIN:
                return
            self.disconnect()
            raise ConnectionError("Error %s while reading from socket. %s." % tuple(e.args))
        if not data:
            self.disconnect()
            raise ConnectionError("Socket connection closed when reading.")
        return data
    
    def ping(self):
        """
        >>> r = Redis(db=9)
        >>> r.ping()
        'PONG'
        >>> 
        """
        return self.send_command('PING\r\n')
    
    def set(self, name, value, preserve=False, getset=False):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo')
        'OK'
        >>> r.set('a', u'pippo \u3235')
        'OK'
        >>> r.get('a')
        u'pippo \u3235'
        >>> r.set('b', 105.2)
        'OK'
        >>> r.set('b', 'xxx', preserve=True)
        0
        >>> r.get('b')
        Decimal("105.2")
        >>> 
        """
        # the following will raise an error for unicode values that can't be encoded to ascii
        # we could probably add an 'encoding' arg to init, but then what do we do with get()?
        # convert back to unicode? and what about ints, or pickled values?
        if getset: command = 'GETSET'
        elif preserve: command = 'SETNX'
        else: command = 'SET'
        value = self._encode(value)
        return self.send_command('%s %s %s\r\n%s\r\n' % (
                command, name, len(value), value
            ))

    __setitem__ = set
    
    def get(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo'), r.set('b', 15), r.set('c', ' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '), r.set('d', '\\r\\n')
        ('OK', 'OK', 'OK', 'OK')
        >>> r.get('a')
        u'pippo'
        >>> r.get('b')
        15
        >>> r.get('d')
        u'\\r\\n'
        >>> r.get('b')
        15
        >>> r.get('c')
        u' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '
        >>> r.get('c')
        u' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '
        >>> r.get('ajhsd')
        """
        return self.send_command('GET %s\r\n' % name)

    def __getitem__(self, name):
        val = self.get(name)
        if val is None:
            raise KeyError
        return val

    
    def getset(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo')
        'OK'
        >>> r.getset('a', 2)
        u'pippo'
        >>> 
        """
        return self.set(name, value, getset=True)
        
    def mget(self, *args):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'pippo'), r.set('b', 15), r.set('c', '\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n'), r.set('d', '\\r\\n')
        ('OK', 'OK', 'OK', 'OK')
        >>> r.mget('a', 'b', 'c', 'd')
        [u'pippo', 15, u'\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n', u'\\r\\n']
        >>> 
        """
        return self.send_command('MGET %s\r\n' % ' '.join(args))
    
    def incr(self, name, amount=1):
        """
        >>> r = Redis(db=9)
        >>> r.delete('a')
        1
        >>> r.incr('a')
        1
        >>> r.incr('a')
        2
        >>> r.incr('a', 2)
        4
        >>>
        """
        if amount == 1:
            return self.send_command('INCR %s\r\n' % name)
        else:
            return self.send_command('INCRBY %s %s\r\n' % (name, amount))

    def decr(self, name, amount=1):
        """
        >>> r = Redis(db=9)
        >>> if r.get('a'):
        ...     r.delete('a')
        ... else:
        ...     print 1
        1
        >>> r.decr('a')
        -1
        >>> r.decr('a')
        -2
        >>> r.decr('a', 5)
        -7
        >>> 
        """
        if amount == 1:
            return self.send_command('DECR %s\r\n' % name)
        else:
            return self.send_command('DECRBY %s %s\r\n' % (name, amount))
    
    def exists(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.exists('dsjhfksjdhfkdsjfh')
        0
        >>> r.set('a', 'a')
        'OK'
        >>> r.exists('a')
        1
        >>>
        """
        return self.send_command('EXISTS %s\r\n' % name)

    def delete(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.delete('dsjhfksjdhfkdsjfh')
        0
        >>> r.set('a', 'a')
        'OK'
        >>> r.delete('a')
        1
        >>> r.exists('a')
        0
        >>> r.delete('a')
        0
        >>> 
        """
        return self.send_command('DEL %s\r\n' % name)

    __delitem__ = delete

    def get_type(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 3)
        'OK'
        >>> r.get_type('a')
        'string'
        >>> r.get_type('zzz')
        >>> 
        """
        res = self.send_command('TYPE %s\r\n' % name)
        return None if res == 'none' else res
    
    def keys(self, pattern):
        """
        >>> r = Redis(db=9)
        >>> r.flush()
        'OK'
        >>> r.set('a', 'a')
        'OK'
        >>> r.keys('a*')
        [u'a']
        >>> r.set('a2', 'a')
        'OK'
        >>> keys = r.keys('a*')
        >>> u'a' in keys
        True
        >>> u'a2' in keys
        True
        >>> r.delete('a2')
        1
        >>> r.keys('sjdfhskjh*')
        []
        >>>
        """
        return self.send_command('KEYS %s\r\n' % pattern).split()
    
    def randomkey(self):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'a')
        'OK'
        >>> isinstance(r.randomkey(), str)
        True
        >>> 
        """
        #raise NotImplementedError("Implemented but buggy, do not use.")
        return self.send_command('RANDOMKEY\r\n')
    
    def rename(self, src, dst, preserve=False):
        """
        >>> r = Redis(db=9)
        >>> try:
        ...     r.rename('a', 'a')
        ... except ResponseError, e:
        ...     print e
        source and destination objects are the same
        >>> r.rename('a', 'b')
        'OK'
        >>> try:
        ...     r.rename('a', 'b')
        ... except ResponseError, e:
        ...     print e
        no such key
        >>> r.set('a', 1)
        'OK'
        >>> r.rename('b', 'a', preserve=True)
        0
        >>> 
        """
        if preserve:
            return self.send_command('RENAMENX %s %s\r\n' % (src, dst))
        else:
            return self.send_command('RENAME %s %s\r\n' % (src, dst))
        
    def dbsize(self):
        """
        >>> r = Redis(db=9)
        >>> type(r.dbsize())
        <type 'int'>
        >>> 
        """
        return self.send_command('DBSIZE\r\n')
    
    def ttl(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.ttl('a')
        -1
        >>> r.expire('a', 10)
        1
        >>> r.ttl('a')
        10
        >>> r.expire('a', 0)
        0
        >>> 
        """
        return self.send_command('TTL %s\r\n' % name)
    
    def expire(self, name, time):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 1)
        'OK'
        >>> r.expire('a', 1)
        1
        >>> r.expire('zzzzz', 1)
        0
        >>> 
        """
        return self.send_command('EXPIRE %s %s\r\n' % (name, time))
    
    def push(self, name, value, head=False):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'a')
        'OK'
        >>> r.set('a', 'a')
        'OK'
        >>> try:
        ...     r.push('a', 'a')
        ... except ResponseError, e:
        ...     print e
        Operation against a key holding the wrong kind of value
        >>> 
        """
        value = self._encode(value)
        return self.send_command('%s %s %s\r\n%s\r\n' % (
            'LPUSH' if head else 'RPUSH', name, len(value), value
        ))
    
    def llen(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'a')
        'OK'
        >>> r.llen('l')
        1
        >>> r.push('l', 'a')
        'OK'
        >>> r.llen('l')
        2
        >>> 
        """
        return self.send_command('LLEN %s\r\n' % name)

    def lrange(self, name, start, end):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.lrange('l', 0, 1)
        []
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lrange('l', 0, 1)
        [u'aaa']
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.lrange('l', 0, 0)
        [u'aaa']
        >>> r.lrange('l', 0, 1)
        [u'aaa', u'bbb']
        >>> r.lrange('l', -1, 0)
        []
        >>> r.lrange('l', -1, -1)
        [u'bbb']
        >>> 
        """
        return self.send_command('LRANGE %s %s %s\r\n' % (name, start, end))
        
    def ltrim(self, name, start, end):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> try:
        ...     r.ltrim('l', 0, 1)
        ... except ResponseError, e:
        ...     print e
        no such key
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.push('l', 'ccc')
        'OK'
        >>> r.ltrim('l', 0, 1)
        'OK'
        >>> r.llen('l')
        2
        >>> r.ltrim('l', 99, 95)
        'OK'
        >>> r.llen('l')
        0
        >>> 
        """
        return self.send_command('LTRIM %s %s %s\r\n' % (name, start, end))
    
    def lindex(self, name, index):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('l')
        >>> r.lindex('l', 0)
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lindex('l', 0)
        u'aaa'
        >>> r.lindex('l', 2)
        >>> r.push('l', 'ccc')
        'OK'
        >>> r.lindex('l', 1)
        u'ccc'
        >>> r.lindex('l', -1)
        u'ccc'
        >>> 
        """
        return self.send_command('LINDEX %s %s\r\n' % (name, index))
        
    def pop(self, name, tail=False):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.pop('l')
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.pop('l')
        u'aaa'
        >>> r.pop('l')
        u'bbb'
        >>> r.pop('l')
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.pop('l', tail=True)
        u'bbb'
        >>> r.pop('l')
        u'aaa'
        >>> r.pop('l')
        >>> 
        """
        return self.send_command('%s %s\r\n' % ('RPOP' if tail else 'LPOP', name))
        
    def poppush(self, src, dst):
        """
        >>> r = Redis(db=9)
        >>> r.delete('lsource')
        0
        >>> r.delete('ldestination')
        0
        >>> r.push('lsource', 'one', head=True)
        'OK'
        >>> r.push('lsource', 'two', head=True)
        'OK'
        >>> r.push('lsource', 'three', head=True)
        'OK'
        >>> r.poppush('lsource', 'ldestination')
        u'one'
        >>> r.lrange('lsource', 0, -1)
        [u'three', u'two']
        >>> r.lrange('ldestination', 0, -1)
        [u'one']
        >>> r.poppush('lsource', 'ldestination')
        u'two'
        >>> r.lrange('lsource', 0, -1)
        [u'three']
        >>> r.lrange('ldestination', 0, -1)
        [u'two', u'one']
        """
        return self.send_command('RPOPLPUSH %s %s\r\n%s\r\n' % (
            src, len(dst), dst
        ))
    
    def lset(self, name, index, value):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> try:
        ...     r.lset('l', 0, 'a')
        ... except ResponseError, e:
        ...     print e
        no such key
        >>> r.push('l', 'aaa')
        'OK'
        >>> try:
        ...     r.lset('l', 1, 'a')
        ... except ResponseError, e:
        ...     print e
        index out of range
        >>> r.lset('l', 0, 'bbb')
        'OK'
        >>> r.lrange('l', 0, 1)
        [u'bbb']
        >>> 
        """
        value = self._encode(value)
        return self.send_command('LSET %s %s %s\r\n%s\r\n' % (
            name, index, len(value), value
        ))
    
    def lrem(self, name, value, num=0):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lrem('l', 'aaa')
        2
        >>> r.lrange('l', 0, 10)
        [u'bbb']
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.lrem('l', 'aaa', 1)
        1
        >>> r.lrem('l', 'aaa', 1)
        1
        >>> r.lrem('l', 'aaa', 1)
        0
        >>> 
        """
        value = self._encode(value)
        return self.send_command('LREM %s %s %s\r\n%s\r\n' % (
            name, num, len(value), value
        ))
    
    def sort(self, name, by=None, get=None, start=None, num=None, desc=False, alpha=False):
        """
        >>> r = Redis(db=9)
        >>> r.delete('l')
        1
        >>> r.push('l', 'ccc')
        'OK'
        >>> r.push('l', 'aaa')
        'OK'
        >>> r.push('l', 'ddd')
        'OK'
        >>> r.push('l', 'bbb')
        'OK'
        >>> r.sort('l', alpha=True)
        [u'aaa', u'bbb', u'ccc', u'ddd']
        >>> r.delete('l')
        1
        >>> for i in range(1, 5):
        ...     res = r.push('l', 1.0 / i)
        >>> r.sort('l')
        [Decimal("0.25"), Decimal("0.333333333333"), Decimal("0.5"), Decimal("1.0")]
        >>> r.sort('l', desc=True)
        [Decimal("1.0"), Decimal("0.5"), Decimal("0.333333333333"), Decimal("0.25")]
        >>> r.sort('l', desc=True, start=2, num=1)
        [Decimal("0.333333333333")]
        >>> r.set('weight_0.5', 10)
        'OK'
        >>> r.sort('l', desc=True, by='weight_*')
        [Decimal("0.5"), Decimal("1.0"), Decimal("0.333333333333"), Decimal("0.25")]
        >>> for i in r.sort('l', desc=True):
        ...     res = r.set('test_%s' % i, 100 - float(i))
        >>> r.sort('l', desc=True, get='test_*')
        [Decimal("99.0"), Decimal("99.5"), Decimal("99.6666666667"), Decimal("99.75")]
        >>> r.sort('l', desc=True, by='weight_*', get='test_*')
        [Decimal("99.5"), Decimal("99.0"), Decimal("99.6666666667"), Decimal("99.75")]
        >>> r.sort('l', desc=True, by='weight_*', get='missing_*')
        [None, None, None, None]
        >>> 
        """
        stmt = ['SORT', name]
        if by:
            stmt.append("BY %s" % by)
        if start and num:
            stmt.append("LIMIT %s %s" % (start, num))
        if get is None:
            pass
        elif isinstance(get, basestring):
            stmt.append("GET %s" % get)
        elif isinstance(get, list) or isinstance(get, tuple):
            for g in get:
                stmt.append("GET %s" % g)
        else:
            raise RedisError("Invalid parameter 'get' for Redis sort")
        if desc:
            stmt.append("DESC")
        if alpha:
            stmt.append("ALPHA")
        return self.send_command(' '.join(stmt + ["\r\n"]))
    
    def sadd(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s')
        >>> r.sadd('s', 'a')
        1
        >>> r.sadd('s', 'b')
        1
        >>> 
        """
        value = self._encode(value)
        return self.send_command('SADD %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        
    def srem(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> r.delete('s')
        1
        >>> r.srem('s', 'aaa')
        0
        >>> r.sadd('s', 'b')
        1
        >>> r.srem('s', 'b')
        1
        >>> r.sismember('s', 'b')
        0
        >>> 
        """
        value = self._encode(value)
        return self.send_command('SREM %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
        
    def spop(self, name):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s')
        >>> r.sadd('s', 'a')
        1
        >>> r.spop('s')
        u'a'
        """
        return self.send_command('SPOP %s\r\n' % name)
        
    def smove(self, src, dst, member):
        """
        >>> r = Redis(db=9)
        >>> _ = r.delete('s1')
        >>> _ = r.delete('s2')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'b')
        1
        >>> r.smove('s1', 's2', 'a')
        1
        >>> src_set = r.smembers('s1')
        >>> 'a' in src_set
        False
        >>> dst_set = r.smembers('s2')
        >>> 'a' in dst_set
        True
        >>> 'b' in dst_set
        True
        """
        return self.send_command('SMOVE %s %s %s\r\n%s\r\n' % (
            src, dst, len(member), member
        ))
    
    def scard(self, name):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s')
        >>> r.sadd('s', 'a')
        1
        >>> r.scard('s')
        1
        """
        return self.send_command('SCARD %s\r\n' % name)
    
    def sismember(self, name, value):
        """
        >>> r = Redis(db=9)
        >>> r.delete('s')
        1
        >>> r.sismember('s', 'b')
        0
        >>> r.sadd('s', 'a')
        1
        >>> r.sismember('s', 'b')
        0
        >>> r.sismember('s', 'a')
        1
        >>>
        """
        value = self._encode(value)
        return self.send_command('SISMEMBER %s %s\r\n%s\r\n' % (
            name, len(value), value
        ))
    
    def sinter(self, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> try:
        ...     r.sinter()
        ... except ResponseError, e:
        ...     print e
        wrong number of arguments
        >>> try:
        ...     r.sinter('l')
        ... except ResponseError, e:
        ...     print e
        Operation against a key holding the wrong kind of value
        >>> r.sinter('s1', 's2', 's3')
        set([])
        >>> r.sinter('s1', 's2')
        set([u'a'])
        >>> 
        """
        return set(self.send_command('SINTER %s\r\n' % ' '.join(args)))
    
    def sinterstore(self, dest, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> r.sinterstore('s_s', 's1', 's2', 's3')
        0
        >>> r.sinterstore('s_s', 's1', 's2')
        1
        >>> r.smembers('s_s')
        set([u'a'])
        >>> 
        """
        return self.send_command('SINTERSTORE %s %s\r\n' % (dest, ' '.join(args)))

    def smembers(self, name):
        """
        >>> r = Redis(db=9)
        >>> r.delete('s')
        1
        >>> r.sadd('s', 'a')
        1
        >>> r.sadd('s', 'b')
        1
        >>> try:
        ...     r.smembers('l')
        ... except ResponseError, e:
        ...     print e
        Operation against a key holding the wrong kind of value
        >>> r.smembers('s')
        set([u'a', u'b'])
        >>> 
        """
        return set(self.send_command('SMEMBERS %s\r\n' % name))

    def sunion(self, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> r.sunion('s1', 's2', 's3')
        set([u'a', u'b'])
        >>> r.sadd('s2', 'c')
        1
        >>> r.sunion('s1', 's2', 's3')
        set([u'a', u'c', u'b'])
        >>> 
        """
        return set(self.send_command('SUNION %s\r\n' % ' '.join(args)))

    def sunionstore(self, dest, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s2', 'a')
        1
        >>> r.sadd('s3', 'b')
        1
        >>> r.sunionstore('s4', 's1', 's2', 's3')
        2
        >>> r.smembers('s4')
        set([u'a', u'b'])
        >>> 
        """
        return self.send_command('SUNIONSTORE %s %s\r\n' % (dest, ' '.join(args)))
        
    def sdiff(self, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> r.sadd('s1', 'x')
        1
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s1', 'b')
        1
        >>> r.sadd('s1', 'c')
        1
        >>> r.sadd('s2', 'c')
        1
        >>> r.sadd('s3', 'a')
        1
        >>> r.sadd('s3', 'd')
        1
        >>> diff = r.sdiff('s1', 's2', 's3')
        >>> 'x' in diff
        True
        >>> 'b' in diff
        True
        """
        return set(self.send_command('SDIFF %s\r\n' % ' '.join(args)))
        
    def sdiffstore(self, dest, *args):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> res = r.delete('s2')
        >>> res = r.delete('s3')
        >>> res = r.delete('s4')
        >>> r.sadd('s1', 'x')
        1
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s1', 'b')
        1
        >>> r.sadd('s1', 'c')
        1
        >>> r.sadd('s2', 'c')
        1
        >>> r.sadd('s3', 'a')
        1
        >>> r.sadd('s3', 'd')
        1
        >>> r.sdiffstore('s4', 's1', 's2', 's3')
        2
        >>> diff = r.smembers('s4')
        >>> 'x' in diff
        True
        >>> 'b' in diff
        True
        """
        return self.send_command('SDIFFSTORE %s %s\r\n' % (dest, ' '.join(args)))
        
    def srandmember(self, key):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('s1')
        >>> r.sadd('s1', 'a')
        1
        >>> r.sadd('s1', 'b')
        1
        >>> rand = r.srandmember('s1')
        >>> rand in ['a', 'b']
        True
        """
        return self.send_command('SRANDMEMBER %s\r\n' % key)
        
    def zadd(self, key, score, member):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('z1')
        >>> r.zadd('z1', 5, 'abc')
        1
        >>> r.zadd('z1', 3, 'def')
        1
        >>> r.zadd('z1', 2, 'abc')
        0
        """
        member = self._encode(member)
        return self.send_command('ZADD %s %s %s\r\n%s\r\n' % (
            key, score, len(member), member
        ))
        
    def zrem(self, key, member):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('z1')
        >>> r.zadd('z1', 5, 'abc')
        1
        >>> r.zrem('z1', 'abc')
        1
        >>> r.zrem('z1', 'def')
        0
        """
        member = self._encode(member)
        return self.send_command('ZREM %s %s\r\n%s\r\n' % (
            key, len(member), member
        ))
        
    def zrange(self, key, start, end, desc=False):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('z1')
        >>> r.zadd('z1', 5, 'a')
        1
        >>> r.zadd('z1', 2, 'b')
        1
        >>> r.zadd('z1', 7, 'c')
        1
        >>> r.zrange('z1', 0, 2)
        [u'b', u'a', u'c']
        >>> r.zrange('z1', 0, 1)
        [u'b', u'a']
        >>> r.zrange('z1', 0, 2, desc=True)
        [u'c', u'a', u'b']
        >>> r.zrange('z1', 0, 1, desc=True)
        [u'c', u'a']
        """
        command = 'ZREVRANGE' if desc else 'ZRANGE'
        return self.send_command('%s %s %s %s\r\n' % (
            command, key, start, end
        ))
        
    def zrangebyscore(self, key, min, max):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('z1')
        >>> r.zadd('z1', 5, 'a')
        1
        >>> r.zadd('z1', 2, 'b')
        1
        >>> r.zadd('z1', 7, 'c')
        1
        >>> r.zadd('z1', 10, 'd')
        1
        >>> r.zrangebyscore('z1', 5, 7)
        [u'a', u'c']
        """
        return self.send_command('ZRANGEBYSCORE %s %s %s\r\n' % (
            key, min, max
        ))
        
    def zcard(self, key):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('z1')
        >>> res = r.delete('z2')
        >>> r.zadd('z1', 5, 'a')
        1
        >>> r.zadd('z1', 2, 'b')
        1
        >>> r.zadd('z1', 7, 'c')
        1
        >>> r.zcard('z1')
        3
        >>> r.zcard('z2')
        0
        """
        return self.send_command('ZCARD %s\r\n' % key)
        
    def zscore(self, key, member):
        """
        >>> r = Redis(db=9)
        >>> res = r.delete('z1')
        >>> res = r.delete('z2')
        >>> r.zadd('z1', 5, 'a')
        1
        >>> r.zscore('z1', 'a')
        5
        >>> r.zscore('z1', 'b')
        >>> r.zscore('z2', 'a')
        """
        member = self._encode(member)
        return self.send_command('ZSCORE %s %s\r\n%s\r\n' % (
            key, len(member), member
        ))
        
    def select(self, db):
        """
        >>> r = Redis(db=9)
        >>> r.delete('a')
        1
        >>> r.select(10)
        'OK'
        >>> r.set('a', 1)
        'OK'
        >>> r.select(9)
        'OK'
        >>> r.get('a')
        >>> 
        """
        return self.send_command('SELECT %s\r\n' % db)
    
    def move(self, name, db):
        """
        >>> r = Redis(db=9)
        >>> r.set('a', 'a')
        'OK'
        >>> r.select(10)
        'OK'
        >>> if r.get('a'):
        ...     r.delete('a')
        ... else:
        ...     print 1
        1
        >>> r.select(9)
        'OK'
        >>> r.move('a', 10)
        1
        >>> r.get('a')
        >>> r.select(10)
        'OK'
        >>> r.get('a')
        u'a'
        >>> r.select(9)
        'OK'
        >>> 
        """
        return self.send_command('MOVE %s %s\r\n' % (name, db))
    
    def save(self, background=False):
        """
        >>> r = Redis(db=9)
        >>> r.save()
        'OK'
        >>> try:
        ...     resp = r.save(background=True)
        ... except ResponseError, e:
        ...     assert str(e) == 'background save already in progress', str(e)
        ... else:
        ...     assert resp == 'OK'
        >>> 
        """
        if background:
            return self.send_command('BGSAVE\r\n')
        else:
            return self.send_command('SAVE\r\n')
        
    def lastsave(self):
        """
        >>> import time
        >>> r = Redis(db=9)
        >>> t = int(time.time())
        >>> r.save()
        'OK'
        >>> r.lastsave() >= t
        True
        >>> 
        """
        return self.send_command('LASTSAVE\r\n')
    
    def flush(self, all_dbs=False):
        """
        >>> r = Redis(db=9)
        >>> r.flush()
        'OK'
        >>> # r.flush(all_dbs=True)
        >>> 
        """
        return self.send_command('%s\r\n' % ('FLUSHALL' if all_dbs else 'FLUSHDB'))
    
    def info(self):
        """
        >>> r = Redis(db=9)
        >>> info = r.info()
        >>> info and isinstance(info, dict)
        True
        >>> isinstance(info.get('connected_clients'), int)
        True
        >>> 
        """
        info = dict()
        for l in self.send_command('INFO\r\n').split('\r\n'):
            if not l:
                continue
            k, v = l.split(':', 1)
            info[k] = int(v) if v.isdigit() else v
        return info
    
    def auth(self, passwd):
        return self.send_command('AUTH %s\r\n' % passwd)
    
    def _get_response(self):
        data = self._read().strip()
        if not data:
            self.disconnect()
            raise ConnectionError("Socket closed on remote end")
        c = data[0]
        if c == '-':
            raise ResponseError(data[5:] if data[:5] == '-ERR ' else data[1:])
        if c == '+':
            return data[1:]
        if c == '*':
            try:
                num = int(data[1:])
            except (TypeError, ValueError):
                raise InvalidResponse("Cannot convert multi-response header '%s' to integer" % data)
            return [self._get_value() for i in range(num)]
        return self._get_value(data)
    
    def _get_value(self, data=None):
        data = data or self._read().strip()
        if data == '$-1':
            return None
        try:
            c, i = data[0], (int(data[1:]) if not "." in data else float(data[1:]))
        except ValueError:
            raise InvalidResponse("Cannot convert data '%s' to integer" % data)
        if c == ':':
            return i
        if c != '$':
            raise InvalidResponse("Unkown response prefix for '%s'" % data)
        buf = []
        while i >= 0:
            data = self._read()
            i -= len(data)
            buf.append(data)

        data = ''.join(buf)[:-2]
        try:
            return int(data) if not '.' in data else self.float_fn(data)
        except (ValueError, decimal.InvalidOperation):
            return data.decode(self.charset)
    
    def disconnect(self):
        if isinstance(self._sock, socket.socket):
            try:
                self._sock.close()
            except socket.error:
                pass
        self._sock = None
        self._fp = None
            
    def connect(self):
        """
        >>> r = Redis(db=9)
        >>> r.connect()
        >>> isinstance(r._sock, socket.socket)
        True
        >>> r.disconnect()
        >>> 
        """
        if isinstance(self._sock, socket.socket):
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, e:
            raise ConnectionError("Error %s connecting to %s:%s. %s." % (e.args[0], self.host, self.port, e.args[1]))
        else:
            self._sock = sock
            self._fp = self._sock.makefile('r')
            if self.db:
                self.select(self.db)
            if self.nodelay is not None:
                self._sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, self.nodelay)
                
            
if __name__ == '__main__':

    # hack to make doctests pass in 2.6
    decimal.Decimal.__repr__ = lambda self: 'Decimal("%s")' % str(self)
    import doctest
    doctest.testmod()
    
