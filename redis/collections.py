# -*- coding: utf-8 -*-
from redis import StrictRedis
import pickle
import collections
from collections import OrderedDict
from ._compat import iteritems

__author__ = 'jscarbor'


class ObjectRedis(collections.MutableMapping):
    """
    View on a Redis database, supporting object keys and arbitrary object values.  This implementation
    uses the Redis key namespace as the namespace for its keys.

    If collections are stored in Redis with the supported collection types, the basic
    python-wrapped collections will be returned, using the same serlalizers provided (default is pickle).
    """

    def __init__(self, redis=StrictRedis(), namespace=None, serializer=pickle, key_serializer=pickle):
        """
        :param redis: The StrictRedis connection to use
        :param namespace: Prepended to keys, None to prepend nothing.  If namespace is none, then all contents of the
              database are considered members of the collection and processed through the serializers.  The namespace
              will be encoded as bytes (str(ns).encode('utf-8')) if it's not already a byte array
        :param serializer: An object containing functions "dumps" to turn an object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        :param key_serializer: Like serializer, but applied to keys
        """
        self.redis = redis
        self.namespace = None
        if namespace is not None:
            self.namespace = ((type(namespace) is bytes and namespace) or str(namespace).encode('utf-8')) + b":::"
        self.serializer = serializer
        self.key_serializer = key_serializer

    def __getitem__(self, key):
        rkey = self._ns(key)
        rtype = self.redis.type(rkey)
        if rtype == b'none':
            raise KeyError(str(key))
        elif rtype == b'string':
            bval = self.redis.get(rkey)
            if bval is not None:
                return self.serializer.loads(bval)
            raise KeyError(str(key))  # It went away between typing and getting
        elif rtype == b'list':
            return RedisList(rkey, self.redis, self.serializer)
        elif rtype == b'set':
            return RedisSet(rkey, self.redis, self.serializer)
        elif rtype == b'hash':
            return RedisDict(rkey, self.redis, self.serializer, self.key_serializer)
        elif rtype == b'zset':
            return RedisSortedSet(rkey, self.redis, self.serializer)
        else:
            raise NotImplementedError(str(rtype))

    def get(self, key, default=None):
        try:
            self.__getitem__(key)
        except KeyError:
            return default

    def __setitem__(self, key, value):
        key.__hash__()
        bkey = self._ns(key)
        d = dir(value)
        if "__imul__" in d and "__iter__" in d:  # list
            def new_list(pipe):
                rl = RedisList(bkey, pipe, self.serializer)
                rl.clear()
                rl.extend(value)

            self.redis.transaction(new_list, key)
        elif "__xor__" in d and "__iter__" in d:  # set
            def new_set(pipe):
                rs = RedisSet(bkey, pipe, self.serializer)
                rs.clear()
                rs.update(value)

            self.redis.transaction(new_set, key)
        elif "__getitem__" in d and 'index' not in d:  # hash
            def new_hash(pipe):
                rd = RedisDict(bkey, pipe, self.serializer, self.serializer)
                rd.clear()
                rd.update(value)

            self.redis.transaction(new_hash, key)
        elif "__getitem__" in d and 'values' in d:  # zset
            def new_zset(pipe):
                rz = RedisSortedSet(bkey, pipe, self.serializer)
                rz.clear()
                rz.update(value)

            self.redis.transaction(new_zset, key)
        else:  # string (other)
            self.redis.set(name=bkey, value=self.serializer.dumps(value))

    def __contains__(self, key):
        return self.redis.exists(self._ns(key))

    def __delitem__(self, key):
        if self.redis.delete(self._ns(key)) == 0:
            raise KeyError(str(key))

    def __iter__(self):
        for k in self.redis.scan_iter(match=(self.namespace is not None and self.namespace + b'*') or None):
            try:
                # __dns can't be done in a list comprehension because the exceptions need to be handled in
                # the case of a null namespace and traversing other items, or in case of different
                # pickling schemes, different namespace termination levels ("foo:" and "foo:bar:", etc.).
                yield self._dns(k)
            except ValueError:  # Other namespaces won't match
                pass
            except pickle.UnpicklingError:  # Other namespaces and schemes won't work
                pass

    def __len__(self):
        return sum(1 for _ in self.__iter__())

    def _dns(self, key):
        """
        decode a stored key by removing the namespace and deserializing it
        :param key: the key as stored in redis with the namespace
        :return: the object key,
        :raises ValueError if the namespace doesn't match
        """
        if self.namespace is not None:
            if key.startswith(self.namespace):
                bkey = key[len(self.namespace):]
            else:
                raise ValueError("mismatched namespace: " + str(key))
        else:
            bkey = key
        return self.key_serializer.loads(bkey)

    def _ns(self, key):
        """
        Convert an object key into one with the redis namespace and a serialized version of the suffix
        :param key: any object
        :return: a redis key beginning with the namepsace for this object, followed by the serialized object
        """
        if self.namespace is not None:
            return self.namespace + self.key_serializer.dumps(key)
        else:
            return self.key_serializer.dumps(key)

    def copy(self):
        d = {}
        for k in self.keys():
            d[k] = self[k]
        return d


class RedisList(collections.MutableSequence):
    """A list backed by Redis, using the Redis linked list construct, and stored a single Redis value.
    Operations on the ends of the list, and len() are O(1).
    Operations on elements by index are O(N)."""

    def __init__(self, name, redis=StrictRedis(), serializer=pickle):
        """

        :param name: The key for this entry in Redis
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer

    def __getitem__(self, index):
        rval = self.redis.lindex(self.name, index)
        if rval is None:
            raise IndexError("empty list")
        return self.serializer.loads(rval)

    def __setitem__(self, index, value):
        self.redis.lset(self.name, index, self.serializer.dumps(value))

    def __delitem__(self, index):
        self.redis.pipeline().lset(self.name, index, '-=-DELETING-=-').lrem(self.name, 1, '-=-DELETING-=-').execute()

    def __len__(self):
        return self.redis.llen(self.name)

    def __insert(self, pipe, index, value):
        value = self.serializer.dumps(value)
        if index >= pipe.llen(self.name):
            pipe.rpush(self.name, value)
        else:
            current = pipe.lindex(self.name, index)
            pipe.lset(self.name, index, '-=-INSERTING-=-')
            pipe.linsert(self.name, 'BEFORE', '-=-INSERTING-=-', value)
            pipe.linsert(self.name, 'AFTER', '-=-INSERTING-=-', current)
            pipe.lrem(self.name, 1, '-=-INSERTING-=-')

    def insert(self, index, value):
        self.redis.transaction(lambda pipe: self.__insert(pipe, index, value), self.name)

    def append(self, value):
        self.redis.rpush(self.name, self.serializer.dumps(value))

    def extend(self, values):
        self.redis.rpush(self.name, *[self.serializer.dumps(v) for v in values])

    def clear(self):
        self.redis.delete(self.name)

    def remove(self, value):
        if not self.redis.lrem(self.name, 1, self.serializer.dumps(value)):
            raise ValueError()

    def __pop(self, pipe, index, rbox):
        bval = pipe.lindex(self.name, index)
        if bval is None:
            raise IndexError()

        rbox.append(bval)
        pipe.lset(self.name, index, '-=-DELETING-=-')
        pipe.lrem(self.name, 1, '-=-DELETING-=-')

    def pop(self, index=-1):
        if index == -1:
            rval = self.redis.rpop(self.name)
        elif index == 0:
            rval = self.redis.lpop(self.name)
        else:
            rbox = []
            self.redis.transaction(lambda pipe: self.__pop(pipe, index, rbox), self.name)
            if len(rbox):
                rval = rbox[-1]
            else:
                raise IndexError()
        if rval is None:
            raise IndexError()
        return self.serializer.loads(rval)

    def __iter__(self):
        return [self.serializer.loads(x) for x in self.redis.lrange(self.name, 0, self.__len__())].__iter__()

    def __contains__(self, item):
        try:
            return self.index(item) is not None and True
        except ValueError:
            return False

    def __reversed__(self):
        return list(self).__reversed__()

    def __index(self, pipe, value, start, stop, rbox):
        if stop is None:
            stop = pipe.llen(self.name)
        bi = self.serializer.dumps(value)
        ix = 0
        for x in pipe.lrange(self.name, start, stop):
            if x == bi:
                rbox.append(ix)
                return
            ix += 1

    def index(self, value, start=0, stop=None):
        rbox = []
        self.redis.transaction(lambda pipe: self.__index(pipe, value, start, stop, rbox), self.name)
        if len(rbox):
            return rbox[-1]
        else:
            raise ValueError()

    def copy(self):
        return list(self)

    def __str__(self):
        return '[%s]' % ', '.join(map(repr, self))


class RedisSet(collections.MutableSet):
    """
    A set, backed by the Redis set construct.
    """

    def __init__(self, name, redis=StrictRedis(), serializer=pickle):
        """

        :param name: The key for this entry in Redis
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer

    def __iter__(self):
        for item in self.redis.sscan_iter(self.name):
            yield self.serializer.loads(item)

    def __len__(self):
        return self.redis.scard(self.name)

    def __contains__(self, item):
        return self.redis.sismember(self.name, self.serializer.dumps(item))

    def update(self, *others):
        return self.add(*[item for sublist in others for item in sublist])

    def add(self, *item):
        """
        :param item: One or more items to be added
        :return: the number of things added
        """
        if len(item):
            # The call to __hash__ for each item insures it's hashable (i.e. unmodifiable), and thus suitable for a set.
            # These sets are persisted and unmodifiable once they're saved, but I haven't thought of a good reason
            # to change the basic contract for set - because this set will also be transformed into an in-memory set
            # in some cases.
            return self.redis.sadd(self.name,
                                   *[(item.__hash__() or True) and self.serializer.dumps(item) for item in item])
        else:
            return 0

    def discard(self, item):
        return self.redis.srem(self.name, self.serializer.dumps(item))

    def copy(self):
        return set(self.__iter__())

    def clear(self):
        self.redis.delete(self.name)

    def __str__(self):
        return '{%s}' % ', '.join(map(repr, self))


class RedisDict(collections.MutableMapping):
    """
    A dictionary, backed by Redis
    """

    def __init__(self, name, redis=StrictRedis(), serializer=pickle, key_serializer=pickle):
        """

        :param name: The key for this entry in Redis
        :param redis: The StrictRedis connection to use
        :param serializer: An object containing functions "dumps" to turn an object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        :param key_serializer: Like serializer, but applied to keys
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer
        self.key_serializer = key_serializer

    def __getitem__(self, item):
        val = self.redis.hget(self.name, self.key_serializer.dumps(item))
        if val is None:
            raise KeyError()
        return self.serializer.loads(val)

    def __setitem__(self, item, value):
        item.__hash__()  # raise a TypeError if it isn't immutable
        self.redis.hset(self.name, self.key_serializer.dumps(item), self.serializer.dumps(value))

    def __delitem__(self, item):
        if not self.redis.hdel(self.name, self.key_serializer.dumps(item)):
            raise KeyError()

    def __iter__(self):
        for k, v in self.redis.hscan_iter(self.name):
            yield self.key_serializer.loads(k)

    def __len__(self):
        return self.redis.hlen(self.name)

    def clear(self):
        self.redis.delete(self.name)

    def copy(self):
        d = {}
        d.update(self)
        return d

    def __str__(self):
        return '{%s}' % ', '.join(([": ".join(map(repr, (k, v))) for k, v in iteritems(self)]))


class RedisSortedSet(collections.MutableMapping):
    """
    A Redis sorted set wrapped as a dict.  Entries are stored in the dictionary keys, scores are their values.
    Items are sorted in order by their values.  Values must be floating point numbers.
    """

    #     This class provides concrete generic implementations of all
    # methods except for __getitem__, __setitem__, __delitem__,
    #     __iter__, and __len__.

    def __init__(self, name, redis=StrictRedis(), serializer=pickle):
        """

        :param name: The name of this set in Redis
        :param redis: The StrictRedis you want to use
        :param serializer: An object containing functions "dumps" to turn an object (to store) into a byte array, and
             "loads" to turn a byte array into an object.  Default = pickle
        """
        self.name = name
        self.redis = redis
        self.serializer = serializer

    def __contains__(self, item):
        """Test to see if a key is in the set. O(1)"""
        return self.redis.zscore(self.name, self.serializer.dumps(item)) is not None

    def copy(self):
        """Create a Python OrderedDict of the contents of this set"""
        return OrderedDict([(self.serializer.loads(k), v) for k, v in self.redis.zscan_iter(self.name)])

    def __iter__(self):
        """Iterate over the whole set. O(N)"""
        return [self.serializer.loads(k) for k, v in self.redis.zscan_iter(self.name)].__iter__()

    def __len__(self):
        """Get the size of the set. O(1)"""
        return self.redis.zcard(self.name)

    def __getitem__(self, key):
        """Get the score of an item in the set. O(log N)"""
        rval = self.redis.zscore(self.name, self.serializer.dumps(key))
        if rval is None:
            raise KeyError(str(key))
        return rval

    def __setitem__(self, key, value):
        """Put an item in the set. O(log N)"""
        key.__hash__()  # See that it's hashable, otherwise it's not suitable for a key
        self.redis.zadd(self.name, value + 0.0, self.serializer.dumps(key))

    def index(self, value):
        """Return the rank of the value (its ordinal position in the set).  O(log N)"""
        # This signature doesn't match the one from collections.Sequence because specifying range is nonsense
        rval = self.redis.zrank(self.name, self.serializer.dumps(value))
        if rval is not None:
            return rval
        else:
            raise ValueError()

    def __delitem__(self, value):
        if self.redis.zrem(self.name, self.serializer.dumps(value)) == 0:
            raise KeyError()

    def update(*args, **kwds):
        newstuff = {}
        self = args[0]
        args = args[1:]
        newstuff.update(*args, **kwds)
        self.redis.zadd(self.name,
                        *[i for sub in [(v + 0, self.serializer.dumps(k)) for k, v in iteritems(newstuff)] for i in
                          sub])

    def clear(self):
        self.redis.delete(self.name)

    def __str__(self):
        return 'RedisSortedSet({%s})' % ', '.join(([": ".join(map(repr, (k, v))) for k, v in iteritems(self)]))
