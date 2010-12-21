redis-py
========

This is the Python interface to the Redis key-value store.


Usage
-----

    >>> import redis
    >>> r = redis.Redis(host='localhost', port=6379, db=0)
    >>> r.set('foo', 'bar')   # or r['foo'] = 'bar'
    True
    >>> r.get('foo')   # or r['foo']
    'bar'

For a complete list of commands, check out the list of Redis commands here:
http://code.google.com/p/redis/wiki/CommandReference

Installation
------------

    $ sudo easy-install redis

alternatively:

    $ sudo pip install redis

From sources:

    $ sudo python setup.py install

Versioning scheme
-----------------

redis-py is versioned after Redis. So, for example, redis-py 2.0.0 should 
support all the commands available in Redis 2.0.0. 

API Reference
-------------

### append(self, key, value)
  Appends the string _value_ to the value at _key_. If _key_
  doesn't already exist, create it with a value of _value_.
  Returns the new length of the value at _key_.

### bgrewriteaof(self)
  Tell the Redis server to rewrite the AOF file from data in memory.

### bgsave(self)
  Tell the Redis server to save its data to disk.  Unlike save(),
  this method is asynchronous and returns immediately.

### blpop(self, keys, timeout=0)
  LPOP a value off of the first non-empty list
  named in the _keys_ list.
  
  If none of the lists in _keys_ has a value to LPOP, then block
  for _timeout_ seconds, or until a value gets pushed on to one
  of the lists.
  
  If timeout is 0, then block indefinitely.

### brpop(self, keys, timeout=0)
  RPOP a value off of the first non-empty list
  named in the _keys_ list.
  
  If none of the lists in _keys_ has a value to LPOP, then block
  for _timeout_ seconds, or until a value gets pushed on to one
  of the lists.
  
  If timeout is 0, then block indefinitely.

### dbsize(self)
  Returns the number of keys in the current database

### decr(self, name, amount=1)
  Decrements the value of _key_ by _amount_.  If no key exists,
  the value will be initialized as 0 - _amount_

### delete(self, *names)
  Delete one or more keys specified by _names_

### encode(self, value)
  Encode _value_ using the instance's charset

### execute_command(self, *args, **options)
  Sends the command to the redis server and returns it's response

### exists(self, name)
  Returns a boolean indicating whether key _name_ exists

### expire(self, name, time)
  Set an expire flag on key _name_ for _time_ seconds

### expireat(self, name, when)
  Set an expire flag on key _name_. _when_ can be represented
  as an integer indicating unix time or a Python datetime object.

### flush(self, all_dbs=False)

### flushall(self)
  Delete all keys in all databases on the current host

### flushdb(self)
  Delete all keys in the current database

### get(self, name)
  Return the value at key _name_, or None of the key doesn't exist

### get_connection(self, host, port, db, password, socket_timeout)
  Returns a connection object

### getset(self, name, value)
  Set the value at key _name_ to _value_ if key doesn't exist
  Return the value at key _name_ atomically

### hdel(self, name, key)
  Delete _key_ from hash _name_

### hexists(self, name, key)
  Returns a boolean indicating if _key_ exists within hash _name_

### hget(self, name, key)
  Return the value of _key_ within the hash _name_

### hgetall(self, name)
  Return a Python dict of the hash's name/value pairs

### hincrby(self, name, key, amount=1)
  Increment the value of _key_ in hash _name_ by _amount_

### hkeys(self, name)
  Return the list of keys within hash _name_

### hlen(self, name)
  Return the number of elements in hash _name_

### hmget(self, name, keys)
  Returns a list of values ordered identically to _keys_

### hmset(self, name, mapping)
  Sets each key in the _mapping_ dict to its corresponding value
  in the hash _name_

### hset(self, name, key, value)
  Set _key_ to _value_ within hash _name_
  Returns 1 if HSET created a new field, otherwise 0

### hsetnx(self, name, key, value)
  Set _key_ to _value_ within hash _name_ if _key_ does not
  exist.  Returns 1 if HSETNX created a field, otherwise 0.

### hvals(self, name)
  Return the list of values within hash _name_

### incr(self, name, amount=1)
  Increments the value of _key_ by _amount_.  If no key exists,
  the value will be initialized as _amount_

### info(self)
  Returns a dictionary containing information about the Redis server

### keys(self, pattern='*')
  Returns a list of keys matching _pattern_

### lastsave(self)
  Return a Python datetime object representing the last time the
  Redis database was saved to disk

### lindex(self, name, index)
  Return the item from list _name_ at position _index_
  
  Negative indexes are supported and will return an item at the
  end of the list

### listen(self)
  Listen for messages on channels this client has been subscribed to

### llen(self, name)
  Return the length of the list _name_

###lock(self, name, timeout=None, sleep=0.10000000000000001)
  Return a new Lock object using key _name_ that mimics
  the behavior of threading.Lock.
  
  If specified, _timeout_ indicates a maximum life for the lock.
  By default, it will remain locked until release() is called.
  
  _sleep_ indicates the amount of time to sleep per loop iteration
  when the lock is in blocking mode and another client is currently
  holding the lock.

### lpop(self, name)
  Remove and return the first item of the list _name_

### lpush(self, name, value)
  Push _value_ onto the head of the list _name_

### lrange(self, name, start, end)
  Return a slice of the list _name_ between
  position _start_ and _end_
  
  _start_ and _end_ can be negative numbers just like
  Python slicing notation

### lrem(self, name, value, num=0)
  Remove the first _num_ occurrences of _value_ from list _name_
  
  If _num_ is 0, then all occurrences will be removed

### lset(self, name, index, value)
  Set _position_ of list _name_ to _value_

### ltrim(self, name, start, end)
  Trim the list _name_, removing all values not within the slice
  between _start_ and _end_
  
  _start_ and _end_ can be negative numbers just like
  Python slicing notation

### mget(self, keys, *args)
  Returns a list of values ordered identically to _keys_
  
  * Passing *args to this method has been deprecated *

### move(self, name, db)
  Moves the key _name_ to a different Redis database _db_

### mset(self, mapping)
  Sets each key in the _mapping_ dict to its corresponding value

### msetnx(self, mapping)
  Sets each key in the _mapping_ dict to its corresponding value if
  none of the keys are already set

### parse_response(self, command_name, catch_errors=False, **options)
  Parses a response from the Redis server

### ping(self)
  Ping the Redis server

### pipeline(self, transaction=True)
  Return a new pipeline object that can queue multiple commands for
  later execution. _transaction_ indicates whether all commands
  should be executed atomically. Apart from multiple atomic operations,
  pipelines are useful for batch loading of data as they reduce the
  number of back and forth network operations between client and server.

### pop(self, name, tail=False)
  Pop and return the first or last element of list _name_
  
  This method has been deprecated, use _Redis.lpop_ or _Redis.rpop_ instead.

### psubscribe(self, patterns)
  Subscribe to all channels matching any pattern in _patterns_

### publish(self, channel, message)
  Publish _message_ on _channel_.
  Returns the number of subscribers the message was delivered to.

### punsubscribe(self, patterns=[])
  Unsubscribe from any channel matching any pattern in _patterns_.
  If empty, unsubscribe from all channels.

### push(self, name, value, head=False)
  Push _value_ onto list _name_.
  
  This method has been deprecated, use __Redis.lpush__ or __Redis.rpush__ instead.

### randomkey(self)
  Returns the name of a random key

### rename(self, src, dst, **kwargs)
  Rename key _src_ to _dst_
  
  * The following flags have been deprecated *
  If _preserve_ is True, rename the key only if the destination name
      doesn't already exist

### renamenx(self, src, dst)
  Rename key _src_ to _dst_ if _dst_ doesn't already exist

### rpop(self, name)
  Remove and return the last item of the list _name_

### rpoplpush(self, src, dst)
  RPOP a value off of the _src_ list and atomically LPUSH it
  on to the _dst_ list.  Returns the value.

### rpush(self, name, value)
  Push _value_ onto the tail of the list _name_

### sadd(self, name, value)
  Add _value_ to set _name_

### save(self)
  Tell the Redis server to save its data to disk,
  blocking until the save is complete

### scard(self, name)
  Return the number of elements in set _name_

### sdiff(self, keys, *args)
  Return the difference of sets specified by _keys_

### sdiffstore(self, dest, keys, *args)
  Store the difference of sets specified by _keys_ into a new
  set named _dest_.  Returns the number of keys in the new set.

### select(self, db, host=None, port=None, password=None, socket_timeout=None)
  Switch to a different Redis connection.
  
  If the host and port aren't provided and there's an existing
  connection, use the existing connection's host and port instead.
  
  Note this method actually replaces the underlying connection object
  prior to issuing the SELECT command.  This makes sure we protect
  the thread-safe connections

### set(self, name, value, **kwargs)
  Set the value at key _name_ to _value_
  
  * The following flags have been deprecated *
  If _preserve_ is True, set the value only if key doesn't already
  exist
  If _getset_ is True, set the value only if key doesn't already exist
  and return the resulting value of key

### setex(self, name, value, time)
  Set the value of key _name_ to _value_
  that expires in _time_ seconds

### setnx(self, name, value)
  Set the value of key _name_ to _value_ if key doesn't exist

### sinter(self, keys, *args)
  Return the intersection of sets specified by _keys_

### sinterstore(self, dest, keys, *args)
  Store the intersection of sets specified by _keys_ into a new
  set named _dest_.  Returns the number of keys in the new set.

### sismember(self, name, value)
  Return a boolean indicating if _value_ is a member of set _name_

### smembers(self, name)
  Return all members of the set _name_

### smove(self, src, dst, value)
  Move _value_ from set _src_ to set _dst_ atomically

### sort(self, name, start=None, num=None, by=None, get=None, desc=False, alpha=False, store=None)
  Sort and return the list, set or sorted set at _name_.
  
  _start_ and _num_ allow for paging through the sorted data
  
  _by_ allows using an external key to weight and sort the items.
  Use an "*" to indicate where in the key the item value is located
  
  _get_ allows for returning items from external keys rather than the
  sorted data itself.  Use an "*" to indicate where int he key
  the item value is located
  
  _desc_ allows for reversing the sort
  
  _alpha_ allows for sorting lexicographically rather than numerically
  
  _store_ allows for storing the result of the sort into
  the key _store_

### spop(self, name)
  Remove and return a random member of set _name_

### srandmember(self, name)
  Return a random member of set _name_

### srem(self, name, value)
  Remove _value_ from set _name_

### subscribe(self, channels)
  Subscribe to _channels_, waiting for messages to be published

### substr(self, name, start, end=-1)
  Return a substring of the string at key _name_. _start_ and _end_
  are 0-based integers specifying the portion of the string to return.

### sunion(self, keys, *args)
  Return the union of sets specifiued by _keys_

### sunionstore(self, dest, keys, *args)
  Store the union of sets specified by _keys_ into a new
  set named _dest_.  Returns the number of keys in the new set.

### ttl(self, name)
  Returns the number of seconds until the key _name_ will expire

### type(self, name)
  Returns the type of key _name_

### unsubscribe(self, channels=[])
  Unsubscribe from _channels_. If empty, unsubscribe
  from all channels

### watch(self, name):
  Watches the value at key _name_.

### zadd(self, name, value, score)
  Add member _value_ with score _score_ to sorted set _name_

### zcard(self, name)
  Return the number of elements in the sorted set _name_

### zincr(self, key, member, value=1)
  This has been deprecated, use zincrby instead

### zincrby(self, name, value, amount=1)
  Increment the score of _value_ in sorted set _name_ by _amount_

### zinter(self, dest, keys, aggregate=None)

###zinterstore(self, dest, keys, aggregate=None)
  Intersect multiple sorted sets specified by _keys_ into
  a new sorted set, _dest_. Scores in the destination will be
  aggregated based on the _aggregate_, or SUM if none is provided.

### zrange(self, name, start, end, desc=False, withscores=False)
  Return a range of values from sorted set _name_ between
  _start_ and _end_ sorted in ascending order.
  
  _start_ and _end_ can be negative, indicating the end of the range.
  
  _desc_ indicates to sort in descending order.
  
  _withscores_ indicates to return the scores along with the values.
   The return type is a list of (value, score) pairs

### zrangebyscore(self, name, min, max, start=None, num=None, withscores=False)
  Return a range of values from the sorted set _name_ with scores
  between _min_ and _max_.
  
  If _start_ and _num_ are specified, then return a slice of the range.
  
  _withscores_ indicates to return the scores along with the values.
  The return type is a list of (value, score) pairs

### zrank(self, name, value)
  Returns a 0-based value indicating the rank of _value_ in sorted set
  _name_

### zrem(self, name, value)
  Remove member _value_ from sorted set _name_

### zremrangebyrank(self, name, min, max)
  Remove all elements in the sorted set _name_ with ranks between
  _min_ and _max_. Values are 0-based, ordered from smallest score
  to largest. Values can be negative indicating the highest scores.
  Returns the number of elements removed

### zremrangebyscore(self, name, min, max)
  Remove all elements in the sorted set _name_ with scores
  between _min_ and _max_. Returns the number of elements removed.

### zrevrange(self, name, start, num, withscores=False)
  Return a range of values from sorted set _name_ between
  _start_ and _num_ sorted in descending order.
  
  _start_ and _num_ can be negative, indicating the end of the range.
  
  _withscores_ indicates to return the scores along with the values
  as a dictionary of value => score

### zrevrank(self, name, value)
  Returns a 0-based value indicating the descending rank of
  _value_ in sorted set _name_

### zscore(self, name, value)
  Return the score of element _value_ in sorted set _name_

### zunion(self, dest, keys, aggregate=None)

### zunionstore(self, dest, keys, aggregate=None)
  Union multiple sorted sets specified by _keys_ into
  a new sorted set, _dest_. Scores in the destination will be
  aggregated based on the _aggregate_, or SUM if none is provided.

Author
------

redis-py is developed and maintained by Andy McCurdy (sedrik@gmail.com).
It can be found here: http://github.com/andymccurdy/redis-py

Special thanks to:

* Ludovico Magnocavallo, author of the original Python Redis client, from
  which some of the socket code is still used.
* Alexander Solovyov for ideas on the generic response callback system.
* Paul Hubbard for initial packaging support.

