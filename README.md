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


Author
------

redis-py is developed and maintained by Andy McCurdy (sedrik@gmail.com).
It can be found here: http://github.com/andymccurdy/redis-py

Special thanks to:

* Ludovico Magnocavallo, author of the original Python Redis client, from
  which some of the socket code is still used.
* Alexander Solovyov for ideas on the generic response callback system.
* Paul Hubbard for initial packaging support.

