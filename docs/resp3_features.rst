RESP 3 Features
===============

As of version 5.0, redis-py supports the `RESP 3 standard <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>`_. Practically, this means that client using RESP 3 will be faster and more performant as fewer type translations occur in the client. It also means new response types like doubles, true simple strings, maps, and booleans are available.

Connecting
-----------

Enabling RESP3 is no different than other connections in redis-py. In all cases, the connection type must be extending by setting `protocol=3`. The following are some base examples illustrating how to enable a RESP 3 connection.

Connect with a standard connection, but specifying resp 3:

.. code:: python

    >>> import redis
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3)
    >>> r.ping()

Or using the URL scheme:

.. code:: python

    >>> import redis
    >>> r = redis.from_url("redis://localhost:6379?protocol=3")
    >>> r.ping()

Connect with async, specifying resp 3:

.. code:: python

    >>> import redis.asyncio as redis
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3)
    >>> await r.ping()

The URL scheme with the async client

.. code:: python

    >>> import redis.asyncio as Redis
    >>> r = redis.from_url("redis://localhost:6379?protocol=3")
    >>> await r.ping()

Connecting to an OSS Redis Cluster with RESP 3

.. code:: python

    >>> from redis.cluster import RedisCluster, ClusterNode
    >>> r = RedisCluster(startup_nodes=[ClusterNode('localhost', 6379), ClusterNode('localhost', 6380)], protocol=3)
    >>> r.ping()

Push notifications
------------------

Push notifications are a way that redis sends out of band data. The RESP 3 protocol includes a `push type <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md#push-type>`_ that allows our client to intercept these out of band messages. By default, clients will log simple messages, but redis-py includes the ability to bring your own function processor.

This means that should you want to perform something, on a given push notification, you specify a function during the connection, as per this examples:

.. code:: python

    >> from redis import Redis
    >>
    >> def our_func(message):
    >>    if message.find("This special thing happened"):
    >>        raise IOError("This was the message: \n" + message)
    >>
    >> r = Redis(protocol=3)
    >> p = r.pubsub(push_handler_func=our_func)

In the example above, upon receipt of a push notification, rather than log the message, in the case where specific text occurs, an IOError is raised. This example, highlights how one could start implementing a customized message handler.

Client-side caching
-------------------

Client-side caching is a technique used to create high performance services.
It utilizes the memory on application servers, typically separate from the database nodes, to cache a subset of the data directly on the application side.
For more information please check `official Redis documentation <https://redis.io/docs/latest/develop/use/client-side-caching/>`_.
Please notice that this feature only available with RESP3 protocol enabled in sync client only. Supported in standalone, Cluster and Sentinel clients.

Basic usage:

Enable caching with default configuration:

.. code:: python

    >>> import redis
    >>> from redis.cache import CacheConfig
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3, cache_config=CacheConfig())

The same interface applies to Redis Cluster and Sentinel.

Enable caching with custom cache implementation:

.. code:: python

    >>> import redis
    >>> from foo.bar import CacheImpl
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3, cache=CacheImpl())

CacheImpl should implement a `CacheInterface` specified in `redis.cache` package.

More comprehensive documentation soon will be available at `official Redis documentation <https://redis.io/docs/latest/>`_.
