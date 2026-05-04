RESP 3 Features
===============

As of version 5.0, redis-py supports the `RESP 3 standard <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>`_. Starting with redis-py 8.0, clients use RESP3 on the wire by default.

By default, redis-py keeps legacy RESP2-compatible Python response shapes for
existing applications. Set ``protocol=3`` explicitly when your application
should receive RESP3-specific Python response shapes or when you want the wire
protocol choice to be visible in code. Set ``protocol=2`` to force RESP2 on the
wire. Set
``legacy_responses=False`` to opt in to protocol-independent unified response
shapes; see :doc:`unified_responses`.

Connecting
-----------

The default connection already uses RESP3 on the wire in redis-py 8.0 and
later while preserving legacy RESP2-compatible Python response shapes. The
following examples show how to set ``protocol=3`` explicitly when you want
RESP3-specific response shapes or visible protocol configuration for standard,
async, and cluster clients.

Connect with a standard connection, explicitly specifying RESP3:

.. code:: python

    >>> import redis
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3)
    >>> r.ping()

Or using the URL scheme:

.. code:: python

    >>> import redis
    >>> r = redis.from_url("redis://localhost:6379?protocol=3")
    >>> r.ping()

Connect with async, explicitly specifying RESP3:

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
For more information please check the `Redis client-side caching documentation <https://redis.io/docs/latest/develop/use/client-side-caching/>`_.
Please notice that this feature is available only with RESP3 protocol enabled
in sync clients. redis-py 8.0 and later use RESP3 on the wire by default, and
the examples below pass ``protocol=3`` explicitly to make the requirement clear.
Supported in standalone, Cluster, and Sentinel clients.

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

More comprehensive documentation soon will be available at the `Redis documentation site <https://redis.io/docs/latest/>`_.
