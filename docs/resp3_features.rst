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

Which commands are cached
~~~~~~~~~~~~~~~~~~~~~~~~~

A reply is cached only when both of the following hold:

1. The command's metadata says its reply may be cached at all: it is ``readonly``, is not
   ``blocking``, takes at least one key name argument, and carries none of the
   ``nondeterministic_output``, ``script_runner`` or ``dont_cache`` markers.
2. The client can identify the key arguments of that particular invocation. A command whose
   keys the client does not yet extract executes normally with caching skipped - it never
   fails and never changes the reply.

The first question is answered by a ``redis.commands.metadata.MetadataResolver``, which is
also what the Cluster client resolves its routing by. By default the client resolves the
command metadata this library ships, so enabling caching adds no ``COMMAND`` round trips.
An unknown command is never cached.

A different resolver can be supplied as ``metadata_resolver``, which is the seam to implement
if you need eligibility decided some other way - it is a small public ABC, and
``StaticMetadataResolver`` shows what a resolver has to answer. Resolvers chain through
``with_fallback``, first match wins, so a resolver placed in front of the static one overrides
the commands it carries and the static records answer for everything else.

Eligibility can also be decided from the connected server, by building a
``DynamicMetadataResolver`` from a live ``COMMAND`` reply. Use it with care: reading that reply
relies on ``CommandsParser``, which lives in the private ``redis._parsers`` package and is not
part of the public API.

.. code:: python

    >>> import redis
    >>> from redis._parsers.commands import CommandsParser
    >>> from redis.cache import CacheConfig
    >>> from redis.commands.metadata import DynamicMetadataResolver, StaticMetadataResolver
    >>> records = CommandsParser(redis.Redis(host='localhost', port=6379)).get_commands_metadata_cache()
    >>> resolver = StaticMetadataResolver(fallback=DynamicMetadataResolver(records))
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(), metadata_resolver=resolver)

Chained this way the server answers only for the commands the shipped table does not carry;
swap the order to let the server override it.

The static table stays the more trustworthy source of the two. It carries the commands
whose server metadata is incomplete or wrong - ``TOUCH`` and ``VRANDMEMBER``, and the
read-only script runners on servers older than 8.10 - and it withholds the routing policies of
the ``movablekeys`` reads (``SINTERCARD``, ``ZDIFF``, ``ZINTER``, ``ZINTERCARD``, ``ZUNION``,
``XREAD``) so the Cluster client keeps resolving their keys itself instead of routing them by
derived keyless policies.

The same argument is accepted by ``ConnectionPool`` (configure it there when you supply your
own ``connection_pool=``) and by ``RedisCluster``, which shares the one resolver with every
node's client. On the Cluster client it also supersedes ``policy_resolver``: given only
``metadata_resolver``, routing is derived from it, so one object serves both. An explicit
``policy_resolver`` still decides routing, for backwards compatibility.

``CacheConfig.DEFAULT_ALLOW_LIST`` is deprecated and no longer consulted.

Client-side caching is not yet implemented in the async clients, so
``redis.asyncio.Redis`` and ``redis.asyncio.RedisCluster`` take no ``metadata_resolver``
argument.

More comprehensive documentation soon will be available at the `Redis documentation site <https://redis.io/docs/latest/>`_.
