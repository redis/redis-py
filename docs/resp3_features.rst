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

Tracking modes
~~~~~~~~~~~~~~

Eligibility decides what *may* be cached. ``tracking_mode`` and ``cache_predicate`` decide what
the application *wants* cached, and with it how much the server has to remember.

By default every cache-managed connection sends ``CLIENT TRACKING ON``, so the server remembers
the keys of every read-only, keyed command that connection performs - whether or not the reply
was stored. ``redis.cache.TrackingMode`` selects a narrower contract:

- ``TrackingMode.PLAIN`` - default behaviour. Every trackable read is tracked, and every
  eligible reply is stored.
- ``TrackingMode.OPTIN`` - ``CLIENT TRACKING ON OPTIN``. The server remembers nothing unless
  ``CLIENT CACHING YES`` comes right before the read, which the client sends for exactly the
  misses it is about to store. Pick it when you cache a small, chosen subset.
- ``TrackingMode.OPTOUT`` - ``CLIENT TRACKING ON OPTOUT``. The server remembers every trackable
  read unless ``CLIENT CACHING NO`` comes right before it, which the client sends for the reads
  it will not store. Pick it when you cache almost everything.

``cache_predicate`` is the intent decision. It is called with the command name and the keys of
the invocation, and is consulted under ``optin`` and ``optout`` only - passing it with ``plain``
warns and is ignored. The keys arrive as the
invocation supplied them, so a predicate that inspects them should not assume ``str`` or
``bytes`` - ``redis.utils.str_if_bytes`` normalises both, as in the examples below.

.. code:: python

    >>> import redis
    >>> from redis.cache import CacheConfig, TrackingMode
    >>> from redis.utils import str_if_bytes
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(
    ...                     max_size=10_000,
    ...                     tracking_mode=TrackingMode.OPTOUT,
    ...                     cache_predicate=lambda command, keys: not str_if_bytes(keys[0]).startswith('counter:'),
    ...                 ))
    >>> r.get('user:42')          # sent alone; stored, and tracked by default
    >>> r.get('counter:hits')     # CLIENT CACHING NO + GET, one write; not stored, not tracked

Under ``optin`` the predicate selects what to cache instead:

.. code:: python

    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(
    ...                     tracking_mode=TrackingMode.OPTIN,
    ...                     cache_predicate=lambda command, keys: str_if_bytes(keys[0]).startswith('user:'),
    ...                 ))
    >>> r.get('user:42')          # miss: CLIENT CACHING YES + GET, one write; stored, tracked
    >>> r.get('user:42')          # hit: nothing on the wire
    >>> r.get('session:9')        # sent alone; not stored, not tracked

``optin`` with no ``cache_predicate`` caches **nothing** - every read is sent alone and left
untracked. That configuration is inert, and the client warns about it when the config is built.

Some things work the same in every mode:

- A cache hit sends nothing to the server.
- Pipelines, transactions and ``send_packed_command`` bypass the cache. Under ``optout`` the
  server still tracks the keys they read.
- A command redirected with ``ASK`` is sent without ``CLIENT CACHING``, and its reply is not
  stored. ``ASKING`` and ``CLIENT CACHING`` each apply only to the command that immediately
  follows them, so one command cannot carry both. Pairing the redirected read with
  ``CLIENT CACHING`` would strip the ``ASK`` allowance, and the read would be redirected
  again. The reply is not stored because it belongs to a slot that is migrating. Under
  ``optout`` the server tracks the read by default anyway.
- Under ``optout``, the client sends ``CLIENT CACHING NO`` only for commands that the metadata
  table marks ``readonly``. Any other read stays tracked. The worst case is an unused entry in
  the server's invalidation table, never a stale reply.
- ``CLIENT CACHING`` is refused with a ``RedisError`` on every connection of a client that has
  a cache, including inside pipelines and transactions. The server applies the flag to the next
  command on that socket, and a pooled client cannot promise which command that is: a stray
  ``CLIENT CACHING NO`` under ``optout`` would leave the next cached read untracked, so its
  stored reply would never be invalidated. The client sends ``CLIENT CACHING`` itself,
  together with the read it applies to, whenever the tracking mode needs it. A client without
  a cache passes the command to the server as before.

The mode is sent in the tracking handshake, because the server refuses to switch a live
connection between ``OPTIN`` and ``OPTOUT``. A configuration change therefore applies to new
connections only.

Refresh on invalidation
~~~~~~~~~~~~~~~~~~~~~~~

By default (``InvalidationPolicy.EVICT``) an invalidated entry is removed, and the next read
of it goes to the server. With ``invalidation_policy=InvalidationPolicy.REFRESH`` the entry
is still removed at once, and the client then re-reads it in the background, so the next
read can be a hit:

.. code:: python

    >>> import redis
    >>> from redis.cache import CacheConfig, InvalidationPolicy
    >>> r = redis.Redis(host='localhost', port=6379, protocol=3,
    ...                 cache_config=CacheConfig(
    ...                     invalidation_policy=InvalidationPolicy.REFRESH,
    ...                     refresh_max_inflight=32,
    ...                 ))

This works in standalone, Cluster and Sentinel clients. ``refresh_max_inflight`` defaults to
16 when omitted and must be a positive integer; passing it under ``InvalidationPolicy.EVICT``
warns and has no effect.

A refresh re-runs the exact command that filled the entry, through the same path as a read,
so the refreshed reply is stored and tracked exactly as a read's would be. Refresh never makes
the cache serve an older value than it would without it: the old value is gone before the
refresh is queued, and a reply the server invalidates again while it is in flight is not
stored. A server ``FLUSHALL`` or ``FLUSHDB`` empties the cache and refreshes nothing, and
every time a connection empties the cache, refreshes still queued on its pool are dropped.

Refresh is best effort, and has costs to weigh before turning it on:

- Every refreshed entry costs one extra read on the server, whether or not the application
  reads the key again. The server reports every change to a cached key, whoever made it, so a
  key another service writes often is refreshed as often.
- Each connection pool refreshes on a single background thread, one key at a time and one
  round trip per key, so refreshes never run in parallel within a pool. The thread starts
  with the first refresh, ends after 30 seconds with nothing to do, and is stopped by the
  pool's ``close()``, which also drops queued refreshes.
- ``refresh_max_inflight`` is the size of that thread's queue: the most keys waiting for a
  refresh, counting the one being refreshed. It bounds the backlog, not concurrency. When
  invalidations arrive faster than one round trip per key, the queue fills, and an
  invalidation that finds it full is handled as without refresh: the entry is removed and
  the next read fetches it. A failed refresh, a nil reply, and a reply the
  ``cache_predicate`` does not select do the same.
- Each refresh borrows a connection from the pool while it runs, and only one the pool can
  hand out at once: an idle connection, or a new one below ``max_connections``. When every
  connection is in use and the pool is at its limit, or while a ``BlockingConnectionPool``
  is in maintenance, the refresh is skipped and the entry stays removed, so a refresh never
  makes an application command fail or wait to get a connection. It can still take the last
  free one, for the length of one round trip. A client built with
  ``single_connection_client=True`` opens a second connection for refreshes.
- An invalidation is processed when the connection that received it next sends a
  command, or when a read finds the invalidated entry: before serving a cached value, the
  reading connection first drains the invalidations queued on the connection that stored
  it. On a busy pool the first happens almost immediately; on an idle one it is often the
  next read of the same key, which processes the invalidation, finds the entry removed and
  fetches the value itself, so refresh gains nothing.
- A refresh that fails with a connection error or a timeout closes that connection, which
  empties the cache, as any closed caching connection does.
- On a Cluster client every node's pool refreshes on its own node, within its own bound, and
  a flush drops only the refreshes queued on the pool that read it. A refresh answered with
  ``MOVED`` or ``ASK`` is not redirected: the entry stays removed, and the next read is routed
  as usual.
- ``cache_predicate`` is also called from the refresh thread, so it must be thread-safe.
- With refresh, a custom ``CacheInterface`` implementation has invalidated entries removed
  through ``take_entries_by_redis_keys``, not ``delete_by_redis_keys``.

Refreshes show up in the cache metrics:

- In ``redis.client.csc.requests``, each refresh is counted like an application read,
  normally as a miss.
- In ``redis.client.csc.evictions``, the invalidated entry is still counted, with reason
  ``invalidation``.
- In ``redis.client.csc.refreshes``, each refresh that reached the server is counted as
  ``success`` when it was answered, stored or not, or ``failure`` when it raised. Each one
  that found the queue full, or was skipped because the pool had no free connection, is
  counted as ``rejected``. A refresh skipped because a read already re-fetched the key, or
  dropped by a flush, is not counted.

Async clients
~~~~~~~~~~~~~

Client-side caching is not yet implemented in the async clients, so
``redis.asyncio.Redis`` takes no ``metadata_resolver`` argument. However,
``redis.asyncio.RedisCluster`` accepts ``metadata_resolver`` for dynamic replica routing.

More comprehensive documentation soon will be available at the `Redis documentation site <https://redis.io/docs/latest/>`_.
