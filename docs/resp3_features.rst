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

New Features
------------

Sharded pubsub
~~~~~~~~~~~~~~

`Sharded pubsub <https://redis.io/docs/interact/pubsub/#:~:text=Sharded%20Pub%2FSub%20helps%20to,the%20shard%20of%20a%20cluster.>`_ is a feature introduced with Redis 7.0, and fully supported by redis-py as of 5.0. It helps scale the usage of pub/sub in cluster mode, by having the cluster shard messages to nodes that own a slot for a shard channel. Here, the cluster ensures the published shard messages are forwarded to the appropriate nodes. Clients subscribe to a channel by connecting to either the master responsible for the slot, or any of its replicas.

This makes use of the `SSUBSCRIBE <https://redis.io/commands/ssubscribe>`_ and `SPUBLISH <https://redis.io/commands/spublish>`_ commands within Redis.

The following, is a simplified example:

.. code:: python

    >>> from redis.cluster import RedisCluster, ClusterNode
    >>> r = RedisCluster(startup_nodes=[ClusterNode('localhost', 6379), ClusterNode('localhost', 6380)], protocol=3)
    >>> p = r.pubsub()
    >>> p.ssubscribe('foo')
    >>> # assume someone sends a message along the channel via a publish
    >>> message = p.get_sharded_message()

Push notifications
~~~~~~~~~~~~~~~~~~

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