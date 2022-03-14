Connecting to Redis
#####################

Generic Client
**************

This is the client used to connect directly to a standard redis node.

.. autoclass:: redis.Redis
   :members:

Sentinel Client
***************

Redis `Sentinel <https://redis.io/topics/sentinel>`_ provides high availability for Redis. There are commands that can only be executed against a redis node running in sentinel mode. Connecting to those nodes, and executing commands against them requires a Sentinel connection.

Connection example (assumes redis redis on the ports listed below):

   >>> from redis import Sentinel
   >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
   >>> sentinel.discover_master('mymaster')
   ('127.0.0.1', 6379)
   >>> sentinel.discover_slaves('mymaster')
   [('127.0.0.1', 6380)]

.. autoclass:: redis.sentinel.Sentinel
    :members:

.. autoclass:: redis.sentinel.SentinelConnectionPool
    :members:

Cluster Client
**************

This client is used for connecting to a redis cluser.

.. autoclass:: redis.cluster.RedisCluster
    :members:

Connection Pools
*****************
.. autoclass:: redis.connection.ConnectionPool
    :members:

More connection examples can be found `here <examples/connection_examples.html>`_.

Async Client
************

This client is used for communicating with Redis, asynchronously.

.. autoclass:: redis.asyncio.connection.Connection
    :members:

More connection examples can be found `here <examples/asyncio_examples.html>`_