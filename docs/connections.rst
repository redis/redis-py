Connecting to Redis
###################


Generic Client
**************

This is the client used to connect directly to a standard Redis node.

.. autoclass:: redis.Redis
   :members:


Sentinel Client
***************

Redis `Sentinel <https://redis.io/topics/sentinel>`_ provides high availability for Redis. There are commands that can only be executed against a Redis node running in sentinel mode. Connecting to those nodes, and executing commands against them requires a Sentinel connection.

Connection example (assumes Redis exists on the ports listed below):

   >>> from redis import Sentinel
   >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
   >>> sentinel.discover_master('mymaster')
   ('127.0.0.1', 6379)
   >>> sentinel.discover_slaves('mymaster')
   [('127.0.0.1', 6380)]

Sentinel
========
.. autoclass:: redis.sentinel.Sentinel
    :members:

SentinelConnectionPool
======================
.. autoclass:: redis.sentinel.SentinelConnectionPool
    :members:


Cluster Client
**************

This client is used for connecting to a Redis Cluster.

RedisCluster
============
.. autoclass:: redis.cluster.RedisCluster
    :members:

ClusterNode
===========
.. autoclass:: redis.cluster.ClusterNode
    :members:


Async Client
************

See complete example: `here <examples/asyncio_examples.html>`_

This client is used for communicating with Redis, asynchronously.

.. autoclass:: redis.asyncio.client.Redis
    :members:


Async Cluster Client
********************

RedisCluster (Async)
====================
.. autoclass:: redis.asyncio.cluster.RedisCluster
    :members:
    :member-order: bysource

ClusterNode (Async)
===================
.. autoclass:: redis.asyncio.cluster.ClusterNode
    :members:
    :member-order: bysource

ClusterPipeline (Async)
=======================
.. autoclass:: redis.asyncio.cluster.ClusterPipeline
    :members: execute_command, execute
    :member-order: bysource


Connection
**********

See complete example: `here <examples/connection_examples.html>`_

Connection
==========
.. autoclass:: redis.connection.Connection
    :members:

Connection (Async)
==================
.. autoclass:: redis.asyncio.connection.Connection
    :members:


Connection Pools
****************

See complete example: `here <examples/connection_examples.html>`_

ConnectionPool
==============
.. autoclass:: redis.connection.ConnectionPool
    :members:

ConnectionPool (Async)
======================
.. autoclass:: redis.asyncio.connection.ConnectionPool
    :members:
