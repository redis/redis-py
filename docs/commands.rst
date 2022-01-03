Redis Commands
##############

Core Commands
*************

The following functions can be used to replicate their equivalent `Redis command <https://redis.io/commands>`_.  Generally they can be used as functions on your redis connection.  For the simplest example, see below:

Getting and settings data in redis::

   import redis
   r = redis.Redis(decode_responses=True)
   r.set('mykey', 'thevalueofmykey')
   r.get('mykey')

.. autoclass:: redis.commands.core.CoreCommands
   :inherited-members:

Sentinel Commands
*****************
.. autoclass:: redis.commands.sentinel.SentinelCommands
   :inherited-members:

Redis Cluster Commands
**********************

The following `Redis commands <https://redis.io/commands>`_ are available within a `Redis Cluster <https://redis.io/topics/cluster-tutorial>`_.  Generally they can be used as functions on your redis connection.

.. autoclass:: redis.commands.cluster.RedisClusterCommands
   :inherited-members:
