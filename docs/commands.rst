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

Scanning pending stream entries
===============================

Use ``xautoclaim(..., justid=True, full_response=True)`` to scan a consumer
group's pending entries by ID. The reply contains the next scan cursor and the
claimed IDs. On Redis 7.0 and later, it also contains the IDs that Redis removed
from the group's pending list because the stream entries had been deleted.

Pass the cursor as ``start_id`` on the next call, even when the claimed ID list
is empty. The cursor ``0-0`` marks the end of the scan. With ``justid=True``
alone, the method returns the claimed IDs. Both forms leave delivery counters
unchanged and omit message payloads.

Sentinel Commands
*****************
.. autoclass:: redis.commands.sentinel.SentinelCommands
   :inherited-members:

Redis Cluster Commands
**********************

The following `Redis commands <https://redis.io/commands>`_ are available within a `Redis Cluster <https://redis.io/topics/cluster-tutorial>`_.  Generally they can be used as functions on your redis connection.

.. autoclass:: redis.commands.cluster.RedisClusterCommands
   :inherited-members:
