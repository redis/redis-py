Redis Sentinel Commands
=======================

redis-py can be used together with `Redis
Sentinel <https://redis.io/topics/sentinel>`_ to discover Redis nodes. You
need to have at least one Sentinel daemon running in order to use
redis-py's Sentinel support.

.. autoclass:: redis.commands.sentinel.SentinelCommands
