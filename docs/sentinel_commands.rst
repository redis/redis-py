Redis Sentinel Commands
=======================

redis-py can be used together with `Redis
Sentinel <https://redis.io/topics/sentinel>`_ to discover Redis nodes. You
need to have at least one Sentinel daemon running in order to use
redis-py's Sentinel support.

Connection example (assumes redis redis on the ports listed below):

   >>> from redis import Sentinel
   >>> sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
   >>> sentinel.discover_master('mymaster')
   ('127.0.0.1', 6379)
   >>> sentinel.discover_slaves('mymaster')
   [('127.0.0.1', 6380)]


.. autoclass:: redis.commands.sentinel.SentinelCommands
   :members: