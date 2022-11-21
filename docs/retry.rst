Retry Helpers
#############

.. automodule:: redis.retry
    :members:


Retry in Redis Standalone
**************************

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.client import Redis
>>> from redis.exceptions import (
>>>    BusyLoadingError,
>>>    ConnectionError,
>>>    TimeoutError
>>> )
>>>
>>> # Run 3 retries with exponential backoff strategy
>>> retry = Retry(ExponentialBackoff(), 3)
>>> # Redis client with retries on custom errors
>>> r = Redis(host='localhost', port=6379, retry=retry, retry_on_error=[BusyLoadingError, ConnectionError, TimeoutError])
>>> # Redis client with retries on TimeoutError only
>>> r_only_timeout = Redis(host='localhost', port=6379, retry=retry, retry_on_timeout=True)

As you can see from the example above, Redis client supports 3 parameters to configure the retry behaviour:

* ``retry``: :class:`~.Retry` instance with a :ref:`backoff-label` strategy and the max number of retries
* ``retry_on_error``: list of :ref:`exceptions-label` to retry on
* ``retry_on_timeout``: if ``True``, retry on :class:`~.TimeoutError` only

If either ``retry_on_error`` or ``retry_on_timeout`` are passed and no ``retry`` is given,
by default it uses a ``Retry(NoBackoff(), 1)`` (meaning 1 retry right after the first failure).


Retry in Redis Cluster
**************************

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.cluster import RedisCluster
>>>
>>> # Run 3 retries with exponential backoff strategy
>>> retry = Retry(ExponentialBackoff(), 3)
>>> # Redis Cluster client with retries
>>> rc = RedisCluster(host='localhost', port=6379, retry=retry, cluster_error_retry_attempts=2)

Retry behaviour in Redis Cluster is a little bit different from Standalone:

* ``retry``: :class:`~.Retry` instance with a :ref:`backoff-label` strategy and the max number of retries, default value is ``Retry(NoBackoff(), 0)``
* ``cluster_error_retry_attempts``: number of times to retry before raising an error when :class:`~.TimeoutError` or :class:`~.ConnectionError` or :class:`~.ClusterDownError` are encountered, default value is ``3``

Let's consider the following example:

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.cluster import RedisCluster
>>>
>>> rc = RedisCluster(host='localhost', port=6379, retry=Retry(ExponentialBackoff(), 6), cluster_error_retry_attempts=1)
>>> rc.set('foo', 'bar')

#. the client library calculates the hash slot for key 'foo'.
#. given the hash slot, it then determines which node to connect to, in order to execute the command.
#. during the connection, a :class:`~.ConnectionError` is raised.
#. because we set ``retry=Retry(ExponentialBackoff(), 6)``, the client tries to reconnect to the node up to 6 times, with an exponential backoff between each attempt.
#. even after 6 retries, the client is still unable to connect.
#. because we set ``cluster_error_retry_attempts=1``, before giving up, the client starts a cluster update, removes the failed node from the startup nodes, and re-initializes the cluster.
#. after the cluster has been re-initialized, it starts a new cycle of retries, up to 6 retries, with an exponential backoff.
#. if the client can connect, we're good. Otherwise, the exception is finally raised to the caller, because we've run out of attempts.