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
>>>    RedisError,
>>> )
>>>
>>> # Run 3 retries with exponential backoff strategy
>>> retry = Retry(ExponentialBackoff(), 3)
>>> # Redis client with retries on custom errors in addition to the errors
>>> # that are already retried by default
>>> r = Redis(host='localhost', port=6379, retry=retry, retry_on_error=[BusyLoadingError, RedisError])

As you can see from the example above, Redis client supports 2 parameters to configure the retry behaviour:

* ``retry``: :class:`~.Retry` instance with a :ref:`backoff-label` strategy and the max number of retries
    * The :class:`~.Retry` instance has default set of :ref:`exceptions-label` to retry on,
      which can be overridden by passing a tuple with :ref:`exceptions-label` to the ``supported_errors`` parameter.
* ``retry_on_error``: list of additional :ref:`exceptions-label` to retry on


If no ``retry`` is provided, a default one is created with  :class:`~.ExponentialWithJitterBackoff` as backoff strategy
and 3 retries.


Retry in Redis Cluster
**************************

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.cluster import RedisCluster
>>>
>>> # Run 3 retries with exponential backoff strategy
>>> retry = Retry(ExponentialBackoff(), 3)
>>> # Redis Cluster client with retries
>>> rc = RedisCluster(host='localhost', port=6379, retry=retry)

Retry behaviour in Redis Cluster is a little bit different from Standalone:

* ``retry``: :class:`~.Retry` instance with a :ref:`backoff-label` strategy and the max number of retries, default value is ``Retry(ExponentialWithJitterBackoff(base=1, cap=10), cluster_error_retry_attempts)``
* ``cluster_error_retry_attempts``: number of times to retry before raising an error when :class:`~.TimeoutError` or :class:`~.ConnectionError` or :class:`~.ClusterDownError` or :class:`~.SlotNotCoveredError` are encountered, default value is ``3``
    * This argument is deprecated - it is used to initialize the number of retries for the retry object,
      only in the case when the ``retry`` object is not provided.
      When the ``retry`` argument is provided, the ``cluster_error_retry_attempts`` argument is ignored!

* Starting from version 6.0.0 of the library, the default retry policy for the nodes connections is without retries.
  This means that if a connection to a node fails, the lower level connection will not retry the connection.
  Instead, it will raise a :class:`~.ConnectionError` to the cluster level call., where it will be retried.
  This is done to avoid blocking the cluster client for too long in case of a node failure.

* The retry object is not yet fully utilized in the cluster client.
  The retry object is used only to determine the number of retries for the cluster level calls.

Let's consider the following example:

>>> from redis.backoff import ExponentialBackoff
>>> from redis.retry import Retry
>>> from redis.cluster import RedisCluster
>>>
>>> rc = RedisCluster(host='localhost', port=6379, retry=Retry(ExponentialBackoff(), 6))
>>> rc.set('foo', 'bar')

#. the client library calculates the hash slot for key 'foo'.
#. given the hash slot, it then determines which node to connect to, in order to execute the command.
#. during the connection, a :class:`~.ConnectionError` is raised.
#. because the default retry policy for the nodes connections is without retries, the error is raised to the cluster level call
#. because we set ``retry=Retry(ExponentialBackoff(), 6)``, the cluster client starts a cluster update, removes the failed node from the startup nodes, and re-initializes the cluster.
#. the cluster client retries the command until it either succeeds or the max number of retries is reached.