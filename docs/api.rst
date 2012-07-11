.. _api:

API Reference
=============

.. module:: redis

The official Redis documentation does a great job of explaining each command 
in `detail <http://redis.io/commands>`_. redis-py exposes two client classes 
that implement these commands. The StrictRedis class attempts to adhere to the 
official official command syntax.

There are a few exceptions:


- SELECT: Not implemented. See the explanation in the Thread Safety section
  below.
- DEL: 'del' is a reserved keyword in the Python syntax. Therefore redis-py
  uses 'delete' instead.
- CONFIG GET|SET: These are implemented separately as config_get or config_set.
- MULTI/EXEC: These are implemented as part of the Pipeline class. Calling
  the pipeline method and specifying use_transaction=True will cause the
  pipeline to be wrapped with the MULTI and EXEC statements when it is executed.
  See more about Pipelines below.
- SUBSCRIBE/LISTEN: Similar to pipelines, PubSub is implemented as a separate
  class as it places the underlying connection in a state where it can't
  execute non-pubsub commands. Calling the pubsub method from the Redis client
  will return a PubSub instance where you can subscribe to channels and listen
  for messages. You can call PUBLISH from both classes.

In addition to the changes above, the Redis class, a subclass of StrictRedis,
overrides several other commands to provide backwards compatibility with older
versions of redis-py:

- LREM: Order of 'num' and 'value' arguments reversed such that 'num' can
  provide a default value of zero.
- ZADD: Redis specifies the 'score' argument before 'value'. These were swapped
  accidentally when being implemented and not discovered until after people
  were already using it. The Redis class expects `*args` in the form of:
  name1, score1, name2, score2, ...
- SETEX: Order of 'time' and 'value' arguments reversed.


Classes
-------


.. autoclass:: redis.Redis
   :members:


.. autoclass:: redis.StrictRedis
   :members:


.. autoclass:: redis.Connection
   :members:

.. autoclass:: redis.ConnectionPool
   :members:

.. doctest::

   >>> pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
   >>> r = redis.Redis(connection_pool=pool)

Exceptions
----------

.. autoexception:: redis.AuthenticationError
.. autoexception:: redis.ConnectionError
.. autoexception:: redis.DataError
.. autoexception:: redis.InvalidResponse
.. autoexception:: redis.PubSubError
.. autoexception:: redis.RedisError
.. autoexception:: redis.ResponseError
.. autoexception:: redis.WatchError


Response Callbacks
------------------

The client class uses a set of callbacks to cast Redis responses to the
appropriate Python type. There are a number of these callbacks defined on
the Redis client class in a dictionary called RESPONSE_CALLBACKS.

Custom callbacks can be added on a per-instance basis using the
set_response_callback method. This method accepts two arguments: a command
name and the callback. Callbacks added in this manner are only valid on the
instance the callback is added to. If you want to define or override a callback
globally, you should make a subclass of the Redis client and add your callback
to its REDIS_CALLBACKS class dictionary.

Response callbacks take at least one parameter: the response from the Redis
server. Keyword arguments may also be accepted in order to further control
how to interpret the response. These keyword arguments are specified during the
command's call to execute_command. The ZRANGE implementation demonstrates the
use of response callback keyword arguments with its "withscores" argument.

Thread Safety
-------------

Redis client instances can safely be shared between threads. Internally,
connection instances are only retrieved from the connection pool during
command execution, and returned to the pool directly after. Command execution
never modifies state on the client instance.

However, there is one caveat: the Redis SELECT command. The SELECT command
allows you to switch the database currently in use by the connection. That
database remains selected until another is selected or until the connection is
closed. This creates an issue in that connections could be returned to the pool
that are connected to a different database.

As a result, redis-py does not implement the SELECT command on client instances.
If you use multiple Redis databases within the same application, you should
create a separate client instance (and possibly a separate connection pool) for
each database.

It is not safe to pass PubSub or Pipeline objects between threads.

Pipelines
----------

Pipelines are a subclass of the base Redis class that provide support for
buffering multiple commands to the server in a single request. They can be used
to dramatically increase the performance of groups of commands by reducing the
number of back-and-forth TCP packets between the client and server.

Pipelines are quite simple to use:

.. doctest::

    >>> r = redis.Redis(...)
    >>> r.set('bing', 'baz')
    >>> # Use the pipeline() method to create a pipeline instance
    >>> pipe = r.pipeline()
    >>> # The following SET commands are buffered
    >>> pipe.set('foo', 'bar')
    >>> pipe.get('bing')
    >>> # the EXECUTE call sends all buffered commands to the server, returning
    >>> # a list of responses, one for each command.
    >>> pipe.execute()
    [True, 'baz']

For ease of use, all commands being buffered into the pipeline return the
pipeline object itself. Therefore calls can be chained like:

.. doctest::

    >>> pipe.set('foo', 'bar').sadd('faz', 'baz').incr('auto_number').execute()
    [True, True, 6]

In addition, pipelines can also ensure the buffered commands are executed
atomically as a group. This happens by default. If you want to disable the
atomic nature of a pipeline but still want to buffer commands, you can turn
off transactions.

.. doctest::

    >>> pipe = r.pipeline(transaction=False)

A common issue occurs when requiring atomic transactions but needing to
retrieve values in Redis prior for use within the transaction. For instance,
let's assume that the INCR command didn't exist and we need to build an atomic
version of INCR in Python.

The completely naive implementation could GET the value, increment it in
Python, and SET the new value back. However, this is not atomic because
multiple clients could be doing this at the same time, each getting the same
value from GET.

Enter the WATCH command. WATCH provides the ability to monitor one or more keys
prior to starting a transaction. If any of those keys change prior the
execution of that transaction, the entire transaction will be canceled and a
WatchError will be raised. To implement our own client-side INCR command, we
could do something like this:

.. doctest::

    >>> with r.pipeline() as pipe:
    ...     while 1:
    ...         try:
    ...             # put a WATCH on the key that holds our sequence value
    ...             pipe.watch('OUR-SEQUENCE-KEY')
    ...             # after WATCHing, the pipeline is put into immediate execution
    ...             # mode until we tell it to start buffering commands again.
    ...             # this allows us to get the current value of our sequence
    ...             current_value = pipe.get('OUR-SEQUENCE-KEY')
    ...             next_value = int(current_value) + 1
    ...             # now we can put the pipeline back into buffered mode with MULTI
    ...             pipe.multi()
    ...             pipe.set('OUR-SEQUENCE-KEY', next_value)
    ...             # and finally, execute the pipeline (the set command)
    ...             pipe.execute()
    ...             # if a WatchError wasn't raised during execution, everything
    ...             # we just did happened atomically.
    ...             break
    ...        except WatchError:
    ...             # another client must have changed 'OUR-SEQUENCE-KEY' between
    ...             # the time we started WATCHing it and the pipeline's execution.
    ...             # our best bet is to just retry.
    ...             continue

Note that, because the Pipeline must bind to a single connection for the
duration of a WATCH, care must be taken to ensure that the connection is
returned to the connection pool by calling the reset() method. If the
Pipeline is used as a context manager (as in the example above) reset()
will be called automatically. Of course you can do this the manual way by
explicity calling reset():

.. doctest::

    >>> pipe = r.pipeline()
    >>> while 1:
    ...     try:
    ...         pipe.watch('OUR-SEQUENCE-KEY')
    ...         ...
    ...         pipe.execute()
    ...         break
    ...     except WatchError:
    ...         continue
    ...     finally:
    ...         pipe.reset()

A convenience method named "transaction" exists for handling all the
boilerplate of handling and retrying watch errors. It takes a callable that
should expect a single parameter, a pipeline object, and any number of keys to
be WATCHed. Our client-side INCR command above can be written like this,
which is much easier to read:

.. doctest::

    >>> def client_side_incr(pipe):
    ...     current_value = pipe.get('OUR-SEQUENCE-KEY')
    ...     next_value = int(current_value) + 1
    ...     pipe.multi()
    ...     pipe.set('OUR-SEQUENCE-KEY', next_value)
    >>>
    >>> r.transaction(client_side_incr, 'OUR-SEQUENCE-KEY')
    [True]
