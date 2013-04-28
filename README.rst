redis-py
========

The Python interface to the Redis key-value store.

.. image:: https://secure.travis-ci.org/andymccurdy/redis-py.png?branch=master
        :target: http://travis-ci.org/andymccurdy/redis-py

Installation
------------

redis-py requires a running Redis server. See `Redis's quickstart
<http://redis.io/topics/quickstart>`_ for installation instructions.

To install redis-py, simply:

.. code-block:: bash

    $ sudo pip install redis

or alternatively (you really should be using pip though):

.. code-block:: bash

    $ sudo easy_install redis

or from source:

.. code-block:: bash

    $ sudo python setup.py install


Getting Started
---------------

.. code-block:: pycon

    >>> import redis
    >>> r = redis.StrictRedis(host='localhost', port=6379, db=0)
    >>> r.set('foo', 'bar')
    True
    >>> r.get('foo')
    'bar'

API Reference
-------------

The `official Redis command documentation <http://redis.io/commands>`_ does a
great job of explaining each command in detail. redis-py exposes two client
classes that implement these commands. The StrictRedis class attempts to adhere
to the official command syntax. There are a few exceptions:

* **SELECT**: Not implemented. See the explanation in the Thread Safety section
  below.
* **DEL**: 'del' is a reserved keyword in the Python syntax. Therefore redis-py
  uses 'delete' instead.
* **CONFIG GET|SET**: These are implemented separately as config_get or config_set.
* **MULTI/EXEC**: These are implemented as part of the Pipeline class. The
  pipeline is wrapped with the MULTI and EXEC statements by default when it
  is executed, which can be disabled by specifying transaction=False.
  See more about Pipelines below.
* **SUBSCRIBE/LISTEN**: Similar to pipelines, PubSub is implemented as a separate
  class as it places the underlying connection in a state where it can't
  execute non-pubsub commands. Calling the pubsub method from the Redis client
  will return a PubSub instance where you can subscribe to channels and listen
  for messages. You can only call PUBLISH from the Redis client (see
  `this comment on issue #151
  <https://github.com/andymccurdy/redis-py/issues/151#issuecomment-1545015>`_
  for details).

In addition to the changes above, the Redis class, a subclass of StrictRedis,
overrides several other commands to provide backwards compatibility with older
versions of redis-py:

* **LREM**: Order of 'num' and 'value' arguments reversed such that 'num' can
  provide a default value of zero.
* **ZADD**: Redis specifies the 'score' argument before 'value'. These were swapped
  accidentally when being implemented and not discovered until after people
  were already using it. The Redis class expects \*args in the form of:
  `name1, score1, name2, score2, ...`
* **SETEX**: Order of 'time' and 'value' arguments reversed.


More Detail
-----------

Connection Pools
^^^^^^^^^^^^^^^^

Behind the scenes, redis-py uses a connection pool to manage connections to
a Redis server. By default, each Redis instance you create will in turn create
its own connection pool. You can override this behavior and use an existing
connection pool by passing an already created connection pool instance to the
connection_pool argument of the Redis class. You may choose to do this in order
to implement client side sharding or have finer grain control of how
connections are managed.

.. code-block:: pycon

    >>> pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    >>> r = redis.Redis(connection_pool=pool)

Connections
^^^^^^^^^^^

ConnectionPools manage a set of Connection instances. redis-py ships with two
types of Connections. The default, Connection, is a normal TCP socket based
connection. The UnixDomainSocketConnection allows for clients running on the
same device as the server to connect via a unix domain socket. To use a
UnixDomainSocketConnection connection, simply pass the unix_socket_path
argument, which is a string to the unix domain socket file. Additionally, make
sure the unixsocket parameter is defined in your redis.conf file. It's
commented out by default.

.. code-block:: pycon

    >>> r = redis.Redis(unix_socket_path='/tmp/redis.sock')

You can create your own Connection subclasses as well. This may be useful if
you want to control the socket behavior within an async framework. To
instantiate a client class using your own connection, you need to create
a connection pool, passing your class to the connection_class argument.
Other keyword parameters your pass to the pool will be passed to the class
specified during initialization.

.. code-block:: pycon

    >>> pool = redis.ConnectionPool(connection_class=YourConnectionClass,
                                    your_arg='...', ...)

Parsers
^^^^^^^

Parser classes provide a way to control how responses from the Redis server
are parsed. redis-py ships with two parser classes, the PythonParser and the
HiredisParser. By default, redis-py will attempt to use the HiredisParser if
you have the hiredis module installed and will fallback to the PythonParser
otherwise.

Hiredis is a C library maintained by the core Redis team. Pieter Noordhuis was
kind enough to create Python bindings. Using Hiredis can provide up to a
10x speed improvement in parsing responses from the Redis server. The
performance increase is most noticeable when retrieving many pieces of data,
such as from LRANGE or SMEMBERS operations.

Hiredis is available on PyPI, and can be installed via pip or easy_install
just like redis-py.

.. code-block:: bash

    $ pip install hiredis

or

.. code-block:: bash

    $ easy_install hiredis

Response Callbacks
^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^

Redis client instances can safely be shared between threads. Internally,
connection instances are only retrieved from the connection pool during
command execution, and returned to the pool directly after. Command execution
never modifies state on the client instance.

However, there is one caveat: the Redis SELECT command. The SELECT command
allows you to switch the database currently in use by the connection. That
database remains selected until another is selected or until the connection is
closed. This creates an issue in that connections could be returned to the pool
that are connected to a different database.

As a result, redis-py does not implement the SELECT command on client
instances. If you use multiple Redis databases within the same application, you
should create a separate client instance (and possibly a separate connection
pool) for each database.

It is not safe to pass PubSub or Pipeline objects between threads.

Pipelines
^^^^^^^^^

Pipelines are a subclass of the base Redis class that provide support for
buffering multiple commands to the server in a single request. They can be used
to dramatically increase the performance of groups of commands by reducing the
number of back-and-forth TCP packets between the client and server.

Pipelines are quite simple to use:

.. code-block:: pycon

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

.. code-block:: pycon

    >>> pipe.set('foo', 'bar').sadd('faz', 'baz').incr('auto_number').execute()
    [True, True, 6]

In addition, pipelines can also ensure the buffered commands are executed
atomically as a group. This happens by default. If you want to disable the
atomic nature of a pipeline but still want to buffer commands, you can turn
off transactions.

.. code-block:: pycon

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

.. code-block:: pycon

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

.. code-block:: pycon

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

.. code-block:: pycon

    >>> def client_side_incr(pipe):
    ...     current_value = pipe.get('OUR-SEQUENCE-KEY')
    ...     next_value = int(current_value) + 1
    ...     pipe.multi()
    ...     pipe.set('OUR-SEQUENCE-KEY', next_value)
    >>>
    >>> r.transaction(client_side_incr, 'OUR-SEQUENCE-KEY')
    [True]

LUA Scripting
^^^^^^^^^^^^^

redis-py supports the EVAL, EVALSHA, and SCRIPT commands. However, there are
a number of edge cases that make these commands tedious to use in real world
scenarios. Therefore, redis-py exposes a Script object that makes scripting
much easier to use.

To create a Script instance, use the `register_script` function on a client
instance passing the LUA code as the first argument. `register_script` returns
a Script instance that you can use throughout your code.

The following trivial LUA script accepts two parameters: the name of a key and
a multiplier value. The script fetches the value stored in the key, multiplies
it with the multiplier value and returns the result.

.. code-block:: pycon

    >>> r = redis.StrictRedis()
    >>> lua = """
    ... local value = redis.call('GET', KEYS[1])
    ... value = tonumber(value)
    ... return value * ARGV[1]"""
    >>> multiply = r.register_script(lua)

`multiply` is now a Script instance that is invoked by calling it like a
function. Script instances accept the following optional arguments:

* **keys**: A list of key names that the script will access. This becomes the
  KEYS list in LUA.
* **args**: A list of argument values. This becomes the ARGV list in LUA.
* **client**: A redis-py Client or Pipeline instance that will invoke the
  script. If client isn't specified, the client that intiially
  created the Script instance (the one that `register_script` was
  invoked from) will be used.

Continuing the example from above:

.. code-block:: pycon

    >>> r.set('foo', 2)
    >>> multiply(keys=['foo'], args=[5])
    10

The value of key 'foo' is set to 2. When multiply is invoked, the 'foo' key is
passed to the script along with the multiplier value of 5. LUA executes the
script and returns the result, 10.

Script instances can be executed using a different client instance, even one
that points to a completely different Redis server.

.. code-block:: pycon

    >>> r2 = redis.StrictRedis('redis2.example.com')
    >>> r2.set('foo', 3)
    >>> multiply(keys=['foo'], args=[5], client=r2)
    15

The Script object ensures that the LUA script is loaded into Redis's script
cache. In the event of a NOSCRIPT error, it will load the script and retry
executing it.

Script objects can also be used in pipelines. The pipeline instance should be
passed as the client argument when calling the script. Care is taken to ensure
that the script is registered in Redis's script cache just prior to pipeline
execution.

.. code-block:: pycon

    >>> pipe = r.pipeline()
    >>> pipe.set('foo', 5)
    >>> multiply(keys=['foo'], args=[5], client=pipe)
    >>> pipe.execute()
    [True, 25]

Author
^^^^^^

redis-py is developed and maintained by Andy McCurdy (sedrik@gmail.com).
It can be found here: http://github.com/andymccurdy/redis-py

Special thanks to:

* Ludovico Magnocavallo, author of the original Python Redis client, from
  which some of the socket code is still used.
* Alexander Solovyov for ideas on the generic response callback system.
* Paul Hubbard for initial packaging support.

