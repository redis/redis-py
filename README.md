# redis-py

The Python interface to the Redis key-value store.

## Installation

    $ sudo pip install redis

or alternatively (you really should be using pip though):

    $ sudo easy_install redis

From source:

    $ sudo python setup.py install


## Getting Started

    >>> import redis
    >>> r = redis.Redis(host='localhost', port=6379, db=0)
    >>> r.set('foo', 'bar')
    True
    >>> r.get('foo')
    'bar'

## More Detail

### Connection Pools

Behind the scenes, redis-py uses a connection pool to manage connections to
a Redis server. By default, each Redis instance you create will in turn create
its own connection pool. You can override this behavior and use an existing
connection pool by passing an already created connection pool instance to the
connection_pool argument of the Redis class. You may choose to do this in order
to implement client side sharding or have finer grain control of how connections
are managed.

    >>> pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    >>> r = redis.Redis(connection_pool=pool)

### Connetions

ConnectionPools manage a set of Connection instances. redis-py ships with two
types of Connections. The default, Connection, is a normal TCP socket based
connection. The UnixDomainSocketConnection allows for clients running on the
same device as the server to connect via a unix domain socket. To use a
UnixDomainSocketConnection connection, simply pass the class to the
connection_class argument of either the Redis or ConnectionPool class. You must
also specify the path argument, which is a string to the unix domain socket
file. Additionally, make sure the unixsocket parameter is defined in your
redis.conf file. It's commented out by default.

    >>> r = redis.Redis(connection_class=redis.UnixDomainSocketConnection,
    >>>                 path='/tmp/redis.sock')

You can create your own Connection subclasses in this way as well. This may be
useful if you want to control the socket behavior within an async framework.

### Parsers

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

Hiredis is available on Pypi, and can be installed via pip or easy_install
just like redis-py.

    $ pip install hiredis

or

    $ easy_install hiredis

### Response Callbacks

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

## Thread Safety

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

## API Reference

The official Redis documentation does a great job of explaining each command in
detail (http://redis.io/commands). In most cases, redis-py uses the same
arguments as the official spec. There are a few exceptions noted here:

* SELECT: Not implemented. See the explanation in the Thread Safety section
  above.
* ZADD: Redis specifies the 'score' argument before 'value'. These were swapped
  accidentally when being implemented and not discovered until after people
  were already using it. As of Redis 2.4, ZADD will start supporting variable
  arguments. redis-py implements these as python keyword arguments where the
  name is the 'value' and the value is the 'score'.
* DEL: 'del' is a reserved keyword in the Python syntax. Therefore redis-py
  uses 'delete' instead.
* CONFIG GET|SET: These are implemented separately as config_get or config_set.
* MULTI/EXEC: These are implemented as part of the Pipeline class. Calling
  the pipeline method and specifying use_transaction=True will cause the
  pipeline to be wrapped with the MULTI and EXEC statements when it is executed.
* SUBSCRIBE/LISTEN: Similar to pipelines, PubSub is implemented as a separate
  class as it places the underlying connection in a state where it can't
  execute non-pubsub commands. Calling the pubsub method from the Redis client
  will return a PubSub instance where you can subscribe to channels and listen
  for messages. You can call PUBLISH from both classes.

## Versioning scheme

redis-py is versioned after Redis. For example, redis-py 2.0.0 should
support all the commands available in Redis 2.0.0.

Author
------

redis-py is developed and maintained by Andy McCurdy (sedrik@gmail.com).
It can be found here: http://github.com/andymccurdy/redis-py

Special thanks to:

* Ludovico Magnocavallo, author of the original Python Redis client, from
  which some of the socket code is still used.
* Alexander Solovyov for ideas on the generic response callback system.
* Paul Hubbard for initial packaging support.

