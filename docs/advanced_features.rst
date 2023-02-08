Advanced Features
=================

A note about threading
----------------------

Redis client instances can safely be shared between threads. Internally,
connection instances are only retrieved from the connection pool during
command execution, and returned to the pool directly after. Command
execution never modifies state on the client instance.

However, there is one caveat: the Redis SELECT command. The SELECT
command allows you to switch the database currently in use by the
connection. That database remains selected until another is selected or
until the connection is closed. This creates an issue in that
connections could be returned to the pool that are connected to a
different database.

As a result, redis-py does not implement the SELECT command on client
instances. If you use multiple Redis databases within the same
application, you should create a separate client instance (and possibly
a separate connection pool) for each database.

It is not safe to pass PubSub or Pipeline objects between threads.

Pipelines
---------

Default pipelines
~~~~~~~~~~~~~~~~~

Pipelines are a subclass of the base Redis class that provide support
for buffering multiple commands to the server in a single request. They
can be used to dramatically increase the performance of groups of
commands by reducing the number of back-and-forth TCP packets between
the client and server.

Pipelines are quite simple to use:

.. code:: pycon

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
   [True, b'baz']

For ease of use, all commands being buffered into the pipeline return
the pipeline object itself. Therefore calls can be chained like:

.. code:: pycon

   >>> pipe.set('foo', 'bar').sadd('faz', 'baz').incr('auto_number').execute()
   [True, True, 6]

In addition, pipelines can also ensure the buffered commands are
executed atomically as a group. This happens by default. If you want to
disable the atomic nature of a pipeline but still want to buffer
commands, you can turn off transactions.

.. code:: pycon

   >>> pipe = r.pipeline(transaction=False)

A common issue occurs when requiring atomic transactions but needing to
retrieve values in Redis prior for use within the transaction. For
instance, let's assume that the INCR command didn't exist and we need to
build an atomic version of INCR in Python.

The completely naive implementation could GET the value, increment it in
Python, and SET the new value back. However, this is not atomic because
multiple clients could be doing this at the same time, each getting the
same value from GET.

Enter the WATCH command. WATCH provides the ability to monitor one or
more keys prior to starting a transaction. If any of those keys change
prior the execution of that transaction, the entire transaction will be
canceled and a WatchError will be raised. To implement our own
client-side INCR command, we could do something like this:

.. code:: pycon

   >>> with r.pipeline() as pipe:
   ...     while True:
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
will be called automatically. Of course you can do this the manual way
by explicitly calling reset():

.. code:: pycon

   >>> pipe = r.pipeline()
   >>> while True:
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
boilerplate of handling and retrying watch errors. It takes a callable
that should expect a single parameter, a pipeline object, and any number
of keys to be WATCHed. Our client-side INCR command above can be written
like this, which is much easier to read:

.. code:: pycon

   >>> def client_side_incr(pipe):
   ...     current_value = pipe.get('OUR-SEQUENCE-KEY')
   ...     next_value = int(current_value) + 1
   ...     pipe.multi()
   ...     pipe.set('OUR-SEQUENCE-KEY', next_value)
   >>>
   >>> r.transaction(client_side_incr, 'OUR-SEQUENCE-KEY')
   [True]

Be sure to call pipe.multi() in the callable passed to Redis.transaction
prior to any write commands.

Pipelines in clusters
~~~~~~~~~~~~~~~~~~~~~

ClusterPipeline is a subclass of RedisCluster that provides support for
Redis pipelines in cluster mode. When calling the execute() command, all
the commands are grouped by the node on which they will be executed, and
are then executed by the respective nodes in parallel. The pipeline
instance will wait for all the nodes to respond before returning the
result to the caller. Command responses are returned as a list sorted in
the same order in which they were sent. Pipelines can be used to
dramatically increase the throughput of Redis Cluster by significantly
reducing the number of network round trips between the client and
the server.

.. code:: pycon

   >>> with rc.pipeline() as pipe:
   ...     pipe.set('foo', 'value1')
   ...     pipe.set('bar', 'value2')
   ...     pipe.get('foo')
   ...     pipe.get('bar')
   ...     print(pipe.execute())
   [True, True, b'value1', b'value2']
   ...     pipe.set('foo1', 'bar1').get('foo1').execute()
   [True, b'bar1']

Please note: - RedisCluster pipelines currently only support key-based
commands. - The pipeline gets its ‘read_from_replicas’ value from the
cluster’s parameter. Thus, if read from replications is enabled in the
cluster instance, the pipeline will also direct read commands to
replicas. - The ‘transaction’ option is NOT supported in cluster-mode.
In non-cluster mode, the ‘transaction’ option is available when
executing pipelines. This wraps the pipeline commands with MULTI/EXEC
commands, and effectively turns the pipeline commands into a single
transaction block. This means that all commands are executed
sequentially without any interruptions from other clients. However, in
cluster-mode this is not possible, because commands are partitioned
according to their respective destination nodes. This means that we can
not turn the pipeline commands into one transaction block, because in
most cases they are split up into several smaller pipelines.

Publish / Subscribe
-------------------

redis-py includes a PubSub object that subscribes to channels and
listens for new messages. Creating a PubSub object is easy.

.. code:: pycon

   >>> r = redis.Redis(...)
   >>> p = r.pubsub()

Once a PubSub instance is created, channels and patterns can be
subscribed to.

.. code:: pycon

   >>> p.subscribe('my-first-channel', 'my-second-channel', ...)
   >>> p.psubscribe('my-*', ...)

The PubSub instance is now subscribed to those channels/patterns. The
subscription confirmations can be seen by reading messages from the
PubSub instance.

.. code:: pycon

   >>> p.get_message()
   {'pattern': None, 'type': 'subscribe', 'channel': b'my-second-channel', 'data': 1}
   >>> p.get_message()
   {'pattern': None, 'type': 'subscribe', 'channel': b'my-first-channel', 'data': 2}
   >>> p.get_message()
   {'pattern': None, 'type': 'psubscribe', 'channel': b'my-*', 'data': 3}

Every message read from a PubSub instance will be a dictionary with the
following keys.

-  **type**: One of the following: 'subscribe', 'unsubscribe',
   'psubscribe', 'punsubscribe', 'message', 'pmessage'
-  **channel**: The channel [un]subscribed to or the channel a message
   was published to
-  **pattern**: The pattern that matched a published message's channel.
   Will be None in all cases except for 'pmessage' types.
-  **data**: The message data. With [un]subscribe messages, this value
   will be the number of channels and patterns the connection is
   currently subscribed to. With [p]message messages, this value will be
   the actual published message.

Let's send a message now.

.. code:: pycon

   # the publish method returns the number matching channel and pattern
   # subscriptions. 'my-first-channel' matches both the 'my-first-channel'
   # subscription and the 'my-*' pattern subscription, so this message will
   # be delivered to 2 channels/patterns
   >>> r.publish('my-first-channel', 'some data')
   2
   >>> p.get_message()
   {'channel': b'my-first-channel', 'data': b'some data', 'pattern': None, 'type': 'message'}
   >>> p.get_message()
   {'channel': b'my-first-channel', 'data': b'some data', 'pattern': b'my-*', 'type': 'pmessage'}

Unsubscribing works just like subscribing. If no arguments are passed to
[p]unsubscribe, all channels or patterns will be unsubscribed from.

.. code:: pycon

   >>> p.unsubscribe()
   >>> p.punsubscribe('my-*')
   >>> p.get_message()
   {'channel': b'my-second-channel', 'data': 2, 'pattern': None, 'type': 'unsubscribe'}
   >>> p.get_message()
   {'channel': b'my-first-channel', 'data': 1, 'pattern': None, 'type': 'unsubscribe'}
   >>> p.get_message()
   {'channel': b'my-*', 'data': 0, 'pattern': None, 'type': 'punsubscribe'}

redis-py also allows you to register callback functions to handle
published messages. Message handlers take a single argument, the
message, which is a dictionary just like the examples above. To
subscribe to a channel or pattern with a message handler, pass the
channel or pattern name as a keyword argument with its value being the
callback function.

When a message is read on a channel or pattern with a message handler,
the message dictionary is created and passed to the message handler. In
this case, a None value is returned from get_message() since the message
was already handled.

.. code:: pycon

   >>> def my_handler(message):
   ...     print('MY HANDLER: ', message['data'])
   >>> p.subscribe(**{'my-channel': my_handler})
   # read the subscribe confirmation message
   >>> p.get_message()
   {'pattern': None, 'type': 'subscribe', 'channel': b'my-channel', 'data': 1}
   >>> r.publish('my-channel', 'awesome data')
   1
   # for the message handler to work, we need tell the instance to read data.
   # this can be done in several ways (read more below). we'll just use
   # the familiar get_message() function for now
   >>> message = p.get_message()
   MY HANDLER:  awesome data
   # note here that the my_handler callback printed the string above.
   # `message` is None because the message was handled by our handler.
   >>> print(message)
   None

If your application is not interested in the (sometimes noisy)
subscribe/unsubscribe confirmation messages, you can ignore them by
passing ignore_subscribe_messages=True to r.pubsub(). This will cause
all subscribe/unsubscribe messages to be read, but they won't bubble up
to your application.

.. code:: pycon

   >>> p = r.pubsub(ignore_subscribe_messages=True)
   >>> p.subscribe('my-channel')
   >>> p.get_message()  # hides the subscribe message and returns None
   >>> r.publish('my-channel', 'my data')
   1
   >>> p.get_message()
   {'channel': b'my-channel', 'data': b'my data', 'pattern': None, 'type': 'message'}

There are three different strategies for reading messages.

The examples above have been using pubsub.get_message(). Behind the
scenes, get_message() uses the system's 'select' module to quickly poll
the connection's socket. If there's data available to be read,
get_message() will read it, format the message and return it or pass it
to a message handler. If there's no data to be read, get_message() will
immediately return None. This makes it trivial to integrate into an
existing event loop inside your application.

.. code:: pycon

   >>> while True:
   >>>     message = p.get_message()
   >>>     if message:
   >>>         # do something with the message
   >>>     time.sleep(0.001)  # be nice to the system :)

Older versions of redis-py only read messages with pubsub.listen().
listen() is a generator that blocks until a message is available. If
your application doesn't need to do anything else but receive and act on
messages received from redis, listen() is an easy way to get up an
running.

.. code:: pycon

   >>> for message in p.listen():
   ...     # do something with the message

The third option runs an event loop in a separate thread.
pubsub.run_in_thread() creates a new thread and starts the event loop.
The thread object is returned to the caller of [un_in_thread(). The
caller can use the thread.stop() method to shut down the event loop and
thread. Behind the scenes, this is simply a wrapper around get_message()
that runs in a separate thread, essentially creating a tiny non-blocking
event loop for you. run_in_thread() takes an optional sleep_time
argument. If specified, the event loop will call time.sleep() with the
value in each iteration of the loop.

Note: Since we're running in a separate thread, there's no way to handle
messages that aren't automatically handled with registered message
handlers. Therefore, redis-py prevents you from calling run_in_thread()
if you're subscribed to patterns or channels that don't have message
handlers attached.

.. code:: pycon

   >>> p.subscribe(**{'my-channel': my_handler})
   >>> thread = p.run_in_thread(sleep_time=0.001)
   # the event loop is now running in the background processing messages
   # when it's time to shut it down...
   >>> thread.stop()

run_in_thread also supports an optional exception handler, which lets
you catch exceptions that occur within the worker thread and handle them
appropriately. The exception handler will take as arguments the
exception itself, the pubsub object, and the worker thread returned by
run_in_thread.

.. code:: pycon

   >>> p.subscribe(**{'my-channel': my_handler})
   >>> def exception_handler(ex, pubsub, thread):
   >>>     print(ex)
   >>>     thread.stop()
   >>>     thread.join(timeout=1.0)
   >>>     pubsub.close()
   >>> thread = p.run_in_thread(exception_handler=exception_handler)

A PubSub object adheres to the same encoding semantics as the client
instance it was created from. Any channel or pattern that's unicode will
be encoded using the charset specified on the client before being sent
to Redis. If the client's decode_responses flag is set the False (the
default), the 'channel', 'pattern' and 'data' values in message
dictionaries will be byte strings (str on Python 2, bytes on Python 3).
If the client's decode_responses is True, then the 'channel', 'pattern'
and 'data' values will be automatically decoded to unicode strings using
the client's charset.

PubSub objects remember what channels and patterns they are subscribed
to. In the event of a disconnection such as a network error or timeout,
the PubSub object will re-subscribe to all prior channels and patterns
when reconnecting. Messages that were published while the client was
disconnected cannot be delivered. When you're finished with a PubSub
object, call its .close() method to shutdown the connection.

.. code:: pycon

   >>> p = r.pubsub()
   >>> ...
   >>> p.close()

The PUBSUB set of subcommands CHANNELS, NUMSUB and NUMPAT are also
supported:

.. code:: pycon

   >>> r.pubsub_channels()
   [b'foo', b'bar']
   >>> r.pubsub_numsub('foo', 'bar')
   [(b'foo', 9001), (b'bar', 42)]
   >>> r.pubsub_numsub('baz')
   [(b'baz', 0)]
   >>> r.pubsub_numpat()
   1204

Monitor
~~~~~~~

redis-py includes a Monitor object that streams every command processed
by the Redis server. Use listen() on the Monitor object to block until a
command is received.

.. code:: pycon

   >>> r = redis.Redis(...)
   >>> with r.monitor() as m:
   >>>     for command in m.listen():
   >>>         print(command)
