Active-Active
=============

MultiDBClient explanation
--------------------------

Starting from redis-py 6.5.0 we introduce a new type of client to communicate
with databases in Active-Active setup. `MultiDBClient` is a wrapper around multiple
Redis or Redis Cluster clients, each of them has 1:1 relation to specific
database. `MultiDBClient` in most of the cases provides the same API as any other
client for the best user experience.

The core feature of `MultiDBClient` is automaticaly triggered failover depends on the
database healthiness. The pre-condition is that each database that is configured
to be used by MultiDBClient are eventually consistent, so client could choose
any database in any point of time for communication. `MultiDBClient` always communicates
with single database, so there's 1 active and N passive databases that acting as a
stand-by replica. By default, active database is choosed based on the weights that
has to be assigned for each database.

We have two mechanisms to verify database healthiness: `Healthcheck` and
`Failure Detector`.

The very basic configuration you need to setup a `MultiDBClient`:

.. code:: python

    // Expected active database (highest weight)
    database1_config = DatabaseConfig(
        weight=1.0,
        from_url="redis://host1:port1",
        client_kwargs={
            'username': "username",
            'password': "password",
        }
    )

    // Passive database (stand-by replica)
    database2_config = DatabaseConfig(
        weight=0.9,
        from_url="redis://host2:port2",
        client_kwargs={
            'username': "username",
            'password': "password",
        }
    )

    config = MultiDbConfig(
        databases_config=[database1_config, database2_config],
    )

    client = MultiDBClient(config)


Healthcheck
-----------

By default, we're using healthcheck based on `ECHO` command to verify that database is
reachable and ready to serve requests (`PING` guarantees first, but not the second).
Additionaly, you can add your own healthcheck implementation and extend a list of
healthecks

All healthchecks are running in the background with given interval and configuration
defined in `MultiDBConfig` class.


Failure Detector
----------------

Unlike healthcheck, `Failure Detector` verifies database healthiness based on organic
trafic, so the default one reacts to any command failures within a sliding window of
seconds and mark database as unhealthy if threshold has been exceeded. You can extend
a list of failure detectors providing your own implementation, configuration defined
in `MultiDBConfig` class.


Databases configuration
-----------------------

You have to provide a configuration for each database in setup separately, using
`DatabaseConfig` class per database. As mentioned, there's an undelying instance
of `Redis` or `RedisCluster` client for each database, so you can pass all the
arguments related to them via `client_kwargs` argument.

.. code:: python

    database_config = DatabaseConfig(
        weight=1.0,
        client_kwargs={
            'host': 'localhost',
            'port': 6379,
            'username': "username",
            'password': "password",
        }
    )

It also supports `from_url` or `from_pool` capabilites to setup a client using
Redis URL or custom `ConnectionPool` object.

.. code:: python

    database_config1 = DatabaseConfig(
        weight=1.0,
        from_url="redis://host1:port1",
        client_kwargs={
            'username': "username",
            'password': "password",
        }
    )

    database_config2 = DatabaseConfig(
        weight=0.9,
        from_pool=connection_pool,
    )

The only exception from `client_kwargs` is the retry configuration. We do not allow
to pass underlying `Retry` object to avoid nesting retries. All the retries are
controlled by top-level `Retry` object that you can setup via `command_retry`
argument (check `MultiDBConfig`)


Pipeline
--------

`MultiDBClient` supports pipeline mode with guaranteed pipeline retry in case
of failover. Unlike, the `Redis` and `RedisCluster` clients you cannot
execute transactions via pipeline mode, only via `transaction` method
on `MultiDBClient`. This was done for better retries handling in case
of failover.

The overall interface for pipeline execution is the same, you can
pipeline commands using chaining calls or context manager.

.. code:: python

    // Chaining
    client = MultiDBClient(config)
    pipe = client.pipeline()
    pipe.set('key1', 'value1')
    pipe.get('key1')
    pipe.execute() // ['OK', 'value1']

    // Context manager
    client = MultiDBClient(config)
    with client.pipeline() as pipe:
        pipe.set('key1', 'value1')
        pipe.get('key1')
        pipe.execute() // ['OK', 'value1']


Transaction
-----------

`MultiDBClient` supports transaction execution via `transaction()` method
with guaranteed transaction retry in case of failover. Like any other
client it accepts a callback with underlying `Pipeline` object to build
your transaction for atomic execution

CAS behaviour supported as well, so you can provide a list of keys to track.

.. code:: python

    client = MultiDBClient(config)

    def callback(pipe: Pipeline):
        pipe.set('key1', 'value1')
        pipe.get('key1')

    client.transaction(callback, 'key1') // ['OK1', 'value1']


Pub/Sub
-------

`MultiDBClient` supports Pub/Sub mode with guaranteed re-subscription
to the same channels in case of failover. So the expectation is that
both publisher and subscriber are using `MultiDBClient` instance to
provide seamless experience in terms of failover.

1. Subscriber failover to another database and re-subscribe to the same
channels.

2. Publisher failover to another database and starts publishing
messages to the same channels.

However, it's still possible to lose messages if order of failover
will be reversed.

Like the other clients, there's two main methods to consume messages:
in the main thread and in the separate thread

.. code:: python

    client = MultiDBClient(config)
    p = client.pubsub()

    // In the main thread
    while True:
        message = p.get_message()
            if message:
                // do something with the message
        time.sleep(0.001)


.. code:: python

    // In separate thread
    client = MultiDBClient(config)
    p = client.pubsub()
    messages_count = 0
    data = json.dumps({'message': 'test'})

    def handler(message):
        nonlocal messages_count
        messages_count += 1

    // Assign a handler and run in a separate thread.
    p.ssubscribe(**{'test-channel': handler})
    pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True)

    for _ in range(10):
        client.publish('test-channel', data)
        sleep(0.1)
