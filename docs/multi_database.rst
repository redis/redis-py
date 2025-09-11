Multi-Database Management
=========================

MultiDBClient explanation
--------------------------

The `MultiDBClient` (introduced in version 6.5.0) manages connections to multiple
Redis databases and provides automatic failover when one database becomes unavailable.
Think of it as a smart load balancer that automatically switches to a healthy database
when your primary one goes down, ensuring your application stays online.
`MultiDBClient` in most of the cases provides the same API as any other client for
the best user experience.

The core feature of MultiDBClient is its ability to automatically trigger failover
when an active database becomes unhealthy.The pre-condition is that all databases
that are configured to be used by `MultiDBClient` are eventually consistent, so client
could choose any database in any point in time for communication. `MultiDBClient`
always communicates with single database, so there's 1 active and N passive
databases that are acting as a stand-by replica. By default, active database is
chosen based on the weights that have to be assigned for each database.

We have two mechanisms to verify database healthiness: `Healthcheck` and
`Failure Detector`.

To be able to use `MultiDBClient` you need to install a `pybreaker` package:

.. code:: python

    pip install pybreaker>=1.4.0

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


Health Monitoring
-----------------
The `MultiDBClient` uses two complementary mechanisms to ensure database availability:

Health Checks (Proactive Monitoring)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These checks run continuously in the background at configured intervals to proactively
detect database issues. They run in the background with a given interval and
configuration defined in the `MultiDBConfig` class.

By default, MultiDBClient sends ECHO commands to verify each database is healthy.

**Custom Health Checks**
~~~~~~~~~~~~~~~~~~~~~
You can add custom health checks for specific requirements:

.. code:: python

    from redis.multidb.healthcheck import AbstractHealthCheck
    from redis.retry import Retry
    from redis.utils import dummy_fail


    class PingHealthCheck(AbstractHealthCheck):
        def __init__(self, retry: Retry):
            super().__init__(retry=retry)

        def check_health(self, database) -> bool:
            return self._retry.call_with_retry(
                lambda: self._returns_pong(database),
                lambda _: dummy_fail()
            )

        def _returns_pong(self, database) -> bool:
            expected_message = ["PONG", b"PONG"]
            actual_message = database.client.execute_command("PING")
            return actual_message in expected_message

**Lag-Aware Healthcheck (Redis Enterprise Only)**
~~~~~~~~~~~~~~~~~~~~~

This is a special type of healthcheck available for Redis Software and Redis Cloud
that utilizes a REST API endpoint to obtain information about the synchronisation
lag between a given database and all other databases in an Active-Active setup.

To use this healthcheck, first you need to adjust your `DatabaseConfig`
to expose `health_check_url` used by your deployment. By default, your
Cluster FQDN should be used as URL, unless you have some kind of
reverse proxy behind an actual REST API endpoint.

.. code:: python

    database1_config = DatabaseConfig(
        weight=1.0,
        from_url="redis://host1:port1",
        health_check_url="https://c1.deployment-name-000000.project.env.com"
        client_kwargs={
            'username': "username",
            'password': "password",
        }
    )

Since, Lag-Aware Healthcheck is only available for Redis Software and Redis Cloud
it's not in the list of the default healthchecks for `MultiDBClient`. You have
to provide it manually during client configuration or in runtime.

.. code:: python

    // Configuration option
    config = MultiDbConfig(
        databases_config=[database1_config, database2_config],
        health_checks=[
          LagAwareHealthCheck(auth_basic=('username','password'), verify_tls=False)
      ]
    )

    client = MultiDBClient(config)

.. code:: python

    // In runtime
    client = MultiDBClient(config)
    client.add_health_check(
        LagAwareHealthCheck(auth_basic=('username','password'), verify_tls=False)
    )

As mentioned we utilise REST API endpoint for Lag-Aware healthchecks, so it accepts
different type of HTTP-related configuration: authentication credentials, request
timeout, TLS related configuration, etc. (check `LagAwareHealthCheck` class).

You can also specify `lag_aware_tolerance` parameter to specify the tolerance in MS
of lag between databases that your application could tolerate.

.. code:: python

    LagAwareHealthCheck(
        rest_api_port=9443,
        auth_basic=('username','password'),
        lag_aware_tolerance=150,
        verify_tls=True,
        ca_file="path/to/file"
    )


Failure Detection (Reactive Monitoring)
~~~~~~~~~~~~~~~~~~~~~

The failure detector watches actual command failures and marks databases as unhealthy
when error rates exceed thresholds within a sliding time window of a few seconds.
This catches issues that proactive health checks might miss during real traffic.
You can extend the list of failure detectors by providing your own implementation,
configuration defined in the `MultiDBConfig` class.


Databases configuration
-----------------------

Each database needs a `DatabaseConfig` that specifies how to connect.

Method 1: Using client_kwargs (most flexible)
~~~~~~~~~~~~~~~~~~~~~

There's an underlying instance of `Redis` or `RedisCluster` client for each database,
so you can pass all the arguments related to them via `client_kwargs` argument:

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

Method 2: Using Redis URL
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    database_config1 = DatabaseConfig(
        weight=1.0,
        from_url="redis://host1:port1",
        client_kwargs={
            'username': "username",
            'password': "password",
        }
    )

Method 3: Using Custom Connection Pool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

  database_config2 = DatabaseConfig(
      weight=0.9,
      from_pool=connection_pool,
  )

**Important**: Don't pass `Retry` objects in `client_kwargs`. `MultiDBClient`
handles all retries at the top level through the `command_retry` configuration.


Pipeline Operations
-------------------

The `MultiDBClient` supports pipeline mode with guaranteed retry functionality during
failover scenarios. Unlike standard `Redis` and `RedisCluster` clients, transactions
cannot be executed through pipeline mode - use the dedicated `transaction()` method
instead. This design choice ensures better retry handling during failover events.

Pipeline operations support both chaining calls and context manager patterns:

Chaining approach
~~~~~~~~~~~~~~~~~

.. code:: python

    client = MultiDBClient(config)
    pipe = client.pipeline()
    pipe.set('key1', 'value1')
    pipe.get('key1')
    pipe.execute() // ['OK', 'value1']

Context Manager Approach
~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    client = MultiDBClient(config)
    with client.pipeline() as pipe:
        pipe.set('key1', 'value1')
        pipe.get('key1')
        pipe.execute() // ['OK', 'value1']


Transaction
-----------

The `MultiDBClient` provides transaction support through the `transaction()`
method with guaranteed retry capabilities during failover. Like other
`Redis` clients, it accepts a callback function that receives a `Pipeline`
object for building atomic operations.

CAS behavior is fully supported by providing a list of
keys to monitor:

.. code:: python

    client = MultiDBClient(config)

    def callback(pipe: Pipeline):
        pipe.set('key1', 'value1')
        pipe.get('key1')

    client.transaction(callback, 'key1') // ['OK1', 'value1']


Pub/Sub
-------

The MultiDBClient offers Pub/Sub functionality with automatic re-subscription
to channels during failover events. For optimal failover handling,
both publishers and subscribers should use MultiDBClient instances.

1. **Subscriber failover**: Automatically reconnects to an alternative database
and re-subscribes to the same channels

2. **Publisher failover**: Seamlessly switches to an alternative database and
continues publishing to the same channels

**Note**: Message loss may occur if failover events happen in reverse order
(publisher fails before subscriber).

Main Thread Message Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    client = MultiDBClient(config)
    p = client.pubsub()

    // In the main thread
    while True:
        message = p.get_message()
            if message:
                // do something with the message
        time.sleep(0.001)


Background Thread Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    p.subscribe(**{'test-channel': handler})
    pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True)

    for _ in range(10):
        client.publish('test-channel', data)
        sleep(0.1)


OSS Cluster API support
-----------------------

As mentioned `MultiDBClient` also supports integration with OSS Cluster API
databases. If you're instantiating client using Redis URL, the only change
you need comparing to standalone client is the `client_class` argument.
DNS server will resolve given URL and will point you to one of the nodes that
could be used to discover overall cluster topology.

.. code:: python

    config = MultiDbConfig(
        client_class=RedisCluster,
        databases_config=[database1_config, database2_config],
    )

If you would like to specify the exact node to use for topology
discovery, you can specify it the same way `RedisCluster` does

.. code:: python

    // Expected active database (highest weight)
    database1_config = DatabaseConfig(
        weight=1.0,
        client_kwargs={
            'username': "username",
            'password': "password",
            'startup_nodes': [ClusterNode('host1', 'port1')],
        }
    )

    // Passive database (stand-by replica)
    database2_config = DatabaseConfig(
        weight=0.9,
        client_kwargs={
            'username': "username",
            'password': "password",
            'startup_nodes': [ClusterNode('host2', 'port2')],
        }
    )

    config = MultiDbConfig(
        client_class=RedisCluster,
        databases_config=[database1_config, database2_config],
    )

Sharded Pub/Sub
~~~~~~~~~~~~~~~

If you would like to use a Sharded Pub/Sub capabilities make sure to use
correct Pub/Sub configuration.

.. code:: python

    client = MultiDBClient(config)
    p = client.pubsub()

    // In the main thread
    while True:
        // Reads messaage from sharded channels.
        message = p.get_sharded_message()
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

    // Proactively executes get_sharded_pubsub() method
    pubsub_thread = pubsub.run_in_thread(sleep_time=0.1, daemon=True, sharded_pubsub=True)

    for _ in range(10):
        client.spublish('test-channel', data)
        sleep(0.1)