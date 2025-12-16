Multi-database client (Active-Active)
=====================================

The multi-database client allows your application to connect to multiple Redis databases, which are typically replicas of each other.
It is designed to work with Redis Software and Redis Cloud Active-Active setups.
The client continuously monitors database health, detects failures, and automatically fails over to the next healthy database using a configurable strategy.
When the original database becomes healthy again, the client can automatically switch back to it.

Key concepts
------------

- Database and weight:
  Each database has a weight indicating its priority. The failover strategy chooses the highest-weight
  healthy database as the active one.

- Circuit breaker:
  Each database is guarded by a circuit breaker with states CLOSED (healthy), OPEN (unhealthy),
  and HALF_OPEN (probing). Health checks toggle these states to avoid hammering a downed database.

- Health checks:
  A set of checks determines whether a database is healthy in proactive manner.
  By default, an "PING" check runs against the database (all cluster nodes must
  pass for a cluster). You can provide your own set of health checks or add an
  additional health check on top of the default one. A Redis Enterprise specific
  "lag-aware" health check is also available.

- Failure detector:
  A detector observes command failures over a moving window (reactive monitoring).
  You can specify an exact number of failures and failures rate to have more
  fine-grain tuned configuration of triggering fail over based on organic traffic.
  You can provide your own set of custom failure detectors or add an additional
  detector on top of the default one.

- Failover strategy:
  The default strategy is based on statically configured weights. It prefers the highest weighted healthy database.

- Command retry:
  Command execution supports retry with backoff. Low-level client retries are disabled and a global retry
  setting is applied at the multi-database layer.

- Auto fallback:
  If configured with a positive interval, the client periodically attempts to fall back to a higher-weighted
  healthy database.

- Events:
  The client emits events like "active database changed" and "commands failed". In addition it resubscribes to Pub/Sub channels automatically.

Synchronous usage
-----------------

Minimal example
^^^^^^^^^^^^^^^

.. code-block:: python

    from redis.multidb.client import MultiDBClient
    from redis.multidb.config import MultiDbConfig, DatabaseConfig

    # Two databases. The first has higher weight -> preferred when healthy.
    cfg = MultiDbConfig(
        databases_config=[
            DatabaseConfig(from_url="redis://db-primary:6379/0", weight=1.0),
            DatabaseConfig(from_url="redis://db-secondary:6379/0", weight=0.5),
        ]
    )

    client = MultiDBClient(cfg)

    # First call triggers initialization and health checks.
    client.set("key", "value")
    print(client.get("key"))

    # Pipeline
    with client.pipeline() as pipe:
        pipe.set("a", 1)
        pipe.incrby("a", 2)
        values = pipe.execute()
        print(values)

    # Transaction
    def txn(pipe):
        current = pipe.get("balance")
        current = int(current or 0)
        pipe.multi()  # mark transaction
        pipe.set("balance", current + 100)

    client.transaction(txn)

    # Pub/Sub usage - will automatically re-subscribe on database switch
    pubsub = client.pubsub()
    pubsub.subscribe("events")

    # In your loop:
    message = pubsub.get_message(timeout=1.0)
    if message:
        print(message)

Asyncio usage
-------------

The asyncio API mirrors the synchronous one and provides async/await semantics.

.. code-block:: python

    import asyncio
    from redis.asyncio.multidb.client import MultiDBClient
    from redis.asyncio.multidb.config import MultiDbConfig, DatabaseConfig

    async def main():
        cfg = MultiDbConfig(
            databases_config=[
                DatabaseConfig(from_url="redis://db-primary:6379/0", weight=1.0),
                DatabaseConfig(from_url="redis://db-secondary:6379/0", weight=0.5),
            ]
        )

        # Context-manager approach for graceful client termination when exits.
        # client = MultiDBClient(cfg) could be used instead
        async with MultiDBClient(cfg) as client:
            await client.set("key", "value")
            print(await client.get("key"))

        # Pipeline
        async with client.pipeline() as pipe:
            pipe.set("a", 1)
            pipe.incrby("a", 2)
            values = await pipe.execute()
            print(values)

        # Transaction
        async def txn(pipe):
            current = await pipe.get("balance")
            current = int(current or 0)
            await pipe.multi()
            await pipe.set("balance", current + 100)

        await client.transaction(txn)

        # Pub/Sub
        pubsub = client.pubsub()
        await pubsub.subscribe("events")
        message = await pubsub.get_message(timeout=1.0)
        if message:
            print(message)

    asyncio.run(main())


MultiDBClient
^^^^^^^^^^^^^

The client exposes the same API as the `Redis` or `RedisCluster` client, making it fully interchangeable and ensuring a simple migration of your application code. Additionally, it supports runtime reconfiguration, allowing you to add features such as health checks, failure detectors, or even new databases without restarting.

Configuration
-------------

MultiDbConfig
^^^^^^^^^^^^^

.. code-block:: python

    from redis.multidb.config import (
        MultiDbConfig, DatabaseConfig,
        DEFAULT_HEALTH_CHECK_INTERVAL, DEFAULT_GRACE_PERIOD
    )
    from redis.retry import Retry
    from redis.backoff import ExponentialWithJitterBackoff

    cfg = MultiDbConfig(
        databases_config=[
            # Construct via URL
            DatabaseConfig(
                from_url="redis://db-a:6379/0",
                weight=1.0,
                # Optional: use a custom circuit breaker grace period
                grace_period=DEFAULT_GRACE_PERIOD,
                # Optional: Redis Enterprise cluster FQDN for REST health checks
                health_check_url="https://cluster.example.com",
                # Optional: Underlying Redis client related configuration
                client_kwargs={"socket_timeout": 5}
            ),
            # Or construct via ConnectionPool
            # DatabaseConfig(from_pool=my_pool, weight=1.0),
        ],

        # Global command retry policy (applied at multi-db layer)
        command_retry=Retry(
            retries=3,
            backoff=ExponentialWithJitterBackoff(base=1, cap=10),
        ),

        # Health checks
        health_check_interval: float = DEFAULT_HEALTH_CHECK_INTERVAL # seconds
        health_check_probes: int = DEFAULT_HEALTH_CHECK_PROBES
        health_check_delay: float = DEFAULT_HEALTH_CHECK_DELAY # seconds
        health_check_policy: HealthCheckPolicies = DEFAULT_HEALTH_CHECK_POLICY,

        # Failure detector
        min_num_failures: int = DEFAULT_MIN_NUM_FAILURES
        failure_rate_threshold: float = DEFAULT_FAILURE_RATE_THRESHOLD
        failures_detection_window: float = DEFAULT_FAILURES_DETECTION_WINDOW # seconds

        # Failover behavior
        failover_attempts: int = DEFAULT_FAILOVER_ATTEMPTS
        failover_delay: float = DEFAULT_FAILOVER_DELAY # seconds
    )

Notes:

- Low-level client retries are disabled automatically per database. The multi-database layer handles retries.
- For clusters, health checks validate all nodes.

DatabaseConfig
^^^^^^^^^^^^^^

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

Health Monitoring
-----------------
The `MultiDBClient` uses two complementary mechanisms to ensure database availability:
- Health Checks (Proactive Monitoring)
- Failure Detection (Reactive Monitoring)


Health Checks (Proactive Monitoring)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
These checks run continuously in the background at configured intervals to proactively
detect database issues. They run in the background with a given interval and
configuration defined in the `MultiDBConfig` class.

To avoid false positives, you can configure amount of health check probes and also
define one of the health check policies to evaluate probes result.

**HealthCheckPolicies.HEALTHY_ALL** - (default) All probes should be successful.
**HealthCheckPolicies.HEALTHY_MAJORITY** - Majority of probes should be successful.
**HealthCheckPolicies.HEALTHY_ANY** - Any of probes should be successful.

PingHealthCheck (default)
^^^^^^^^^^^^^^^^^^^^^^^^^

The default health check sends the [PING](https://redis.io/docs/latest/commands/ping/) command
to the database (and to all nodes for clusters).

Lag-Aware Healthcheck (Redis Enterprise Only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is a special type of healthcheck available for Redis (Enterprise) Software
that utilizes a REST API endpoint to obtain information about the synchronization
lag between a given database and all other databases in an Active-Active setup.

To use this healthcheck, first you need to adjust your `DatabaseConfig`
to expose `health_check_url` used by your deployment. By default, your
Cluster FQDN should be used as URL, unless you have some kind of
reverse proxy behind an actual REST API endpoint.

.. code-block:: python

    from redis.multidb.client import MultiDBClient
    from redis.multidb.config import MultiDbConfig, DatabaseConfig
    from redis.multidb.healthcheck import PingHealthCheck, LagAwareHealthCheck
    from redis.retry import Retry
    from redis.backoff import ExponentialWithJitterBackoff

    cfg = MultiDbConfig(
        databases_config=[
            DatabaseConfig(
                from_url="redis://db-primary:6379/0",
                weight=1.0,
                health_check_url="https://cluster.example.com",  # optional for LagAware
            ),
            DatabaseConfig(
                from_url="redis://db-secondary:6379/0",
                weight=0.5,
                health_check_url="https://cluster.example.com",
            ),
        ],
        # Add custom health check to replace the default
        health_checks=[
            # Redis Enterprise REST-based lag-aware check
            LagAwareHealthCheck(
                # Customize REST port, lag tolerance, TLS, etc.
                rest_api_port=9443,
                lag_aware_tolerance=100,  # ms
                verify_tls=True,
                # auth_basic=("user", "pass"),
                # ca_file="/path/ca.pem",
                # client_cert_file="/path/cert.pem",
                # client_key_file="/path/key.pem",
            ),
        ],
    )

    client = MultiDBClient(cfg)


**Custom Health Checks**
~~~~~~~~~~~~~~~~~~~~~
You can add custom health checks for specific requirements:

.. code-block:: python

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


Failure Detection (Reactive Monitoring)
-----------------

The failure detector monitors command failures and marks a database as unhealthy when its failure rate exceeds a defined threshold within a sliding time window.
Under real traffic conditions, this reactive detection mechanism likely triggers earlier than proactive health checks.
You can extend the set of failure detectors by implementing your own and configuring it through the `MultiDBConfig` class.

By default the failure detector is configured for 1000 failures and a 10% failure rate
threshold within a 2 seconds sliding window. This could be adjusted regarding
your application specifics and traffic pattern.

.. code-block:: python

    from redis.multidb.config import MultiDbConfig, DatabaseConfig
    from redis.multidb.client import MultiDBClient

    cfg = MultiDbConfig(
        databases_config=[
            DatabaseConfig(from_url="redis://db-a:6379/0", weight=1.0),
            DatabaseConfig(from_url="redis://db-b:6379/0", weight=0.5),
        ],
        # Default detector also created from config values
    )

    client = MultiDBClient(cfg)

    # Add an additional detector, optionally limited to specific exception types:
    client.add_failure_detector(
        CustomFailureDetector()
    )

Failover and automatic fallback
--------------------------

Weight-based failover chooses the highest-weighted database with a CLOSED circuit. If no database is
healthy it returns `TemporaryUnavailableException`. This exception indicates that the application should
retry sending requests for a configurable period of time (the configuration (`failover_attempts` * `failover_delay`)
defaults to 120 seconds). If none of the databases became available, then a `NoValidDatabaseException` is thrown.

To enable periodic fallback to a higher-priority healthy database, set `auto_fallback_interval` (seconds):

.. code-block:: python

    from redis.multidb.config import MultiDbConfig, DatabaseConfig

    cfg = MultiDbConfig(
        databases_config=[
            DatabaseConfig(from_url="redis://db-primary:6379/0", weight=1.0),
            DatabaseConfig(from_url="redis://db-secondary:6379/0", weight=0.5),
        ],
        # Try to fallback to higher-weight healthy database every 30 seconds
        auto_fallback_interval=30.0,
    )
    client = MultiDBClient(cfg)


Custom failover callbacks
-------------------------

You may want to activate custom actions when failover happens. For example, you may want to collect some metrics,
logs or externally persist a connection state.

You can register your own event listener for the `ActiveDatabaseChanged` event (which is emitted when a failover happens) using
the `EventDispatcher`.

.. code-block:: python

    class LogFailoverEventListener(EventListenerInterface):
        def __init__(self, logger: Logger):
            self.logger = logger

        def listen(self, event: ActiveDatabaseChanged):
            self.logger.warning(
                f"Failover happened. Active database switched from {event.old_database} to {event.new_database}"
            )

    event_dispatcher = EventDispatcher()
    listener = LogFailoverEventListener(logging.getLogger(__name__))

    # Register custom listener
    event_dispatcher.register_listeners(
        {
            ActiveDatabaseChanged: [listener],
        }
    )

    config = MultiDbConfig(
        client_class=client_class,
        databases_config=db_configs,
        command_retry=command_retry,
        min_num_failures=min_num_failures,
        health_check_probes=3,
        health_check_interval=health_check_interval,
        event_dispatcher=event_dispatcher,
        health_check_probes_delay=health_check_delay,
    )

    client = MultiDBClient(config)


Managing databases at runtime
-----------------------------

You can manually add/remove databases, update weights, and promote a database if it’s healthy.

.. code-block:: python

    from redis.multidb.client import MultiDBClient
    from redis.multidb.config import MultiDbConfig, DatabaseConfig
    from redis.multidb.database import Database
    from redis.multidb.circuit import PBCircuitBreakerAdapter
    import pybreaker
    from redis import Redis

    cfg = MultiDbConfig(
        databases_config=[DatabaseConfig(from_url="redis://db-a:6379/0", weight=1.0)]
    )
    client = MultiDBClient(cfg)

    # Add a database programmatically
    other = Database(
        client=Redis.from_url("redis://db-b:6379/0"),
        circuit=PBCircuitBreakerAdapter(pybreaker.CircuitBreaker(reset_timeout=5.0)),
        weight=0.5,
        health_check_url=None,
    )
    client.add_database(other)

    # Update weight; if it becomes the highest and healthy, it may become active
    client.update_database_weight(other, 0.9)

    # Promote a specific healthy database to active
    client.set_active_database(other)

    # Remove a database
    client.remove_database(other)

Pub/Sub and re-subscription
--------------------------

The MultiDBClient offers Pub/Sub functionality with automatic re-subscription
to channels during failover events. For optimal failover handling,
both publishers and subscribers should use MultiDBClient instances.

1. **Subscriber failover**: Automatically reconnects to an alternative database
and re-subscribes to the same channels
2. **Publisher failover**: Seamlessly switches to an alternative database and
continues publishing to the same channels
**Note**: Message loss may occur if failover events happen in reverse order
(publisher fails before subscriber).

.. code-block:: python

    pubsub = client.pubsub()
    pubsub.subscribe("news", "alerts")
    # If failover happens here, subscriptions are re-established on the new active DB.
    msg = pubsub.get_message(timeout=1.0)
    if msg:
        print(msg)

Pipelines and transactions
--------------------------

Pipelines and transactions are executed against the active database at execution time. The client ensures
the active database is healthy and up-to-date before running the stack.

.. code-block:: python

    with client.pipeline() as pipe:
        pipe.set("x", 1)
        pipe.incr("x")
        results = pipe.execute()

    def txn(pipe):
        pipe.multi()
        pipe.set("y", "42")

    client.transaction(txn)

Best practices
--------------

- Assign the highest weight to your primary database and lower weights to replicas or disaster recovery sites.
- Keep `health_check_interval` short enough to promptly detect failures but avoid excessive load.
- Tune `command_retry` and failover attempts to your SLA and workload profile.
- Use `auto_fallback_interval` if you want the client to fail over back to your primary automatically.
- Handle `TemporaryUnavailableException` to be able to recover before giving up. In the meantime, you
can switch the data source (e.g. cache). `NoValidDatabaseException` indicates that there are no healthy
databases to operate.

Troubleshooting
---------------

- NoValidDatabaseException:
  Indicates no healthy database is available. Check circuit breaker states and health checks.

- TemporaryUnavailableException
  Indicates that currently there are no healthy databases, but you can still send requests until
  `NoValidDatabaseException` is thrown. Probe interval is configured with `failure_attemtps`

- Health checks always failing:
  Verify connectivity and, for clusters, that all nodes are reachable. For `LagAwareHealthCheck`,
  ensure `health_check_url` points to your Redis Enterprise endpoint and authentication/TLS options
  are configured properly.

- Pub/Sub not receiving messages after failover:
  Ensure you are using the client’s Pub/Sub helper. The client re-subscribes automatically on switch.
