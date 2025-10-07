import logging
import threading
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Callable, List, Optional

from redis.background import BackgroundScheduler
from redis.client import PubSubWorkerThread
from redis.commands import CoreCommands, RedisModuleCommands
from redis.multidb.circuit import CircuitBreaker
from redis.multidb.circuit import State as CBState
from redis.multidb.command_executor import DefaultCommandExecutor
from redis.multidb.config import DEFAULT_GRACE_PERIOD, MultiDbConfig
from redis.multidb.database import Database, Databases, SyncDatabase
from redis.multidb.exception import NoValidDatabaseException, UnhealthyDatabaseException
from redis.multidb.failure_detector import FailureDetector
from redis.multidb.healthcheck import HealthCheck, HealthCheckPolicy

logger = logging.getLogger(__name__)


class MultiDBClient(RedisModuleCommands, CoreCommands):
    """
    Client that operates on multiple logical Redis databases.
    Should be used in Active-Active database setups.
    """

    def __init__(self, config: MultiDbConfig):
        self._databases = config.databases()
        self._health_checks = config.default_health_checks()

        if config.health_checks is not None:
            self._health_checks.extend(config.health_checks)

        self._health_check_interval = config.health_check_interval
        self._health_check_policy: HealthCheckPolicy = config.health_check_policy.value(
            config.health_check_probes, config.health_check_probes_delay
        )
        self._failure_detectors = config.default_failure_detectors()

        if config.failure_detectors is not None:
            self._failure_detectors.extend(config.failure_detectors)

        self._failover_strategy = (
            config.default_failover_strategy()
            if config.failover_strategy is None
            else config.failover_strategy
        )
        self._failover_strategy.set_databases(self._databases)
        self._auto_fallback_interval = config.auto_fallback_interval
        self._event_dispatcher = config.event_dispatcher
        self._command_retry = config.command_retry
        self._command_retry.update_supported_errors((ConnectionRefusedError,))
        self.command_executor = DefaultCommandExecutor(
            failure_detectors=self._failure_detectors,
            databases=self._databases,
            command_retry=self._command_retry,
            failover_strategy=self._failover_strategy,
            failover_attempts=config.failover_attempts,
            failover_delay=config.failover_delay,
            event_dispatcher=self._event_dispatcher,
            auto_fallback_interval=self._auto_fallback_interval,
        )
        self.initialized = False
        self._hc_lock = threading.RLock()
        self._bg_scheduler = BackgroundScheduler()
        self._config = config

    def initialize(self):
        """
        Perform initialization of databases to define their initial state.
        """

        def raise_exception_on_failed_hc(error):
            raise error

        # Initial databases check to define initial state
        self._check_databases_health(on_error=raise_exception_on_failed_hc)

        # Starts recurring health checks on the background.
        self._bg_scheduler.run_recurring(
            self._health_check_interval,
            self._check_databases_health,
        )

        is_active_db_found = False

        for database, weight in self._databases:
            # Set on state changed callback for each circuit.
            database.circuit.on_state_changed(self._on_circuit_state_change_callback)

            # Set states according to a weights and circuit state
            if database.circuit.state == CBState.CLOSED and not is_active_db_found:
                self.command_executor.active_database = database
                is_active_db_found = True

        if not is_active_db_found:
            raise NoValidDatabaseException(
                "Initial connection failed - no active database found"
            )

        self.initialized = True

    def get_databases(self) -> Databases:
        """
        Returns a sorted (by weight) list of all databases.
        """
        return self._databases

    def set_active_database(self, database: SyncDatabase) -> None:
        """
        Promote one of the existing databases to become an active.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True
                break

        if not exists:
            raise ValueError("Given database is not a member of database list")

        self._check_db_health(database)

        if database.circuit.state == CBState.CLOSED:
            highest_weighted_db, _ = self._databases.get_top_n(1)[0]
            self.command_executor.active_database = database
            return

        raise NoValidDatabaseException(
            "Cannot set active database, database is unhealthy"
        )

    def add_database(self, database: SyncDatabase):
        """
        Adds a new database to the database list.
        """
        for existing_db, _ in self._databases:
            if existing_db == database:
                raise ValueError("Given database already exists")

        self._check_db_health(database)

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.add(database, database.weight)
        self._change_active_database(database, highest_weighted_db)

    def _change_active_database(
        self, new_database: SyncDatabase, highest_weight_database: SyncDatabase
    ):
        if (
            new_database.weight > highest_weight_database.weight
            and new_database.circuit.state == CBState.CLOSED
        ):
            self.command_executor.active_database = new_database

    def remove_database(self, database: Database):
        """
        Removes a database from the database list.
        """
        weight = self._databases.remove(database)
        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]

        if (
            highest_weight <= weight
            and highest_weighted_db.circuit.state == CBState.CLOSED
        ):
            self.command_executor.active_database = highest_weighted_db

    def update_database_weight(self, database: SyncDatabase, weight: float):
        """
        Updates a database from the database list.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True
                break

        if not exists:
            raise ValueError("Given database is not a member of database list")

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.update_weight(database, weight)
        database.weight = weight
        self._change_active_database(database, highest_weighted_db)

    def add_failure_detector(self, failure_detector: FailureDetector):
        """
        Adds a new failure detector to the database.
        """
        self._failure_detectors.append(failure_detector)

    def add_health_check(self, healthcheck: HealthCheck):
        """
        Adds a new health check to the database.
        """
        with self._hc_lock:
            self._health_checks.append(healthcheck)

    def execute_command(self, *args, **options):
        """
        Executes a single command and return its result.
        """
        if not self.initialized:
            self.initialize()

        return self.command_executor.execute_command(*args, **options)

    def pipeline(self):
        """
        Enters into pipeline mode of the client.
        """
        return Pipeline(self)

    def transaction(self, func: Callable[["Pipeline"], None], *watches, **options):
        """
        Executes callable as transaction.
        """
        if not self.initialized:
            self.initialize()

        return self.command_executor.execute_transaction(func, *watches, *options)

    def pubsub(self, **kwargs):
        """
        Return a Publish/Subscribe object. With this object, you can
        subscribe to channels and listen for messages that get published to
        them.
        """
        if not self.initialized:
            self.initialize()

        return PubSub(self, **kwargs)

    def _check_db_health(self, database: SyncDatabase) -> bool:
        """
        Runs health checks on the given database until first failure.
        """
        # Health check will setup circuit state
        is_healthy = self._health_check_policy.execute(self._health_checks, database)

        if not is_healthy:
            if database.circuit.state != CBState.OPEN:
                database.circuit.state = CBState.OPEN
            return is_healthy
        elif is_healthy and database.circuit.state != CBState.CLOSED:
            database.circuit.state = CBState.CLOSED

        return is_healthy

    def _check_databases_health(self, on_error: Callable[[Exception], None] = None):
        """
        Runs health checks as a recurring task.
        Runs health checks against all databases.
        """
        with ThreadPoolExecutor(max_workers=len(self._databases)) as executor:
            # Submit all health checks
            futures = {
                executor.submit(self._check_db_health, database)
                for database, _ in self._databases
            }

            try:
                for future in as_completed(
                    futures, timeout=self._health_check_interval
                ):
                    try:
                        future.result()
                    except UnhealthyDatabaseException as e:
                        unhealthy_db = e.database
                        unhealthy_db.circuit.state = CBState.OPEN

                        logger.exception(
                            "Health check failed, due to exception",
                            exc_info=e.original_exception,
                        )

                        if on_error:
                            on_error(e.original_exception)
            except TimeoutError:
                raise TimeoutError(
                    "Health check execution exceeds health_check_interval"
                )

    def _on_circuit_state_change_callback(
        self, circuit: CircuitBreaker, old_state: CBState, new_state: CBState
    ):
        if new_state == CBState.HALF_OPEN:
            self._check_db_health(circuit.database)
            return

        if old_state == CBState.CLOSED and new_state == CBState.OPEN:
            self._bg_scheduler.run_once(
                DEFAULT_GRACE_PERIOD, _half_open_circuit, circuit
            )

    def close(self):
        self.command_executor.active_database.client.close()


def _half_open_circuit(circuit: CircuitBreaker):
    circuit.state = CBState.HALF_OPEN


class Pipeline(RedisModuleCommands, CoreCommands):
    """
    Pipeline implementation for multiple logical Redis databases.
    """

    def __init__(self, client: MultiDBClient):
        self._command_stack = []
        self._client = client

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self) -> int:
        return len(self._command_stack)

    def __bool__(self) -> bool:
        """Pipeline instances should always evaluate to True"""
        return True

    def reset(self) -> None:
        self._command_stack = []

    def close(self) -> None:
        """Close the pipeline"""
        self.reset()

    def pipeline_execute_command(self, *args, **options) -> "Pipeline":
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self._command_stack.append((args, options))
        return self

    def execute_command(self, *args, **kwargs):
        """Adds a command to the stack"""
        return self.pipeline_execute_command(*args, **kwargs)

    def execute(self) -> List[Any]:
        """Execute all the commands in the current pipeline"""
        if not self._client.initialized:
            self._client.initialize()

        try:
            return self._client.command_executor.execute_pipeline(
                tuple(self._command_stack)
            )
        finally:
            self.reset()


class PubSub:
    """
    PubSub object for multi database client.
    """

    def __init__(self, client: MultiDBClient, **kwargs):
        """Initialize the PubSub object for a multi-database client.

        Args:
            client: MultiDBClient instance to use for pub/sub operations
            **kwargs: Additional keyword arguments to pass to the underlying pubsub implementation
        """

        self._client = client
        self._client.command_executor.pubsub(**kwargs)

    def __enter__(self) -> "PubSub":
        return self

    def __del__(self) -> None:
        try:
            # if this object went out of scope prior to shutting down
            # subscriptions, close the connection manually before
            # returning it to the connection pool
            self.reset()
        except Exception:
            pass

    def reset(self) -> None:
        return self._client.command_executor.execute_pubsub_method("reset")

    def close(self) -> None:
        self.reset()

    @property
    def subscribed(self) -> bool:
        return self._client.command_executor.active_pubsub.subscribed

    def execute_command(self, *args):
        return self._client.command_executor.execute_pubsub_method(
            "execute_command", *args
        )

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        return self._client.command_executor.execute_pubsub_method(
            "psubscribe", *args, **kwargs
        )

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empty, unsubscribe from
        all patterns.
        """
        return self._client.command_executor.execute_pubsub_method(
            "punsubscribe", *args
        )

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        return self._client.command_executor.execute_pubsub_method(
            "subscribe", *args, **kwargs
        )

    def unsubscribe(self, *args):
        """
        Unsubscribe from the supplied channels. If empty, unsubscribe from
        all channels
        """
        return self._client.command_executor.execute_pubsub_method("unsubscribe", *args)

    def ssubscribe(self, *args, **kwargs):
        """
        Subscribes the client to the specified shard channels.
        Channels supplied as keyword arguments expect a channel name as the key
        and a callable as the value. A channel's callable will be invoked automatically
        when a message is received on that channel rather than producing a message via
        ``listen()`` or ``get_sharded_message()``.
        """
        return self._client.command_executor.execute_pubsub_method(
            "ssubscribe", *args, **kwargs
        )

    def sunsubscribe(self, *args):
        """
        Unsubscribe from the supplied shard_channels. If empty, unsubscribe from
        all shard_channels
        """
        return self._client.command_executor.execute_pubsub_method(
            "sunsubscribe", *args
        )

    def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number, or None, to wait indefinitely.
        """
        return self._client.command_executor.execute_pubsub_method(
            "get_message",
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=timeout,
        )

    def get_sharded_message(
        self, ignore_subscribe_messages: bool = False, timeout: float = 0.0
    ):
        """
        Get the next message if one is available in a sharded channel, otherwise None.

        If timeout is specified, the system will wait for `timeout` seconds
        before returning. Timeout should be specified as a floating point
        number, or None, to wait indefinitely.
        """
        return self._client.command_executor.execute_pubsub_method(
            "get_sharded_message",
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=timeout,
        )

    def run_in_thread(
        self,
        sleep_time: float = 0.0,
        daemon: bool = False,
        exception_handler: Optional[Callable] = None,
        sharded_pubsub: bool = False,
    ) -> "PubSubWorkerThread":
        return self._client.command_executor.execute_pubsub_run(
            sleep_time,
            daemon=daemon,
            exception_handler=exception_handler,
            pubsub=self,
            sharded_pubsub=sharded_pubsub,
        )
