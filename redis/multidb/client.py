import threading
import socket

from redis.background import BackgroundScheduler
from redis.exceptions import ConnectionError, TimeoutError
from redis.commands import RedisModuleCommands, CoreCommands, SentinelCommands
from redis.multidb.command_executor import DefaultCommandExecutor
from redis.multidb.config import MultiDbConfig, DEFAULT_GRACE_PERIOD
from redis.multidb.circuit import State as CBState, CircuitBreaker
from redis.multidb.database import State as DBState, Database, AbstractDatabase, Databases
from redis.multidb.exception import NoValidDatabaseException
from redis.multidb.failure_detector import FailureDetector
from redis.multidb.healthcheck import HealthCheck


class MultiDBClient(RedisModuleCommands, CoreCommands, SentinelCommands):
    """
    Client that operates on multiple logical Redis databases.
    Should be used in Active-Active database setups.
    """
    def __init__(self, config: MultiDbConfig):
        self._databases = config.databases()
        self._health_checks = config.health_checks
        self._health_check_interval = config.health_check_interval
        self._failure_detectors = config.failure_detectors
        self._failover_strategy = config.failover_strategy
        self._failover_strategy.set_databases(self._databases)
        self._auto_fallback_interval = config.auto_fallback_interval
        self._event_dispatcher = config.event_dispatcher
        self._command_executor = DefaultCommandExecutor(
            failure_detectors=self._failure_detectors,
            databases=self._databases,
            failover_strategy=self._failover_strategy,
            event_dispatcher=self._event_dispatcher,
            auto_fallback_interval=self._auto_fallback_interval,
        )
        self._initialized = False
        self._hc_lock = threading.RLock()
        self._bg_scheduler = BackgroundScheduler()

    def _initialize(self):
        """
        Perform initialization of databases to define their initial state.
        """

        # Initial databases check to define initial state
        self._check_databases_health()

        # Starts recurring health checks on the background.
        self._bg_scheduler.run_recurring(
            self._health_check_interval,
            self._check_databases_health,
        )

        is_active_db = False

        for database, weight in self._databases:
            # Set on state changed callback for each circuit.
            database.circuit.on_state_changed(self._on_circuit_state_change_callback)

            # Set states according to a weights and circuit state
            if database.circuit.state == CBState.CLOSED and not is_active_db:
                database.state = DBState.ACTIVE
                self._command_executor.active_database = database
                is_active_db = True
            elif database.circuit.state == CBState.CLOSED and is_active_db:
                database.state = DBState.PASSIVE
            else:
                database.state = DBState.DISCONNECTED

        if not is_active_db:
            raise NoValidDatabaseException('Initial connection failed - no active database found')

        self._initialized = True

    def get_databases(self) -> Databases:
        """
        Returns a sorted (by weight) list of all databases.
        """
        return self._databases

    def set_active_database(self, database: AbstractDatabase) -> None:
        """
        Promote one of the existing databases to become an active.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True

        if not exists:
            raise ValueError('Given database is not a member of database list')

        self._check_db_health(database)

        if database.circuit.state == CBState.CLOSED:
            highest_weighted_db, _ = self._databases.get_top_n(1)[0]
            highest_weighted_db.state = DBState.PASSIVE
            database.state = DBState.ACTIVE
            self._command_executor.active_database = database
            return

        raise NoValidDatabaseException('Cannot set active database, database is unhealthy')

    def add_database(self, database: AbstractDatabase):
        """
        Adds a new database to the database list.
        """
        for existing_db, _ in self._databases:
            if existing_db == database:
                raise ValueError('Given database already exists')

        self._check_db_health(database)

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.add(database, database.weight)

        if database.weight > highest_weight and database.circuit.state == CBState.CLOSED:
            database.state = DBState.ACTIVE
            self._command_executor.active_database = database
            highest_weighted_db.state = DBState.PASSIVE

    def remove_database(self, database: Database):
        """
        Removes a database from the database list.
        """
        weight = self._databases.remove(database)
        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]

        if highest_weight <= weight and highest_weighted_db.circuit.state == CBState.CLOSED:
            highest_weighted_db.state = DBState.ACTIVE
            self._command_executor.active_database = highest_weighted_db

    def update_database_weight(self, database: AbstractDatabase, weight: float):
        """
        Updates a database from the database list.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True

        if not exists:
            raise ValueError('Given database is not a member of database list')

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.update_weight(database, weight)

        if weight > highest_weight and database.circuit.state == CBState.CLOSED:
            database.state = DBState.ACTIVE
            self._command_executor.active_database = database
            highest_weighted_db.state = DBState.PASSIVE

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
        if not self._initialized:
            self._initialize()

        return self._command_executor.execute_command(*args, **options)

    def _check_db_health(self, database: AbstractDatabase) -> None:
        """
        Runs health checks on the given database until first failure.
        """
        is_healthy = True

        with self._hc_lock:
            # Health check will setup circuit state
            for health_check in self._health_checks:
                if not is_healthy:
                    # If one of the health checks failed, it's considered unhealthy
                    break

                try:
                    is_healthy = health_check.check_health(database)

                    if not is_healthy and database.circuit.state != CBState.OPEN:
                        database.circuit.state = CBState.OPEN
                    elif is_healthy and database.circuit.state != CBState.CLOSED:
                        database.circuit.state = CBState.CLOSED
                except (ConnectionError, TimeoutError, socket.timeout):
                    if database.circuit.state != CBState.OPEN:
                        database.circuit.state = CBState.OPEN
                    is_healthy = False


    def _check_databases_health(self):
        """
        Runs health checks as recurring task.
        """
        for database, _ in self._databases:
            self._check_db_health(database)

    def _on_circuit_state_change_callback(self, circuit: CircuitBreaker, old_state: CBState, new_state: CBState):
        if new_state == CBState.HALF_OPEN:
            self._check_db_health(circuit.database)
            return

        if old_state == CBState.CLOSED and new_state == CBState.OPEN:
            self._bg_scheduler.run_once(DEFAULT_GRACE_PERIOD, _half_open_circuit, circuit)

def _half_open_circuit(circuit: CircuitBreaker):
    circuit.state = CBState.HALF_OPEN