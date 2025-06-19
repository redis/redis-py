import threading

from redis.commands import RedisModuleCommands, CoreCommands, SentinelCommands
from redis.multidb.command_executor import DefaultCommandExecutor
from redis.multidb.config import MultiDbConfig
from redis.multidb.circuit import State as CBState
from redis.multidb.database import State as DBState, Database, AbstractDatabase
from redis.multidb.exception import NoValidDatabaseException


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
        self._lock = threading.RLock()

    def _initialize(self):
        """
        Perform initialization of databases to define their initial state.
        """

        is_active_db = False

        for database, weight in self._databases:
            self._check_db_health(database)

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

    def add_database(self, database: Database):
        """
        Adds a new database to the database list.
        """
        with self._lock:
            if database in self._databases:
                raise ValueError('Given database already exists')


    def execute_command(self, *args, **options):
        """
        Executes a single command and return its result.
        """
        if not self._initialized:
            self._initialize()

        with self._lock:
            return self._command_executor.execute_command(*args, **options)

    def _check_db_health(self, database: AbstractDatabase) -> None:
        """
        Runs health checks on the given database until first failure.
        """
        is_healthy = True

        # Health check will setup circuit state
        for health_check in self._health_checks:
            if not is_healthy:
                # If one of the health checks failed, it's considered unhealthy
                break

            is_healthy = health_check.check_health(database)

