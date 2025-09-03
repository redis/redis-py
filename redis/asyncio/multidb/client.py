import asyncio
from typing import Callable, Optional, Coroutine, Any

from redis.asyncio.multidb.command_executor import DefaultCommandExecutor
from redis.asyncio.multidb.database import AsyncDatabase, Databases
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.asyncio.multidb.healthcheck import HealthCheck
from redis.multidb.circuit import State as CBState, CircuitBreaker
from redis.asyncio.multidb.config import MultiDbConfig, DEFAULT_GRACE_PERIOD
from redis.background import BackgroundScheduler
from redis.commands import AsyncRedisModuleCommands, AsyncCoreCommands
from redis.multidb.exception import NoValidDatabaseException


class MultiDBClient(AsyncRedisModuleCommands, AsyncCoreCommands):
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
        self._failure_detectors = config.default_failure_detectors()

        if config.failure_detectors is not None:
            self._failure_detectors.extend(config.failure_detectors)

        self._failover_strategy = config.default_failover_strategy() \
            if config.failover_strategy is None else config.failover_strategy
        self._failover_strategy.set_databases(self._databases)
        self._auto_fallback_interval = config.auto_fallback_interval
        self._event_dispatcher = config.event_dispatcher
        self._command_retry = config.command_retry
        self._command_retry.update_supported_errors([ConnectionRefusedError])
        self.command_executor = DefaultCommandExecutor(
            failure_detectors=self._failure_detectors,
            databases=self._databases,
            command_retry=self._command_retry,
            failover_strategy=self._failover_strategy,
            event_dispatcher=self._event_dispatcher,
            auto_fallback_interval=self._auto_fallback_interval,
        )
        self.initialized = False
        self._hc_lock = asyncio.Lock()
        self._bg_scheduler = BackgroundScheduler()
        self._config = config

    async def initialize(self):
        """
        Perform initialization of databases to define their initial state.
        """
        async def raise_exception_on_failed_hc(error):
            raise error

        # Initial databases check to define initial state
        await self._check_databases_health(on_error=raise_exception_on_failed_hc)

        # Starts recurring health checks on the background.
        await self._bg_scheduler.run_recurring_async(
            self._health_check_interval,
            self._check_databases_health,
        )

        is_active_db_found = False

        for database, weight in self._databases:
            # Set on state changed callback for each circuit.
            database.circuit.on_state_changed(self._on_circuit_state_change_callback)

            # Set states according to a weights and circuit state
            if database.circuit.state == CBState.CLOSED and not is_active_db_found:
                await self.command_executor.set_active_database(database)
                is_active_db_found = True

        if not is_active_db_found:
            raise NoValidDatabaseException('Initial connection failed - no active database found')

        self.initialized = True

    def get_databases(self) -> Databases:
        """
        Returns a sorted (by weight) list of all databases.
        """
        return self._databases

    async def set_active_database(self, database: AsyncDatabase) -> None:
        """
        Promote one of the existing databases to become an active.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True
                break

        if not exists:
            raise ValueError('Given database is not a member of database list')

        await self._check_db_health(database)

        if database.circuit.state == CBState.CLOSED:
            highest_weighted_db, _ = self._databases.get_top_n(1)[0]
            await self.command_executor.set_active_database(database)
            return

        raise NoValidDatabaseException('Cannot set active database, database is unhealthy')

    async def add_database(self, database: AsyncDatabase):
        """
        Adds a new database to the database list.
        """
        for existing_db, _ in self._databases:
            if existing_db == database:
                raise ValueError('Given database already exists')

        await self._check_db_health(database)

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.add(database, database.weight)
        await self._change_active_database(database, highest_weighted_db)

    async def _change_active_database(self, new_database: AsyncDatabase, highest_weight_database: AsyncDatabase):
        if new_database.weight > highest_weight_database.weight and new_database.circuit.state == CBState.CLOSED:
            await self.command_executor.set_active_database(new_database)

    async def remove_database(self, database: AsyncDatabase):
        """
        Removes a database from the database list.
        """
        weight = self._databases.remove(database)
        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]

        if highest_weight <= weight and highest_weighted_db.circuit.state == CBState.CLOSED:
            await self.command_executor.set_active_database(highest_weighted_db)

    async def update_database_weight(self, database: AsyncDatabase, weight: float):
        """
        Updates a database from the database list.
        """
        exists = None

        for existing_db, _ in self._databases:
            if existing_db == database:
                exists = True
                break

        if not exists:
            raise ValueError('Given database is not a member of database list')

        highest_weighted_db, highest_weight = self._databases.get_top_n(1)[0]
        self._databases.update_weight(database, weight)
        database.weight = weight
        await self._change_active_database(database, highest_weighted_db)

    def add_failure_detector(self, failure_detector: AsyncFailureDetector):
        """
        Adds a new failure detector to the database.
        """
        self._failure_detectors.append(failure_detector)

    async def add_health_check(self, healthcheck: HealthCheck):
        """
        Adds a new health check to the database.
        """
        async with self._hc_lock:
            self._health_checks.append(healthcheck)

    async def execute_command(self, *args, **options):
        """
        Executes a single command and return its result.
        """
        if not self.initialized:
            await self.initialize()

        return await self.command_executor.execute_command(*args, **options)

    async def _check_databases_health(
            self,
            on_error: Optional[Callable[[Exception], Coroutine[Any, Any, None]]] = None,
    ):
        """
        Runs health checks as a recurring task.
        Runs health checks against all databases.
        """
        for database, _ in self._databases:
            async with self._hc_lock:
                await self._check_db_health(database, on_error)

    async def _check_db_health(
            self,
            database: AsyncDatabase,
            on_error: Optional[Callable[[Exception], Coroutine[Any, Any, None]]] = None,
    ) -> None:
        """
        Runs health checks on the given database until first failure.
        """
        is_healthy = True

        # Health check will setup circuit state
        for health_check in self._health_checks:
            if not is_healthy:
                # If one of the health checks failed, it's considered unhealthy
                break

            try:
                is_healthy = await health_check.check_health(database)

                if not is_healthy and database.circuit.state != CBState.OPEN:
                    database.circuit.state = CBState.OPEN
                elif is_healthy and database.circuit.state != CBState.CLOSED:
                    database.circuit.state = CBState.CLOSED
            except Exception as e:
                if database.circuit.state != CBState.OPEN:
                    database.circuit.state = CBState.OPEN
                is_healthy = False

                if on_error:
                    await on_error(e)

    def _on_circuit_state_change_callback(self, circuit: CircuitBreaker, old_state: CBState, new_state: CBState):
        loop = asyncio.get_running_loop()

        if new_state == CBState.HALF_OPEN:
            asyncio.create_task(self._check_db_health(circuit.database))
            return

        if old_state == CBState.CLOSED and new_state == CBState.OPEN:
            loop.call_later(DEFAULT_GRACE_PERIOD, _half_open_circuit, circuit)

def _half_open_circuit(circuit: CircuitBreaker):
    circuit.state = CBState.HALF_OPEN