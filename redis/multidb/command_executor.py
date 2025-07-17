import socket
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Union, Optional, Callable

from redis.client import Pipeline, PubSub, PubSubWorkerThread
from redis.exceptions import ConnectionError, TimeoutError
from redis.event import EventDispatcherInterface, OnCommandsFailEvent
from redis.multidb.config import DEFAULT_AUTO_FALLBACK_INTERVAL
from redis.multidb.database import Database, AbstractDatabase, Databases
from redis.multidb.circuit import State as CBState
from redis.multidb.event import RegisterCommandFailure, ActiveDatabaseChanged, ResubscribeOnActiveDatabaseChanged
from redis.multidb.failover import FailoverStrategy
from redis.multidb.failure_detector import FailureDetector


class CommandExecutor(ABC):

    @property
    @abstractmethod
    def failure_detectors(self) -> List[FailureDetector]:
        """Returns a list of failure detectors."""
        pass

    @abstractmethod
    def add_failure_detector(self, failure_detector: FailureDetector) -> None:
        """Adds new failure detector to the list of failure detectors."""
        pass

    @property
    @abstractmethod
    def databases(self) -> Databases:
        """Returns a list of databases."""
        pass

    @property
    @abstractmethod
    def active_database(self) -> Optional[Database]:
        """Returns currently active database."""
        pass

    @active_database.setter
    @abstractmethod
    def active_database(self, database: AbstractDatabase) -> None:
        """Sets currently active database."""
        pass

    @abstractmethod
    def pubsub(self, **kwargs):
        """Initializes a PubSub object on a currently active database"""
        pass

    @property
    @abstractmethod
    def active_pubsub(self) -> Optional[PubSub]:
        """Returns currently active pubsub."""
        pass

    @active_pubsub.setter
    @abstractmethod
    def active_pubsub(self, pubsub: PubSub) -> None:
        """Sets currently active pubsub."""
        pass

    @property
    @abstractmethod
    def failover_strategy(self) -> FailoverStrategy:
        """Returns failover strategy."""
        pass

    @property
    @abstractmethod
    def auto_fallback_interval(self) -> float:
        """Returns auto-fallback interval."""
        pass

    @auto_fallback_interval.setter
    @abstractmethod
    def auto_fallback_interval(self, auto_fallback_interval: float) -> None:
        """Sets auto-fallback interval."""
        pass

    @abstractmethod
    def execute_command(self, *args, **options):
        """Executes a command and returns the result."""
        pass


class DefaultCommandExecutor(CommandExecutor):

    def __init__(
            self,
            failure_detectors: List[FailureDetector],
            databases: Databases,
            failover_strategy: FailoverStrategy,
            event_dispatcher: EventDispatcherInterface,
            auto_fallback_interval: float = DEFAULT_AUTO_FALLBACK_INTERVAL,
    ):
        """
        :param failure_detectors: List of failure detectors.
        :param databases: List of databases.
        :param failover_strategy: Strategy that defines the failover logic.
        :param event_dispatcher: Event dispatcher.
        :param auto_fallback_interval: Interval between fallback attempts. Fallback to a new database according to
        failover_strategy.
        """
        self._failure_detectors = failure_detectors
        self._databases = databases
        self._failover_strategy = failover_strategy
        self._event_dispatcher = event_dispatcher
        self._auto_fallback_interval = auto_fallback_interval
        self._next_fallback_attempt: datetime
        self._active_database: Optional[Database] = None
        self._active_pubsub: Optional[PubSub] = None
        self._active_pubsub_kwargs = {}
        self._setup_event_dispatcher()
        self._schedule_next_fallback()

    @property
    def failure_detectors(self) -> List[FailureDetector]:
        return self._failure_detectors

    def add_failure_detector(self, failure_detector: FailureDetector) -> None:
        self._failure_detectors.append(failure_detector)

    @property
    def databases(self) -> Databases:
        return self._databases

    @property
    def active_database(self) -> Optional[AbstractDatabase]:
        return self._active_database

    @active_database.setter
    def active_database(self, database: AbstractDatabase) -> None:
        old_active = self._active_database
        self._active_database = database

        if old_active is not None and old_active is not database:
            self._event_dispatcher.dispatch(
                ActiveDatabaseChanged(old_active, self._active_database, self, **self._active_pubsub_kwargs)
            )

    @property
    def active_pubsub(self) -> Optional[PubSub]:
        return self._active_pubsub

    @active_pubsub.setter
    def active_pubsub(self, pubsub: PubSub) -> None:
        self._active_pubsub = pubsub

    @property
    def failover_strategy(self) -> FailoverStrategy:
        return self._failover_strategy

    @property
    def auto_fallback_interval(self) -> float:
        return self._auto_fallback_interval

    @auto_fallback_interval.setter
    def auto_fallback_interval(self, auto_fallback_interval: int) -> None:
        self._auto_fallback_interval = auto_fallback_interval

    def execute_command(self, *args, **options):
        """Executes a command and returns the result."""
        def callback(database):
            return database.client.execute_command(*args, **options)

        return self._execute_with_failure_detection(callback, args)

    def execute_pipeline(self, command_stack: tuple):
        """
        Executes a stack of commands in pipeline.
        """
        def callback(database):
            with database.client.pipeline() as pipe:
                for command, options in command_stack:
                    pipe.execute_command(*command, **options)

                return pipe.execute()

        return self._execute_with_failure_detection(callback, command_stack)

    def execute_transaction(self, transaction: Callable[[Pipeline], None], *watches, **options):
        """
        Executes a transaction block wrapped in callback.
        """
        def callback(database):
            return database.client.transaction(transaction, *watches, **options)

        return self._execute_with_failure_detection(callback)

    def pubsub(self, **kwargs):
        def callback(database):
            if self._active_pubsub is None:
                self._active_pubsub = database.client.pubsub(**kwargs)
                self._active_pubsub_kwargs = kwargs
            return None

        return self._execute_with_failure_detection(callback)

    def execute_pubsub_method(self, method_name: str, *args, **kwargs):
        """
        Executes given method on active pub/sub.
        """
        def callback(database):
            method = getattr(self.active_pubsub, method_name)
            return method(*args, **kwargs)

        return self._execute_with_failure_detection(callback, *args)

    def execute_pubsub_run_in_thread(
        self,
        pubsub,
        sleep_time: float = 0.0,
        daemon: bool = False,
        exception_handler: Optional[Callable] = None,
    ) -> "PubSubWorkerThread":
        def callback(database):
            return self._active_pubsub.run_in_thread(
                sleep_time, daemon=daemon, exception_handler=exception_handler, pubsub=pubsub
            )

        return self._execute_with_failure_detection(callback)

    def _execute_with_failure_detection(self, callback: Callable, cmds: tuple = ()):
        """
        Execute a commands execution callback with failure detection.
        """
        if (
                self._active_database is None
                or self._active_database.circuit.state != CBState.CLOSED
                or (
                    self._auto_fallback_interval != DEFAULT_AUTO_FALLBACK_INTERVAL
                    and self._next_fallback_attempt <= datetime.now()
                )
        ):
            self.active_database = self._failover_strategy.database
            self._schedule_next_fallback()

        try:
            return callback(self._active_database)
        except (ConnectionError, TimeoutError, socket.timeout) as e:
            # Register command failure
            self._event_dispatcher.dispatch(OnCommandsFailEvent(cmds, e, self.active_database.client))

            # Retry until failure detector will trigger opening of circuit
            return self._execute_with_failure_detection(callback, cmds)

    def _schedule_next_fallback(self) -> None:
        if self._auto_fallback_interval == DEFAULT_AUTO_FALLBACK_INTERVAL:
            return

        self._next_fallback_attempt = datetime.now() + timedelta(seconds=self._auto_fallback_interval)

    def _setup_event_dispatcher(self):
        """
        Registers necessary listeners.
        """
        failure_listener = RegisterCommandFailure(self._failure_detectors, self._databases)
        resubscribe_listener = ResubscribeOnActiveDatabaseChanged()
        self._event_dispatcher.register_listeners({
            OnCommandsFailEvent: [failure_listener],
            ActiveDatabaseChanged: [resubscribe_listener],
        })