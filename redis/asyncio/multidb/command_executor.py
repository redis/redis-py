from abc import abstractmethod
from typing import List, Optional, Callable, Any

from redis.asyncio.client import PubSub, Pipeline
from redis.asyncio.multidb.database import Databases, AsyncDatabase
from redis.asyncio.multidb.failover import AsyncFailoverStrategy
from redis.asyncio.multidb.failure_detector import AsyncFailureDetector
from redis.asyncio.retry import Retry
from redis.multidb.command_executor import CommandExecutor


class AsyncCommandExecutor(CommandExecutor):

    @property
    @abstractmethod
    def databases(self) -> Databases:
        """Returns a list of databases."""
        pass

    @property
    @abstractmethod
    def failure_detectors(self) -> List[AsyncFailureDetector]:
        """Returns a list of failure detectors."""
        pass

    @abstractmethod
    def add_failure_detector(self, failure_detector: AsyncFailureDetector) -> None:
        """Adds a new failure detector to the list of failure detectors."""
        pass

    @property
    @abstractmethod
    def active_database(self) -> Optional[AsyncDatabase]:
        """Returns currently active database."""
        pass

    @active_database.setter
    @abstractmethod
    def active_database(self, database: AsyncDatabase) -> None:
        """Sets the currently active database."""
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
    def failover_strategy(self) -> AsyncFailoverStrategy:
        """Returns failover strategy."""
        pass

    @property
    @abstractmethod
    def command_retry(self) -> Retry:
        """Returns command retry object."""
        pass

    @abstractmethod
    async def pubsub(self, **kwargs):
        """Initializes a PubSub object on a currently active database"""
        pass

    @abstractmethod
    async def execute_command(self, *args, **options):
        """Executes a command and returns the result."""
        pass

    @abstractmethod
    async def execute_pipeline(self, command_stack: tuple):
        """Executes a stack of commands in pipeline."""
        pass

    @abstractmethod
    async def execute_transaction(self, transaction: Callable[[Pipeline], None], *watches, **options):
        """Executes a transaction block wrapped in callback."""
        pass

    @abstractmethod
    def execute_pubsub_method(self, method_name: str, *args, **kwargs):
        """Executes a given method on active pub/sub."""
        pass

    @abstractmethod
    def execute_pubsub_run(self, sleep_time: float, **kwargs) -> Any:
        """Executes pub/sub run in a thread."""
        pass