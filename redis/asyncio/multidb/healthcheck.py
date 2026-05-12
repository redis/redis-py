import asyncio
import inspect
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Tuple, Type, Union

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.asyncio.http.http_client import DEFAULT_TIMEOUT, AsyncHTTPClientWrapper
from redis.backoff import NoBackoff
from redis.client import Redis as SyncRedis
from redis.cluster import RedisCluster as SyncRedisCluster
from redis.http.http_client import HttpClient
from redis.multidb.exception import UnhealthyDatabaseException
from redis.retry import Retry

# Type alias for async Redis clients (standalone or cluster)
AsyncRedisClientT = Union[AsyncRedis, AsyncRedisCluster]


def _get_init_params(cls: Type) -> frozenset:
    """Extract parameter names from a class's __init__ method."""
    sig = inspect.signature(cls.__init__)
    return frozenset(
        name
        for name, param in sig.parameters.items()
        if name != "self"
        and param.kind
        in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    )


def _filter_kwargs(kwargs: dict, cls: Type) -> dict:
    """Filter kwargs to only include parameters accepted by the class's __init__."""
    allowed = _get_init_params(cls)
    return {k: v for k, v in kwargs.items() if k in allowed}


DEFAULT_HEALTH_CHECK_PROBES = 3
DEFAULT_HEALTH_CHECK_INTERVAL = 5
DEFAULT_HEALTH_CHECK_TIMEOUT = 3
DEFAULT_HEALTH_CHECK_DELAY = 0.5
DEFAULT_LAG_AWARE_TOLERANCE = 5000

logger = logging.getLogger(__name__)


class HealthCheck(ABC):
    """
    Health check interface.
    """

    @property
    @abstractmethod
    def health_check_probes(self) -> int:
        """Number of probes to execute health checks."""
        pass

    @property
    @abstractmethod
    def health_check_delay(self) -> float:
        """Delay between health check probes."""
        pass

    @property
    @abstractmethod
    def health_check_timeout(self) -> float:
        """Timeout for the full health check operation (including all probes)."""
        pass

    @abstractmethod
    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        """
        Function to determine the health status.

        Args:
            database: The database being checked
            hc_client: A Redis client (AsyncRedis or AsyncRedisCluster) to use for
                health checks. This client follows topology changes automatically.

        Returns:
            True if the database is healthy, False otherwise.
        """
        pass


class HealthCheckPolicy(ABC):
    """
    Health checks execution policy.
    """

    @abstractmethod
    async def execute(self, health_checks: List[HealthCheck], database) -> bool:
        """Execute health checks and return database health status."""
        pass

    @abstractmethod
    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        pass

    @abstractmethod
    async def get_client(self, database) -> AsyncRedisClientT:
        """
        Get a health check client for the database.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close all health check clients."""
        pass


class AbstractHealthCheckPolicy(HealthCheckPolicy):
    """
    Abstract health check policy.
    """

    def __init__(self):
        # Single client per database, keyed by database id
        self._clients: dict[int, AsyncRedisClientT] = {}

    async def execute(self, health_checks: List[HealthCheck], database) -> bool:
        """
        Execute all health checks concurrently with individual timeouts.
        Each health check runs with its own timeout, and all run in parallel.

        All exception handling is centralized here - _execute() methods just
        propagate exceptions naturally.
        """

        # Create wrapper tasks that apply individual timeouts
        async def execute_with_timeout(health_check: HealthCheck):
            return await asyncio.wait_for(
                self._execute(health_check, database),
                timeout=health_check.health_check_timeout,
            )

        # Run all health checks concurrently and collect results/exceptions
        results = await asyncio.gather(
            *[execute_with_timeout(hc) for hc in health_checks],
            return_exceptions=True,
        )

        # Check results - handle exceptions and failures
        for result in results:
            if isinstance(result, Exception):
                # Any exception (including TimeoutError) makes the database unhealthy
                raise UnhealthyDatabaseException("Unhealthy database", database, result)
            elif not result:
                # Health check returned False
                return False

        return True

    async def get_client(self, database) -> AsyncRedisClientT:
        """
        Get or create a health check client for the database.

        Creates a single client instance per database that follows topology
        changes automatically. For cluster databases, the client handles
        node discovery and slot mapping internally.
        """
        db_id = id(database)
        client = self._clients.get(db_id)

        if client is None:
            # Check for both sync and async standalone Redis clients
            if isinstance(database.client, (AsyncRedis, SyncRedis)):
                conn_kwargs = database.client.get_connection_kwargs()
                filtered_kwargs = _filter_kwargs(conn_kwargs, AsyncRedis)
                client = AsyncRedis(**filtered_kwargs)
            elif isinstance(database.client, (AsyncRedisCluster, SyncRedisCluster)):
                # Cluster client - create a single cluster client that handles
                # topology changes internally
                conn_kwargs = database.client.get_connection_kwargs().copy()
                filtered_kwargs = _filter_kwargs(conn_kwargs, AsyncRedisCluster)
                startup_nodes = database.client.startup_nodes
                # Use the first node as the startup node
                if startup_nodes:
                    first_node = startup_nodes[0]
                    client = AsyncRedisCluster(
                        host=first_node.host,
                        port=first_node.port,
                        dynamic_startup_nodes=database.client.nodes_manager._dynamic_startup_nodes,
                        address_remap=database.client.nodes_manager.address_remap,
                        require_full_coverage=database.client.nodes_manager._require_full_coverage,
                        retry=database.client.retry,
                        **filtered_kwargs,
                    )
                else:
                    raise ValueError(
                        "Cluster client has no nodes - cannot create health check client"
                    )
            else:
                raise TypeError(f"Unsupported client type: {type(database.client)}")
            self._clients[db_id] = client

        return client

    async def close(self) -> None:
        """Close all health check clients."""
        close_tasks = [
            asyncio.create_task(client.aclose()) for client in self._clients.values()
        ]

        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)

        self._clients.clear()

    @abstractmethod
    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        pass


class HealthyAllPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if all health check probes are successful.
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.

        Uses a single client that handles topology changes automatically.
        """
        client = await self.get_client(database)
        probes = health_check.health_check_probes

        for attempt in range(probes):
            result = await health_check.check_health(database, client)
            if not result:
                return False

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        return True


class HealthyMajorityPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if a majority of health check probes are successful.

    Majority means more than half must pass:
    - 3 probes: need 2+ to pass (1 failure allowed)
    - 4 probes: need 3+ to pass (1 failure allowed, tie = unhealthy)
    - 5 probes: need 3+ to pass (2 failures allowed)
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.

        Uses a single client that handles topology changes automatically.
        """
        probes = health_check.health_check_probes
        # Strict majority: more than half must pass
        # (probes - 1) // 2 gives the max allowed failures
        allowed_unsuccessful_probes = (probes - 1) // 2
        client = await self.get_client(database)
        last_exception = None

        for attempt in range(probes):
            try:
                result = await health_check.check_health(database, client)
                if not result:
                    # Probe failed (returned False)
                    allowed_unsuccessful_probes -= 1
                    if allowed_unsuccessful_probes < 0:
                        return False
            except Exception as e:
                # Probe failed (exception)
                last_exception = e
                allowed_unsuccessful_probes -= 1
                if allowed_unsuccessful_probes < 0:
                    raise last_exception

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        return True


class HealthyAnyPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if at least one health check probe is successful.
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.

        Uses a single client that handles topology changes automatically.
        """
        probes = health_check.health_check_probes
        last_exception = None
        client = await self.get_client(database)

        for attempt in range(probes):
            try:
                result = await health_check.check_health(database, client)
                if result:
                    # At least one probe succeeded
                    return True
            except Exception as e:
                last_exception = e

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        # All probes failed
        if last_exception:
            raise last_exception

        return False


class HealthCheckPolicies(Enum):
    HEALTHY_ALL = HealthyAllPolicy
    HEALTHY_MAJORITY = HealthyMajorityPolicy
    HEALTHY_ANY = HealthyAnyPolicy


DEFAULT_HEALTH_CHECK_POLICY: HealthCheckPolicies = HealthCheckPolicies.HEALTHY_ALL


class AbstractHealthCheck(HealthCheck):
    def __init__(
        self,
        health_check_probes: int = DEFAULT_HEALTH_CHECK_PROBES,
        health_check_delay: float = DEFAULT_HEALTH_CHECK_DELAY,
        health_check_timeout: float = DEFAULT_HEALTH_CHECK_TIMEOUT,
    ):
        if health_check_probes < 1:
            raise ValueError("health_check_probes must be greater than 0")
        self._health_check_probes = health_check_probes
        self._health_check_delay = health_check_delay
        self._health_check_timeout = health_check_timeout

    @property
    def health_check_probes(self) -> int:
        return self._health_check_probes

    @property
    def health_check_delay(self) -> float:
        return self._health_check_delay

    @property
    def health_check_timeout(self) -> float:
        return self._health_check_timeout

    @abstractmethod
    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        pass


class PingHealthCheck(AbstractHealthCheck):
    """
    Health check based on PING command.
    """

    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        if isinstance(hc_client, AsyncRedis):
            return await hc_client.execute_command("PING")
        else:
            # For a cluster checks if all nodes are healthy.
            all_nodes = hc_client.get_nodes()
            for node in all_nodes:
                if not await node.redis_connection.execute_command("PING"):
                    return False

            return True


class LagAwareHealthCheck(AbstractHealthCheck):
    """
    Health check available for Redis Enterprise deployments.
    Verify via REST API that the database is healthy based on different lags.
    """

    def __init__(
        self,
        rest_api_port: int = 9443,
        lag_aware_tolerance: int = DEFAULT_LAG_AWARE_TOLERANCE,
        http_timeout: float = DEFAULT_TIMEOUT,
        auth_basic: Optional[Tuple[str, str]] = None,
        verify_tls: bool = True,
        # TLS verification (server) options
        ca_file: Optional[str] = None,
        ca_path: Optional[str] = None,
        ca_data: Optional[Union[str, bytes]] = None,
        # Mutual TLS (client cert) options
        client_cert_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        client_key_password: Optional[str] = None,
        # Health check configuration
        health_check_probes: int = DEFAULT_HEALTH_CHECK_PROBES,
        health_check_delay: float = DEFAULT_HEALTH_CHECK_DELAY,
        health_check_timeout: float = DEFAULT_HEALTH_CHECK_TIMEOUT,
    ):
        """
        Initialize LagAwareHealthCheck with the specified parameters.

        Args:
            rest_api_port: Port number for Redis Enterprise REST API (default: 9443)
            lag_aware_tolerance: Tolerance in lag between databases in MS (default: 100)
            http_timeout: Request timeout in seconds (default: DEFAULT_TIMEOUT)
            auth_basic: Tuple of (username, password) for basic authentication
            verify_tls: Whether to verify TLS certificates (default: True)
            ca_file: Path to CA certificate file for TLS verification
            ca_path: Path to CA certificates directory for TLS verification
            ca_data: CA certificate data as string or bytes
            client_cert_file: Path to client certificate file for mutual TLS
            client_key_file: Path to client private key file for mutual TLS
            client_key_password: Password for encrypted client private key
        """
        self._http_client = AsyncHTTPClientWrapper(
            HttpClient(
                timeout=http_timeout,
                auth_basic=auth_basic,
                retry=Retry(NoBackoff(), retries=0),
                verify_tls=verify_tls,
                ca_file=ca_file,
                ca_path=ca_path,
                ca_data=ca_data,
                client_cert_file=client_cert_file,
                client_key_file=client_key_file,
                client_key_password=client_key_password,
            )
        )
        self._rest_api_port = rest_api_port
        self._lag_aware_tolerance = lag_aware_tolerance
        super().__init__(
            health_check_probes=health_check_probes,
            health_check_delay=health_check_delay,
            health_check_timeout=health_check_timeout,
        )

    async def check_health(self, database, hc_client: AsyncRedisClientT) -> bool:
        """
        Check database health via Redis Enterprise REST API.

        Note: The client parameter is not used for this health check as it
        relies on the REST API instead of Redis protocol. The client is
        accepted for interface compatibility.
        """
        if database.health_check_url is None:
            raise ValueError(
                "Database health check url is not set. Please check DatabaseConfig for the current database."
            )

        if isinstance(database.client, (AsyncRedis, SyncRedis)):
            db_host = database.client.get_connection_kwargs()["host"]
        else:
            # Cluster client
            db_host = database.client.get_nodes()[0].host

        base_url = f"{database.health_check_url}:{self._rest_api_port}"
        self._http_client.client.base_url = base_url

        # Find bdb matching to the current database host
        matching_bdb = None
        for bdb in await self._http_client.get("/v1/bdbs"):
            for endpoint in bdb["endpoints"]:
                if endpoint["dns_name"] == db_host:
                    matching_bdb = bdb
                    break

                # In case if the host was set as public IP
                for addr in endpoint["addr"]:
                    if addr == db_host:
                        matching_bdb = bdb
                        break

        if matching_bdb is None:
            logger.warning("LagAwareHealthCheck failed: Couldn't find a matching bdb")
            raise ValueError("Could not find a matching bdb")

        url = (
            f"/v1/bdbs/{matching_bdb['uid']}/availability"
            f"?extend_check=lag&availability_lag_tolerance_ms={self._lag_aware_tolerance}"
        )
        await self._http_client.get(url, expect_json=False)

        # Status checked in an http client, otherwise HttpError will be raised
        return True
