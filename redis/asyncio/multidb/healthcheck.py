import asyncio
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Optional, Tuple, Union

from redis.asyncio import Connection, ConnectionPool
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.http.http_client import DEFAULT_TIMEOUT, AsyncHTTPClientWrapper
from redis.backoff import NoBackoff
from redis.client import Redis as SyncRedis
from redis.http.http_client import HttpClient
from redis.multidb.exception import UnhealthyDatabaseException
from redis.retry import Retry

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
    async def check_health(self, database, connection: Connection) -> bool:
        """Function to determine the health status."""
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
    async def get_connections(self, database) -> list[ConnectionPool]:
        """Get connections to the database."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close all connections to the database."""
        pass


class AbstractHealthCheckPolicy(HealthCheckPolicy):
    """
    Abstract health check policy.
    """

    def __init__(self):
        self._connections: dict[int, list[ConnectionPool]] = {}  # keyed by database id

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

    async def get_connections(self, database) -> list[ConnectionPool]:
        db_id = id(database)
        conns = self._connections.get(db_id)

        if conns is None:
            # Check for both sync and async standalone Redis clients
            if isinstance(database.client, (AsyncRedis, SyncRedis)):
                conn_kwargs = database.client.get_connection_kwargs()
                conns = [ConnectionPool(**conn_kwargs)]
            else:
                # Cluster client - get connections for all nodes
                conns = []
                all_nodes = database.client.get_nodes()
                for node in all_nodes:
                    conn_kwargs = node.redis_connection.get_connection_kwargs()
                    conns.append(ConnectionPool(**conn_kwargs))
            self._connections[db_id] = conns

        return conns

    async def close(self) -> None:
        disc_tasks = []

        for db_id in self._connections:
            for pool in self._connections[db_id]:
                disc_tasks.append(asyncio.create_task(pool.disconnect()))

        await asyncio.gather(
            *disc_tasks,
            return_exceptions=True,
        )
        self._connections.clear()

    @abstractmethod
    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        pass


async def _check_health(
    database, pool: ConnectionPool, health_check: HealthCheck
) -> bool:
    conn = await pool.get_connection()
    try:
        if not await health_check.check_health(database, conn):
            return False
        return True
    except BaseException:
        await conn.disconnect()
        raise
    finally:
        await pool.release(conn)


class HealthyAllPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if all health check probes are successful.
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        conns = await self.get_connections(database)
        probes = health_check.health_check_probes
        for attempt in range(probes):
            results = await asyncio.gather(
                *[_check_health(database, pool, health_check) for pool in conns],
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, Exception):
                    raise result
                if not result:
                    return False

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)
        return True


class HealthyMajorityPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if a majority of health check probes are successful.
    A probe is successful only if all nodes (connections) pass the health check.
    """

    async def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        probes = health_check.health_check_probes
        allowed_unsuccessful_probes = (probes - 1) // 2
        conns = await self.get_connections(database)
        last_exception = None

        for attempt in range(probes):
            results = await asyncio.gather(
                *[_check_health(database, pool, health_check) for pool in conns],
                return_exceptions=True,
            )

            # A probe is successful only if ALL nodes pass (no exceptions, all True)
            probe_failed = False
            for result in results:
                if isinstance(result, Exception):
                    probe_failed = True
                    last_exception = result
                    break
                elif not result:
                    probe_failed = True
                    break

            if probe_failed:
                allowed_unsuccessful_probes -= 1
                if allowed_unsuccessful_probes < 0:
                    # Majority of probes have failed
                    if last_exception:
                        raise last_exception
                    return False

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        return True


class HealthyAnyPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if at least one health check probe is successful.
    """

    async def _execute(self, health_check: HealthCheck, database):
        """
        Executes health check against given database.
        """
        probes = health_check.health_check_probes
        last_exception = None
        is_healthy = False
        conns = await self.get_connections(database)

        for attempt in range(probes):
            results = await asyncio.gather(
                *[_check_health(database, pool, health_check) for pool in conns],
                return_exceptions=True,
            )
            # Check if all results are True (not False and not Exception)
            if all(r is True for r in results):
                is_healthy = True
                break
            # Store any exception for later
            for r in results:
                if isinstance(r, Exception):
                    last_exception = r
                    break

            if attempt < probes - 1:
                await asyncio.sleep(health_check.health_check_delay)

        # If all probes failed and we have an exception, raise it
        if not is_healthy and last_exception:
            raise last_exception

        return is_healthy


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
    async def check_health(self, database, connection: Connection) -> bool:
        pass


class PingHealthCheck(AbstractHealthCheck):
    """
    Health check based on PING command.
    """

    async def check_health(self, database, connection: Connection) -> bool:
        await connection.send_command("PING")
        response = await connection.read_response()
        return response in (b"PONG", "PONG")


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

    async def check_health(self, database, connection: Connection) -> bool:
        if database.health_check_url is None:
            raise ValueError(
                "Database health check url is not set. Please check DatabaseConfig for the current database."
            )

        if isinstance(database.client, (AsyncRedis, SyncRedis)):
            db_host = database.client.get_connection_kwargs()["host"]
        else:
            # Cluster client
            db_host = database.client.startup_nodes[0].host

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
