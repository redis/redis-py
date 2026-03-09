import logging
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from time import sleep
from typing import List, Optional, Tuple, Union

from redis import Redis
from redis.backoff import NoBackoff
from redis.http.http_client import DEFAULT_TIMEOUT, HttpClient
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
    def check_health(self, database) -> bool:
        """Function to determine the health status."""
        pass


class HealthCheckPolicy(ABC):
    """
    Health checks execution policy.
    """

    @abstractmethod
    def execute(self, health_checks: List[HealthCheck], database) -> bool:
        """Execute health checks and return database health status."""
        pass

    @abstractmethod
    def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        pass


class AbstractHealthCheckPolicy(HealthCheckPolicy):
    """
    Abstract health check policy.
    """

    def execute(self, health_checks: List[HealthCheck], database) -> bool:
        with ThreadPoolExecutor(max_workers=len(health_checks)) as executor:
            futures = {
                executor.submit(self._execute, health_check, database): health_check
                for health_check in health_checks
            }

            for future, health_check in futures.items():
                try:
                    result = future.result(timeout=health_check.health_check_timeout)

                    if not result:
                        return False
                except Exception as e:
                    raise UnhealthyDatabaseException("Unhealthy database", database, e)

        return True

    @abstractmethod
    def _execute(self, health_check: HealthCheck, database):
        """
        Executes health check against given database.
        """
        pass


class HealthyAllPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if all health check probes are successful.
    """

    def _execute(self, health_check: HealthCheck, database) -> bool:
        probes = health_check.health_check_probes
        for attempt in range(probes):
            try:
                if not health_check.check_health(database):
                    return False
            except Exception as e:
                raise UnhealthyDatabaseException("Unhealthy database", database, e)

            if attempt < probes - 1:
                sleep(health_check.health_check_delay)
        return True


class HealthyMajorityPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if a majority of health check probes are successful.
    """

    def _execute(self, health_check: HealthCheck, database) -> bool:
        """
        Executes health check against given database.
        """
        probes = health_check.health_check_probes
        allowed_unsuccessful_probes = (probes - 1) // 2

        for attempt in range(probes):
            try:
                if not health_check.check_health(database):
                    allowed_unsuccessful_probes -= 1
                    if allowed_unsuccessful_probes < 0:
                        return False
            except Exception as e:
                allowed_unsuccessful_probes -= 1
                if allowed_unsuccessful_probes < 0:
                    raise UnhealthyDatabaseException("Unhealthy database", database, e)

            if attempt < probes - 1:
                sleep(health_check.health_check_delay)

        return True


class HealthyAnyPolicy(AbstractHealthCheckPolicy):
    """
    Policy that returns True if at least one health check probe is successful.
    """

    def _execute(self, health_check: HealthCheck, database):
        """
        Executes health check against given database.
        """
        probes = health_check.health_check_probes
        exception = None
        is_healthy = False

        for attempt in range(probes):
            try:
                if health_check.check_health(database):
                    is_healthy = True
                    break
            except Exception as e:
                exception = e

            if attempt < probes - 1:
                sleep(health_check.health_check_delay)

        if not is_healthy and exception:
            raise UnhealthyDatabaseException("Unhealthy database", database, exception)

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
    def check_health(self, database) -> bool:
        pass


class PingHealthCheck(AbstractHealthCheck):
    """
    Health check based on PING command.
    """

    def check_health(self, database) -> bool:
        if isinstance(database.client, Redis):
            return database.client.execute_command("PING")
        else:
            # For a cluster checks if all nodes are healthy.
            all_nodes = database.client.get_nodes()
            for node in all_nodes:
                if not node.redis_connection.execute_command("PING"):
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
        self._http_client = HttpClient(
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
        self._rest_api_port = rest_api_port
        self._lag_aware_tolerance = lag_aware_tolerance
        super().__init__(
            health_check_probes=health_check_probes,
            health_check_delay=health_check_delay,
            health_check_timeout=health_check_timeout,
        )

    def check_health(self, database) -> bool:
        if database.health_check_url is None:
            raise ValueError(
                "Database health check url is not set. Please check DatabaseConfig for the current database."
            )

        if isinstance(database.client, Redis):
            db_host = database.client.get_connection_kwargs()["host"]
        else:
            db_host = database.client.startup_nodes[0].host

        base_url = f"{database.health_check_url}:{self._rest_api_port}"
        self._http_client.base_url = base_url

        # Find bdb matching to the current database host
        matching_bdb = None
        for bdb in self._http_client.get("/v1/bdbs"):
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
        self._http_client.get(url, expect_json=False)

        # Status checked in an http client, otherwise HttpError will be raised
        return True
