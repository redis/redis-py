import logging
from abc import abstractmethod, ABC
from typing import Optional, Tuple, Union

from redis import Redis
from redis.backoff import ExponentialWithJitterBackoff
from redis.http.http_client import DEFAULT_TIMEOUT, HttpClient
from redis.retry import Retry
from redis.utils import dummy_fail

DEFAULT_HEALTH_CHECK_RETRIES = 3
DEFAULT_HEALTH_CHECK_BACKOFF = ExponentialWithJitterBackoff(cap=10)

logger = logging.getLogger(__name__)

class HealthCheck(ABC):

    @property
    @abstractmethod
    def retry(self) -> Retry:
        """The retry object to use for health checks."""
        pass

    @abstractmethod
    def check_health(self, database) -> bool:
        """Function to determine the health status."""
        pass

class AbstractHealthCheck(HealthCheck):
    def __init__(
            self,
            retry: Retry = Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)
    ) -> None:
        self._retry = retry
        self._retry.update_supported_errors([ConnectionRefusedError])

    @property
    def retry(self) -> Retry:
        return self._retry

    @abstractmethod
    def check_health(self, database) -> bool:
        pass


class EchoHealthCheck(AbstractHealthCheck):
    def __init__(
        self,
        retry: Retry = Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF)
    ) -> None:
        """
        Check database healthiness by sending an echo request.
        """
        super().__init__(
            retry=retry,
        )
    def check_health(self, database) -> bool:
        return self._retry.call_with_retry(
            lambda: self._returns_echoed_message(database),
            lambda _: dummy_fail()
        )

    def _returns_echoed_message(self, database) -> bool:
        expected_message = ["healthcheck", b"healthcheck"]

        if isinstance(database.client, Redis):
            actual_message = database.client.execute_command("ECHO" ,"healthcheck")
            return actual_message in expected_message
        else:
            # For a cluster checks if all nodes are healthy.
            all_nodes = database.client.get_nodes()
            for node in all_nodes:
                actual_message = node.redis_connection.execute_command("ECHO" ,"healthcheck")

                if actual_message not in expected_message:
                    return False

            return True

class LagAwareHealthCheck(AbstractHealthCheck):
    """
    Health check available for Redis Enterprise deployments.
    Verify via REST API that the database is healthy based on different lags.
    """
    def __init__(
        self,
        retry: Retry = Retry(retries=DEFAULT_HEALTH_CHECK_RETRIES, backoff=DEFAULT_HEALTH_CHECK_BACKOFF),
        rest_api_port: int = 9443,
        lag_aware_tolerance: int = 100,
        timeout: float = DEFAULT_TIMEOUT,
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
    ):
        """
        Initialize LagAwareHealthCheck with the specified parameters.

        Args:
            retry: Retry configuration for health checks
            rest_api_port: Port number for Redis Enterprise REST API (default: 9443)
            lag_aware_tolerance: Tolerance in lag between databases in MS (default: 100)
            timeout: Request timeout in seconds (default: DEFAULT_TIMEOUT)
            auth_basic: Tuple of (username, password) for basic authentication
            verify_tls: Whether to verify TLS certificates (default: True)
            ca_file: Path to CA certificate file for TLS verification
            ca_path: Path to CA certificates directory for TLS verification
            ca_data: CA certificate data as string or bytes
            client_cert_file: Path to client certificate file for mutual TLS
            client_key_file: Path to client private key file for mutual TLS
            client_key_password: Password for encrypted client private key
        """
        super().__init__(
            retry=retry,
        )
        self._http_client = HttpClient(
            timeout=timeout,
            auth_basic=auth_basic,
            retry=self.retry,
            verify_tls=verify_tls,
            ca_file=ca_file,
            ca_path=ca_path,
            ca_data=ca_data,
            client_cert_file=client_cert_file,
            client_key_file=client_key_file,
            client_key_password=client_key_password
        )
        self._rest_api_port = rest_api_port
        self._lag_aware_tolerance = lag_aware_tolerance

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
                if endpoint['dns_name'] == db_host:
                    matching_bdb = bdb
                    break

                # In case if the host was set as public IP
                for addr in endpoint['addr']:
                    if addr == db_host:
                        matching_bdb = bdb
                        break

        if matching_bdb is None:
            logger.warning("LagAwareHealthCheck failed: Couldn't find a matching bdb")
            raise ValueError("Could not find a matching bdb")

        url = (f"/v1/bdbs/{matching_bdb['uid']}/availability"
               f"?extend_check=lag&availability_lag_tolerance_ms={self._lag_aware_tolerance}")
        self._http_client.get(url, expect_json=False)

        # Status checked in an http client, otherwise HttpError will be raised
        return True
