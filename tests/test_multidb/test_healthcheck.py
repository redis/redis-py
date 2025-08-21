from unittest.mock import MagicMock

import pytest

from redis.backoff import ExponentialBackoff
from redis.multidb.database import Database
from redis.multidb.healthcheck import EchoHealthCheck
from redis.http.http_client import HttpError
from redis.multidb.healthcheck import EchoHealthCheck, LagAwareHealthCheck
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError
from redis.retry import Retry


class TestEchoHealthCheck:
    def test_database_is_healthy_on_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'healthcheck']
        hc = EchoHealthCheck(Retry(backoff=ExponentialBackoff(cap=1.0), retries=3))
        db = Database(mock_client, mock_cb, 0.9)

        assert hc.check_health(db) == True
        assert mock_client.execute_command.call_count == 3

    def test_database_is_unhealthy_on_incorrect_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'wrong']
        hc = EchoHealthCheck(Retry(backoff=ExponentialBackoff(cap=1.0), retries=3))
        db = Database(mock_client, mock_cb, 0.9)

        assert hc.check_health(db) == False
        assert mock_client.execute_command.call_count == 3

    def test_database_close_circuit_on_successful_healthcheck(self, mock_client, mock_cb):
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'healthcheck']
        mock_cb.state = CBState.HALF_OPEN
        hc = EchoHealthCheck(Retry(backoff=ExponentialBackoff(cap=1.0), retries=3))
        db = Database(mock_client, mock_cb, 0.9)

        assert hc.check_health(db) == True
        assert mock_client.execute_command.call_count == 3


class TestLagAwareHealthCheck:
    def test_database_is_healthy_when_bdb_matches_by_dns_name(self, mock_client, mock_cb):
        """
        Ensures health check succeeds when /v1/bdbs contains an endpoint whose dns_name
        matches database host, and availability endpoint returns success.
        """
        host = "db1.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        # Mock HttpClient used inside LagAwareHealthCheck
        mock_http = MagicMock()
        mock_http.get.side_effect = [
            # First call: list of bdbs
            [
                {
                    "uid": "bdb-1",
                    "endpoints": [
                        {"dns_name": host, "addr": ["10.0.0.1", "10.0.0.2"]},
                    ],
                }
            ],
            # Second call: availability check (no JSON expected)
            None,
        ]

        hc = LagAwareHealthCheck(
            retry=Retry(backoff=ExponentialBackoff(cap=1.0), retries=3),
            rest_api_port=1234,
        )
        # Inject our mocked http client
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0)

        assert hc.check_health(db) is True
        # Base URL must be set correctly
        assert hc._http_client.base_url == f"https://{host}:1234"
        # Calls: first to list bdbs, then to availability
        assert mock_http.get.call_count == 2
        first_call = mock_http.get.call_args_list[0]
        second_call = mock_http.get.call_args_list[1]
        assert first_call.args[0] == "/v1/bdbs"
        assert second_call.args[0] == "/v1/bdbs/bdb-1/availability"
        assert second_call.kwargs.get("expect_json") is False

    def test_database_is_healthy_when_bdb_matches_by_addr(self, mock_client, mock_cb):
        """
        Ensures health check succeeds when endpoint addr list contains the database host.
        """
        host_ip = "203.0.113.5"
        mock_client.get_connection_kwargs.return_value = {"host": host_ip}

        mock_http = MagicMock()
        mock_http.get.side_effect = [
            [
                {
                    "uid": "bdb-42",
                    "endpoints": [
                        {"dns_name": "not-matching.example.com", "addr": [host_ip]},
                    ],
                }
            ],
            None,
        ]

        hc = LagAwareHealthCheck(
            retry=Retry(backoff=ExponentialBackoff(cap=1.0), retries=3),
        )
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0)

        assert hc.check_health(db) is True
        assert mock_http.get.call_count == 2
        assert mock_http.get.call_args_list[1].args[0] == "/v1/bdbs/bdb-42/availability"

    def test_raises_value_error_when_no_matching_bdb(self, mock_client, mock_cb):
        """
        Ensures health check raises ValueError when there's no bdb matching the database host.
        """
        host = "db2.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = MagicMock()
        # Return bdbs that do not match host by dns_name nor addr
        mock_http.get.return_value = [
            {"uid": "a", "endpoints": [{"dns_name": "other.example.com", "addr": ["10.0.0.9"]}]},
            {"uid": "b", "endpoints": [{"dns_name": "another.example.com", "addr": ["10.0.0.10"]}]},
        ]

        hc = LagAwareHealthCheck(
            retry=Retry(backoff=ExponentialBackoff(cap=1.0), retries=3),
        )
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0)

        with pytest.raises(ValueError, match="Could not find a matching bdb"):
            hc.check_health(db)

        # Only the listing call should have happened
        mock_http.get.assert_called_once_with("/v1/bdbs")

    def test_propagates_http_error_from_availability(self, mock_client, mock_cb):
        """
        Ensures that any HTTP error raised by the availability endpoint is propagated.
        """
        host = "db3.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = MagicMock()
        # First: list bdbs -> match by dns_name
        mock_http.get.side_effect = [
            [{"uid": "bdb-err", "endpoints": [{"dns_name": host, "addr": []}]}],
            # Second: availability -> raise HttpError
            HttpError(url=f"https://{host}:9443/v1/bdbs/bdb-err/availability", status=503, message="busy"),
        ]

        hc = LagAwareHealthCheck(
            retry=Retry(backoff=ExponentialBackoff(cap=1.0), retries=3),
        )
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0)

        with pytest.raises(HttpError, match="busy") as e:
            hc.check_health(db)
            assert e.status == 503

        # Ensure both calls were attempted
        assert mock_http.get.call_count == 2