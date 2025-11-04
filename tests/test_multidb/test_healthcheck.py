from unittest.mock import MagicMock, Mock

import pytest

from redis.multidb.database import Database
from redis.http.http_client import HttpError
from redis.multidb.healthcheck import (
    PingHealthCheck,
    LagAwareHealthCheck,
    HealthCheck,
    HealthyAllPolicy,
    UnhealthyDatabaseException,
    HealthyMajorityPolicy,
    HealthyAnyPolicy,
)
from redis.multidb.circuit import State as CBState


@pytest.mark.onlynoncluster
class TestHealthyAllPolicy:
    def test_policy_returns_true_for_all_successful_probes(self):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.return_value = True
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy(3, 0.01)
        assert policy.execute([mock_hc1, mock_hc2], mock_db)
        assert mock_hc1.check_health.call_count == 3
        assert mock_hc2.check_health.call_count == 3

    def test_policy_returns_false_on_first_failed_probe(self):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.side_effect = [True, True, False]
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy(3, 0.01)
        assert not policy.execute([mock_hc1, mock_hc2], mock_db)
        assert mock_hc1.check_health.call_count == 3
        assert mock_hc2.check_health.call_count == 0

    def test_policy_raise_unhealthy_database_exception(self):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.side_effect = [True, True, ConnectionError]
        mock_hc2.check_health.return_value = True
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy(3, 0.01)
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            policy.execute([mock_hc1, mock_hc2], mock_db)
            assert mock_hc1.check_health.call_count == 3
            assert mock_hc2.check_health.call_count == 0


@pytest.mark.onlynoncluster
class TestHealthyMajorityPolicy:
    @pytest.mark.parametrize(
        "probes,hc1_side_effect,hc2_side_effect,hc1_call_count,hc2_call_count,expected_result",
        [
            (3, [True, False, False], [True, True, True], 3, 0, False),
            (3, [True, True, True], [True, False, False], 3, 3, False),
            (3, [True, False, True], [True, True, True], 3, 3, True),
            (3, [True, True, True], [True, False, True], 3, 3, True),
            (3, [True, True, False], [True, False, True], 3, 3, True),
            (4, [True, True, False, False], [True, True, True, True], 4, 0, False),
            (4, [True, True, True, True], [True, True, False, False], 4, 4, False),
            (4, [False, True, True, True], [True, True, True, True], 4, 4, True),
            (4, [True, True, True, True], [True, False, True, True], 4, 4, True),
            (4, [False, True, True, True], [True, True, False, True], 4, 4, True),
        ],
        ids=[
            "HC1 - no majority - odd",
            "HC2 - no majority - odd",
            "HC1 - majority- odd",
            "HC2 - majority - odd",
            "HC1 + HC2 - majority - odd",
            "HC1 - no majority - even",
            "HC2 - no majority - even",
            "HC1 - majority - even",
            "HC2 - majority - even",
            "HC1 + HC2 - majority - even",
        ],
    )
    def test_policy_returns_true_for_majority_successful_probes(
        self,
        probes,
        hc1_side_effect,
        hc2_side_effect,
        hc1_call_count,
        hc2_call_count,
        expected_result,
    ):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyMajorityPolicy(probes, 0.01)
        assert policy.execute([mock_hc1, mock_hc2], mock_db) == expected_result
        assert mock_hc1.check_health.call_count == hc1_call_count
        assert mock_hc2.check_health.call_count == hc2_call_count

    @pytest.mark.parametrize(
        "probes,hc1_side_effect,hc2_side_effect,hc1_call_count,hc2_call_count",
        [
            (3, [True, ConnectionError, ConnectionError], [True, True, True], 3, 0),
            (3, [True, True, True], [True, ConnectionError, ConnectionError], 3, 3),
            (
                4,
                [True, ConnectionError, ConnectionError, True],
                [True, True, True, True],
                3,
                0,
            ),
            (
                4,
                [True, True, True, True],
                [True, ConnectionError, ConnectionError, False],
                4,
                3,
            ),
        ],
        ids=[
            "HC1 - majority- odd",
            "HC2 - majority - odd",
            "HC1 - majority - even",
            "HC2 - majority - even",
        ],
    )
    def test_policy_raise_unhealthy_database_exception_on_majority_probes_exceptions(
        self, probes, hc1_side_effect, hc2_side_effect, hc1_call_count, hc2_call_count
    ):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyAllPolicy(3, 0.01)
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            policy.execute([mock_hc1, mock_hc2], mock_db)
            assert mock_hc1.check_health.call_count == hc1_call_count
            assert mock_hc2.check_health.call_count == hc2_call_count


@pytest.mark.onlynoncluster
class TestHealthyAnyPolicy:
    @pytest.mark.parametrize(
        "hc1_side_effect,hc2_side_effect,hc1_call_count,hc2_call_count,expected_result",
        [
            ([False, False, False], [True, True, True], 3, 0, False),
            ([False, False, True], [False, False, False], 3, 3, False),
            ([False, True, True], [False, False, True], 2, 3, True),
            ([True, True, True], [False, True, False], 1, 2, True),
        ],
        ids=[
            "HC1 - no successful",
            "HC2 - no successful",
            "HC1 - successful",
            "HC2 - successful",
        ],
    )
    def test_policy_returns_true_for_any_successful_probe(
        self,
        hc1_side_effect,
        hc2_side_effect,
        hc1_call_count,
        hc2_call_count,
        expected_result,
    ):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.side_effect = hc1_side_effect
        mock_hc2.check_health.side_effect = hc2_side_effect
        mock_db = Mock(spec=Database)

        policy = HealthyAnyPolicy(3, 0.01)
        assert policy.execute([mock_hc1, mock_hc2], mock_db) == expected_result
        assert mock_hc1.check_health.call_count == hc1_call_count
        assert mock_hc2.check_health.call_count == hc2_call_count

    def test_policy_raise_unhealthy_database_exception_if_exception_occurs_on_failed_health_check(
        self,
    ):
        mock_hc1 = Mock(spec=HealthCheck)
        mock_hc2 = Mock(spec=HealthCheck)
        mock_hc1.check_health.side_effect = [False, False, ConnectionError]
        mock_hc2.check_health.side_effect = [True, True, True]
        mock_db = Mock(spec=Database)

        policy = HealthyAnyPolicy(3, 0.01)
        with pytest.raises(UnhealthyDatabaseException, match="Unhealthy database"):
            policy.execute([mock_hc1, mock_hc2], mock_db)
            assert mock_hc1.check_health.call_count == 3
            assert mock_hc2.check_health.call_count == 0


@pytest.mark.onlynoncluster
class TestPingHealthCheck:
    def test_database_is_healthy_on_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.return_value = "PONG"
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert hc.check_health(db)
        assert mock_client.execute_command.call_count == 1

    def test_database_is_unhealthy_on_incorrect_echo_response(
        self, mock_client, mock_cb
    ):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.return_value = False
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert not hc.check_health(db)
        assert mock_client.execute_command.call_count == 1

    def test_database_close_circuit_on_successful_healthcheck(
        self, mock_client, mock_cb
    ):
        mock_client.execute_command.return_value = "PONG"
        mock_cb.state = CBState.HALF_OPEN
        hc = PingHealthCheck()
        db = Database(mock_client, mock_cb, 0.9)

        assert hc.check_health(db)
        assert mock_client.execute_command.call_count == 1


@pytest.mark.onlynoncluster
class TestLagAwareHealthCheck:
    def test_database_is_healthy_when_bdb_matches_by_dns_name(
        self, mock_client, mock_cb
    ):
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

        hc = LagAwareHealthCheck(rest_api_port=1234, lag_aware_tolerance=150)
        # Inject our mocked http client
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")

        assert hc.check_health(db) is True
        # Base URL must be set correctly
        assert hc._http_client.base_url == "https://healthcheck.example.com:1234"
        # Calls: first to list bdbs, then to availability
        assert mock_http.get.call_count == 2
        first_call = mock_http.get.call_args_list[0]
        second_call = mock_http.get.call_args_list[1]
        assert first_call.args[0] == "/v1/bdbs"
        assert (
            second_call.args[0]
            == "/v1/bdbs/bdb-1/availability?extend_check=lag&availability_lag_tolerance_ms=150"
        )
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

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")

        assert hc.check_health(db) is True
        assert mock_http.get.call_count == 2
        assert (
            mock_http.get.call_args_list[1].args[0]
            == "/v1/bdbs/bdb-42/availability?extend_check=lag&availability_lag_tolerance_ms=5000"
        )

    def test_raises_value_error_when_no_matching_bdb(self, mock_client, mock_cb):
        """
        Ensures health check raises ValueError when there's no bdb matching the database host.
        """
        host = "db2.example.com"
        mock_client.get_connection_kwargs.return_value = {"host": host}

        mock_http = MagicMock()
        # Return bdbs that do not match host by dns_name nor addr
        mock_http.get.return_value = [
            {
                "uid": "a",
                "endpoints": [{"dns_name": "other.example.com", "addr": ["10.0.0.9"]}],
            },
            {
                "uid": "b",
                "endpoints": [
                    {"dns_name": "another.example.com", "addr": ["10.0.0.10"]}
                ],
            },
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")

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
            HttpError(
                url=f"https://{host}:9443/v1/bdbs/bdb-err/availability",
                status=503,
                message="busy",
            ),
        ]

        hc = LagAwareHealthCheck()
        hc._http_client = mock_http

        db = Database(mock_client, mock_cb, 1.0, "https://healthcheck.example.com")

        with pytest.raises(HttpError, match="busy") as e:
            hc.check_health(db)
            assert e.status == 503

        # Ensure both calls were attempted
        assert mock_http.get.call_count == 2
