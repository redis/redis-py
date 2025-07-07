from redis.backoff import ExponentialBackoff
from redis.multidb.database import Database, State
from redis.multidb.healthcheck import EchoHealthCheck
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
        db = Database(mock_client, mock_cb, 0.9, State.ACTIVE)

        assert hc.check_health(db) == True
        assert mock_client.execute_command.call_count == 3

    def test_database_is_unhealthy_on_incorrect_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'wrong']
        hc = EchoHealthCheck(Retry(backoff=ExponentialBackoff(cap=1.0), retries=3))
        db = Database(mock_client, mock_cb, 0.9, State.ACTIVE)

        assert hc.check_health(db) == False
        assert mock_client.execute_command.call_count == 3

    def test_database_close_circuit_on_successful_healthcheck(self, mock_client, mock_cb):
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'healthcheck']
        mock_cb.state = CBState.HALF_OPEN
        hc = EchoHealthCheck(Retry(backoff=ExponentialBackoff(cap=1.0), retries=3))
        db = Database(mock_client, mock_cb, 0.9, State.ACTIVE)

        assert hc.check_health(db) == True
        assert mock_client.execute_command.call_count == 3