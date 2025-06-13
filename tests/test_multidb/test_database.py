from redis.backoff import ExponentialBackoff
from redis.multidb.database import Database, State
from redis.multidb.healthcheck import EchoHealthCheck
from redis.multidb.circuit import State as CBState
from redis.exceptions import ConnectionError


class TestDatabase:
    def test_database_is_healthy_on_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'healthcheck']
        mock_cb.state = CBState.CLOSED
        hc = EchoHealthCheck(
            check_interval=1.0,
            num_retries=3,
            backoff=ExponentialBackoff(),
        )
        db = Database(mock_client, mock_cb, 0.9, State.ACTIVE, health_checks=[hc])

        assert db.is_healthy() == True
        assert mock_client.execute_command.call_count == 3
        assert db.circuit.state == CBState.CLOSED

    def test_database_is_unhealthy_on_incorrect_echo_response(self, mock_client, mock_cb):
        """
        Mocking responses to mix error and actual responses to ensure that health check retry
        according to given configuration.
        """
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, 'wrong']
        mock_cb.state = CBState.CLOSED
        hc = EchoHealthCheck(
            check_interval=1.0,
            num_retries=3,
            backoff=ExponentialBackoff(),
        )
        db = Database(mock_client, mock_cb, 0.9, State.ACTIVE, health_checks=[hc])

        assert db.is_healthy() == False
        assert mock_client.execute_command.call_count == 3
        assert db.circuit.state == CBState.OPEN

    def test_database_is_unhealthy_on_exceeded_healthcheck_retries(self, mock_client, mock_cb):
        mock_client.execute_command.side_effect = [ConnectionError, ConnectionError, ConnectionError, ConnectionError]
        mock_cb.state = CBState.CLOSED
        hc = EchoHealthCheck(
            check_interval=1.0,
            num_retries=3,
            backoff=ExponentialBackoff(),
        )
        db = Database(mock_client, mock_cb, 0.9, State.ACTIVE, health_checks=[hc])

        assert db.is_healthy() == False
        assert mock_client.execute_command.call_count == 4
        assert db.circuit.state == CBState.OPEN