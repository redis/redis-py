from redis.backoff import NoBackoff
import pytest

from redis.exceptions import ConnectionError
from redis.connection import Connection, UnixDomainSocketConnection
from redis.retry import Retry


class BackoffMock:
    def __init__(self):
        self.reset_calls = 0
        self.calls = 0

    def reset(self):
        self.reset_calls += 1

    def compute(self, failures):
        self.calls += 1
        return 0


class TestConnectionConstructorWithRetry:
    "Test that the Connection constructors properly handles Retry objects"

    @pytest.mark.parametrize("retry_on_timeout", [False, True])
    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_timeout_boolean(self, Class, retry_on_timeout):
        c = Class(retry_on_timeout=retry_on_timeout)
        assert c.retry_on_timeout == retry_on_timeout
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == (1 if retry_on_timeout else 0)

    @pytest.mark.parametrize("retries", range(10))
    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_timeout_retry(self, Class, retries):
        retry_on_timeout = retries > 0
        c = Class(retry_on_timeout=retry_on_timeout,
                  retry=Retry(NoBackoff(), retries))
        assert c.retry_on_timeout == retry_on_timeout
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == retries


class TestRetry:
    "Test that Retry calls backoff and retries the expected number of times"

    def setup_method(self, test_method):
        self.actual_attempts = 0
        self.actual_failures = 0

    def _do(self):
        self.actual_attempts += 1
        raise ConnectionError()

    def _fail(self, error):
        self.actual_failures += 1

    @pytest.mark.parametrize("retries", range(10))
    def test_retry(self, retries):
        backoff = BackoffMock()
        retry = Retry(backoff, retries)
        with pytest.raises(ConnectionError):
            retry.call_with_retry(self._do, self._fail)

        assert self.actual_attempts == 1 + retries
        assert self.actual_failures == 1 + retries
        assert backoff.reset_calls == 1
        assert backoff.calls == retries
