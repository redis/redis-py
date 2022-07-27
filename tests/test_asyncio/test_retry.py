import pytest

from redis.asyncio.connection import Connection, UnixDomainSocketConnection
from redis.asyncio.retry import Retry
from redis.backoff import AbstractBackoff, NoBackoff
from redis.exceptions import ConnectionError, TimeoutError


class BackoffMock(AbstractBackoff):
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

    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_error_set(self, Class):
        class CustomError(Exception):
            pass

        retry_on_error = [ConnectionError, TimeoutError, CustomError]
        c = Class(retry_on_error=retry_on_error)
        assert c.retry_on_error == retry_on_error
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == 1
        assert set(c.retry._supported_errors) == set(retry_on_error)

    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_error_not_set(self, Class):
        c = Class()
        assert c.retry_on_error == []
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == 0

    @pytest.mark.parametrize("retry_on_timeout", [False, True])
    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_timeout(self, Class, retry_on_timeout):
        c = Class(retry_on_timeout=retry_on_timeout)
        assert c.retry_on_timeout == retry_on_timeout
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == (1 if retry_on_timeout else 0)

    @pytest.mark.parametrize("retries", range(10))
    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_with_retry_on_timeout(self, Class, retries: int):
        retry_on_timeout = retries > 0
        c = Class(retry_on_timeout=retry_on_timeout, retry=Retry(NoBackoff(), retries))
        assert c.retry_on_timeout == retry_on_timeout
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == retries

    @pytest.mark.parametrize("retries", range(10))
    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_with_retry_on_error(self, Class, retries: int):
        class CustomError(Exception):
            pass

        retry_on_error = [ConnectionError, TimeoutError, CustomError]
        c = Class(retry_on_error=retry_on_error, retry=Retry(NoBackoff(), retries))
        assert c.retry_on_error == retry_on_error
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == retries
        assert set(c.retry._supported_errors) == set(retry_on_error)


class TestRetry:
    "Test that Retry calls backoff and retries the expected number of times"

    def setup_method(self, test_method):
        self.actual_attempts = 0
        self.actual_failures = 0

    async def _do(self):
        self.actual_attempts += 1
        raise ConnectionError()

    async def _fail(self, error):
        self.actual_failures += 1

    async def _fail_inf(self, error):
        self.actual_failures += 1
        if self.actual_failures == 5:
            raise ConnectionError()

    @pytest.mark.parametrize("retries", range(10))
    @pytest.mark.asyncio
    async def test_retry(self, retries: int):
        backoff = BackoffMock()
        retry = Retry(backoff, retries)
        with pytest.raises(ConnectionError):
            await retry.call_with_retry(self._do, self._fail)

        assert self.actual_attempts == 1 + retries
        assert self.actual_failures == 1 + retries
        assert backoff.reset_calls == 1
        assert backoff.calls == retries

    @pytest.mark.asyncio
    async def test_infinite_retry(self):
        backoff = BackoffMock()
        # specify infinite retries, but give up after 5
        retry = Retry(backoff, -1)
        with pytest.raises(ConnectionError):
            await retry.call_with_retry(self._do, self._fail_inf)

        assert self.actual_attempts == 5
        assert self.actual_failures == 5
