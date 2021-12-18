from unittest.mock import patch

import pytest

from redis.backoff import NoBackoff
from redis.client import Redis
from redis.connection import Connection, UnixDomainSocketConnection
from redis.exceptions import (
    BusyLoadingError,
    ConnectionError,
    ReadOnlyError,
    TimeoutError,
)
from redis.retry import Retry

from .conftest import _get_client


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
        c = Class(retry_on_timeout=retry_on_timeout, retry=Retry(NoBackoff(), retries))
        assert c.retry_on_timeout == retry_on_timeout
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == retries

    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_error(self, Class):
        c = Class(retry_on_error=[ReadOnlyError])
        assert c.retry_on_error == [ReadOnlyError]
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == 1

    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_error_empty_value(self, Class):
        c = Class(retry_on_error=[])
        assert c.retry_on_error == []
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == 0

    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_error_and_timeout(self, Class):
        c = Class(
            retry_on_error=[ReadOnlyError, BusyLoadingError], retry_on_timeout=True
        )
        assert c.retry_on_error == [ReadOnlyError, BusyLoadingError, TimeoutError]
        assert isinstance(c.retry, Retry)
        assert c.retry._retries == 1

    @pytest.mark.parametrize("retries", range(10))
    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    def test_retry_on_error_retry(self, Class, retries):
        c = Class(retry_on_error=[ReadOnlyError], retry=Retry(NoBackoff(), retries))
        assert c.retry_on_error == [ReadOnlyError]
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


@pytest.mark.onlynoncluster
class TestRedisClientRetry:
    "Test the standalone Redis client behavior with retries"

    def test_client_retry_on_error_with_success(self, request):
        with patch.object(Redis, "parse_response") as parse_response:

            def mock_parse_response(connection, *args, **options):
                def ok_response(connection, *args, **options):
                    return "MOCK_OK"

                parse_response.side_effect = ok_response
                raise ReadOnlyError()

            parse_response.side_effect = mock_parse_response
            r = _get_client(Redis, request, retry_on_error=[ReadOnlyError])
            assert r.get("foo") == "MOCK_OK"
            assert parse_response.call_count == 2

    def test_client_retry_on_error_raise(self, request):
        with patch.object(Redis, "parse_response") as parse_response:
            parse_response.side_effect = BusyLoadingError()
            retries = 3
            r = _get_client(
                Redis,
                request,
                retry_on_error=[ReadOnlyError, BusyLoadingError],
                retry=Retry(NoBackoff(), retries),
            )
            with pytest.raises(BusyLoadingError):
                try:
                    r.get("foo")
                finally:
                    assert parse_response.call_count == retries + 1

    def test_client_retry_on_error_different_error_raised(self, request):
        with patch.object(Redis, "parse_response") as parse_response:
            parse_response.side_effect = TimeoutError()
            retries = 3
            r = _get_client(
                Redis,
                request,
                retry_on_error=[ReadOnlyError],
                retry=Retry(NoBackoff(), retries),
            )
            with pytest.raises(TimeoutError):
                try:
                    r.get("foo")
                finally:
                    assert parse_response.call_count == 1

    def test_client_retry_on_error_and_timeout(self, request):
        with patch.object(Redis, "parse_response") as parse_response:
            parse_response.side_effect = TimeoutError()
            retries = 3
            r = _get_client(
                Redis,
                request,
                retry_on_error=[ReadOnlyError],
                retry_on_timeout=True,
                retry=Retry(NoBackoff(), retries),
            )
            with pytest.raises(TimeoutError):
                try:
                    r.get("foo")
                finally:
                    assert parse_response.call_count == retries + 1

    def test_client_retry_on_timeout(self, request):
        with patch.object(Redis, "parse_response") as parse_response:
            parse_response.side_effect = TimeoutError()
            retries = 3
            r = _get_client(
                Redis, request, retry_on_timeout=True, retry=Retry(NoBackoff(), retries)
            )
            with pytest.raises(TimeoutError):
                try:
                    r.get("foo")
                finally:
                    assert parse_response.call_count == retries + 1
