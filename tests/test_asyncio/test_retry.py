import warnings
from threading import Lock

import pytest
from redis.asyncio import Redis, RedisCluster
from redis.asyncio.connection import (
    Connection,
    ConnectionPool,
    UnixDomainSocketConnection,
)
from redis.asyncio.retry import Retry
from redis.backoff import AbstractBackoff, ExponentialBackoff, NoBackoff
from redis.exceptions import ConnectionError, TimeoutError
from redis.retry import AbstractRetry
from redis.retry import Retry as SyncRetry


class BackoffMock(AbstractBackoff):
    def __init__(self):
        self.reset_calls = 0
        self.calls = 0

    def reset(self):
        self.reset_calls += 1

    def compute(self, failures):
        self.calls += 1
        return 0


class UncopyableBackoff(AbstractBackoff):
    def __init__(self):
        self._lock = Lock()

    def compute(self, failures):
        with self._lock:
            return 0


class CustomAsyncRetry(AbstractRetry):
    def __init__(self):
        super().__init__(NoBackoff(), 1, (ConnectionError,))

    def __eq__(self, other):
        return self is other

    async def call_with_retry(self, do, fail, **kwargs):
        return await do()


class DuckTypedAsyncRetry:
    async def call_with_retry(self, do, fail, **kwargs):
        return await do()

    def get_retries(self):
        return 1

    def update_supported_errors(self, specified_errors):
        pass


class DuckTypedAwaitableRetry:
    def call_with_retry(self, do, fail, **kwargs):
        return do()

    def get_retries(self):
        return 1

    def update_supported_errors(self, specified_errors):
        pass


@pytest.mark.fixed_client
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

    @pytest.mark.parametrize("Class", [Connection, UnixDomainSocketConnection])
    @pytest.mark.asyncio
    async def test_sync_retry_is_used_by_connect(self, Class, monkeypatch):
        retry = SyncRetry(NoBackoff(), 2)
        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            connection = Class(retry=retry)
        attempts = 0
        failures = 0

        async def connect_check_health(
            _self, check_health=True, retry_socket_connect=True
        ):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ConnectionError

        async def disconnect(_self, *args, **kwargs):
            nonlocal failures
            failures += 1

        monkeypatch.setattr(Class, "connect_check_health", connect_check_health)
        monkeypatch.setattr(Class, "disconnect", disconnect)

        assert isinstance(connection.retry, Retry)
        await connection.connect()

        assert attempts == 3
        assert failures == 2

    @pytest.mark.asyncio
    async def test_pool_from_url_converts_sync_retry(self, monkeypatch):
        retry = SyncRetry(NoBackoff(), 2)
        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            pool = ConnectionPool.from_url(
                "redis://localhost:6379",
                retry=retry,
                retry_on_error=[ConnectionError],
            )

        async def ensure_connection(_self, _connection):
            pass

        monkeypatch.setattr(ConnectionPool, "ensure_connection", ensure_connection)
        connection = await pool.get_connection()

        assert isinstance(connection.retry, Retry)
        assert connection.retry.get_retries() == 2
        assert ConnectionError in connection.retry._supported_errors

        await pool.release(connection)
        await pool.aclose()

    @pytest.mark.parametrize(
        "retry",
        [CustomAsyncRetry(), DuckTypedAsyncRetry(), DuckTypedAwaitableRetry()],
    )
    def test_async_shaped_retry_is_preserved(self, retry):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            pool = ConnectionPool(retry=retry)

        assert pool.connection_kwargs["retry"] is retry

    @pytest.mark.asyncio
    async def test_pool_set_retry_applies_to_new_connection(self, monkeypatch):
        pool = ConnectionPool()
        retry = Retry(NoBackoff(), 2)
        pool.set_retry(retry)

        async def ensure_connection(_self, _connection):
            pass

        monkeypatch.setattr(ConnectionPool, "ensure_connection", ensure_connection)
        connection = await pool.get_connection()

        assert connection.retry.get_retries() == retry.get_retries()
        assert isinstance(connection.retry._backoff, NoBackoff)

        await pool.release(connection)
        await pool.aclose()

    def test_pool_converts_sync_retry(self):
        retry = SyncRetry(NoBackoff(), 2)
        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            client = Redis(retry=retry)

        assert isinstance(client.get_retry(), Retry)
        assert client.get_retry().get_retries() == retry.get_retries()

        new_retry = SyncRetry(ExponentialBackoff(), 3)
        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            client.set_retry(new_retry)

        assert isinstance(client.get_retry(), Retry)
        assert client.get_retry().get_retries() == new_retry.get_retries()

    def test_cluster_converts_sync_retry(self):
        retry = SyncRetry(NoBackoff(), 2)
        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            client = RedisCluster(host="127.0.0.1", port=6379, retry=retry)

        assert isinstance(client.retry, Retry)
        assert client.retry.get_retries() == retry.get_retries()

        new_retry = SyncRetry(ExponentialBackoff(), 3)
        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            client.set_retry(new_retry)

        assert isinstance(client.retry, Retry)
        assert client.retry.get_retries() == new_retry.get_retries()

    def test_cluster_preserves_uncopyable_sync_backoff(self):
        backoff = UncopyableBackoff()
        retry = SyncRetry(backoff, 2)

        with pytest.warns(UserWarning, match="synchronous redis.retry.Retry"):
            client = RedisCluster(host="127.0.0.1", port=6379, retry=retry)

        assert isinstance(client.retry, Retry)
        assert client.retry._backoff is backoff


@pytest.mark.fixed_client
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


class TestRedisClientRetry:
    "Test the Redis client behavior with retries"

    async def test_get_set_retry_object(self, request):
        retry = Retry(NoBackoff(), 2)
        url = request.config.getoption("--redis-url")
        r = await Redis.from_url(url, retry_on_timeout=True, retry=retry)
        assert r.get_retry()._retries == retry._retries
        assert isinstance(r.get_retry()._backoff, NoBackoff)
        new_retry_policy = Retry(ExponentialBackoff(), 3)
        exiting_conn = await r.connection_pool.get_connection()
        r.set_retry(new_retry_policy)
        assert r.get_retry()._retries == new_retry_policy._retries
        assert isinstance(r.get_retry()._backoff, ExponentialBackoff)
        assert exiting_conn.retry._retries == new_retry_policy._retries
        await r.connection_pool.release(exiting_conn)
        new_conn = await r.connection_pool.get_connection()
        assert new_conn.retry._retries == new_retry_policy._retries
        await r.connection_pool.release(new_conn)
        await r.aclose()
