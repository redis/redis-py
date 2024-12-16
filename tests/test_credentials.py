import functools
import random
import string
import threading
from time import sleep
from typing import Optional, Tuple, Union
from asyncio import sleep as asyncio_sleep, Lock as AsyncioLock

import pytest
from entraid.cred_provider import TokenAuthConfig, EntraIdCredentialsProvider
from entraid.identity_provider import create_provider_from_service_principal
from mock.mock import Mock, call

import redis
from redis import AuthenticationError, DataError, ResponseError, Redis, asyncio
from redis.asyncio import Redis as AsyncRedis, Connection
from redis.asyncio import ConnectionPool as AsyncConnectionPool
from redis.auth.idp import IdentityProviderInterface
from redis.exceptions import ConnectionError, RedisError
from redis.backoff import NoBackoff
from redis.connection import ConnectionInterface, ConnectionPool
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from redis.retry import Retry
from redis.utils import str_if_bytes
from tests.conftest import _get_client, skip_if_redis_enterprise, get_credential_provider


@pytest.fixture()
def r(request):
    credential_provider = request.param.get("cred_provider_class", None)
    single_connection = request.param.get("single_connection_client", False)

    if credential_provider is not None:
        credential_provider = get_credential_provider(request)

    with _get_client(
        redis.Redis,
        request,
        credential_provider=credential_provider,
        single_connection_client=single_connection,
    ) as client:
        yield client


class NoPassCredProvider(CredentialProvider):
    def get_credentials(self) -> Union[Tuple[str], Tuple[str, str]]:
        return "username", ""


class RandomAuthCredProvider(CredentialProvider):
    def __init__(self, user: Optional[str], endpoint: str):
        self.user = user
        self.endpoint = endpoint

    @functools.lru_cache(maxsize=10)
    def get_credentials(self) -> Union[Tuple[str, str], Tuple[str]]:
        def get_random_string(length):
            letters = string.ascii_lowercase
            result_str = "".join(random.choice(letters) for i in range(length))
            return result_str

        if self.user:
            auth_token: str = get_random_string(5) + self.user + "_" + self.endpoint
            return self.user, auth_token
        else:
            auth_token: str = get_random_string(5) + self.endpoint
            return (auth_token,)


def init_acl_user(r, request, username, password):
    # reset the user
    r.acl_deluser(username)
    if password:
        assert (
            r.acl_setuser(
                username,
                enabled=True,
                passwords=["+" + password],
                keys="~*",
                commands=[
                    "+ping",
                    "+command",
                    "+info",
                    "+select",
                    "+flushdb",
                    "+cluster",
                ],
            )
            is True
        )
    else:
        assert (
            r.acl_setuser(
                username,
                enabled=True,
                keys="~*",
                commands=[
                    "+ping",
                    "+command",
                    "+info",
                    "+select",
                    "+flushdb",
                    "+cluster",
                ],
                nopass=True,
            )
            is True
        )

    if request is not None:

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)


def init_required_pass(r, request, password):
    r.config_set("requirepass", password)

    def teardown():
        try:
            r.auth(password)
        except (ResponseError, AuthenticationError):
            r.auth("default", "")
        r.config_set("requirepass", "")

    request.addfinalizer(teardown)


class TestCredentialsProvider:
    @skip_if_redis_enterprise()
    def test_only_pass_without_creds_provider(self, r, request):
        # test for default user (`username` is supposed to be optional)
        password = "password"
        init_required_pass(r, request, password)
        assert r.auth(password) is True

        r2 = _get_client(redis.Redis, request, flushdb=False, password=password)

        assert r2.ping() is True

    @skip_if_redis_enterprise()
    def test_user_and_pass_without_creds_provider(self, r, request):
        """
        Test backward compatibility with username and password
        """
        # test for other users
        username = "username"
        password = "password"

        init_acl_user(r, request, username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, username=username, password=password
        )

        assert r2.ping() is True

    @pytest.mark.parametrize("username", ["username", None])
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_credential_provider_with_supplier(self, r, request, username):
        creds_provider = RandomAuthCredProvider(
            user=username,
            endpoint="localhost",
        )

        password = creds_provider.get_credentials()[-1]

        if username:
            init_acl_user(r, request, username, password)
        else:
            init_required_pass(r, request, password)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=creds_provider
        )

        assert r2.ping() is True

    def test_credential_provider_no_password_success(self, r, request):
        init_acl_user(r, request, "username", "")
        r2 = _get_client(
            redis.Redis,
            request,
            flushdb=False,
            credential_provider=NoPassCredProvider(),
        )
        assert r2.ping() is True

    @pytest.mark.onlynoncluster
    def test_credential_provider_no_password_error(self, r, request):
        init_acl_user(r, request, "username", "password")
        with pytest.raises(AuthenticationError) as e:
            _get_client(
                redis.Redis,
                request,
                flushdb=False,
                credential_provider=NoPassCredProvider(),
            )
        assert e.match("invalid username-password")

    @pytest.mark.onlynoncluster
    def test_password_and_username_together_with_cred_provider_raise_error(
        self, r, request
    ):
        init_acl_user(r, request, "username", "password")
        cred_provider = UsernamePasswordCredentialProvider(
            username="username", password="password"
        )
        with pytest.raises(DataError) as e:
            _get_client(
                redis.Redis,
                request,
                flushdb=False,
                username="username",
                password="password",
                credential_provider=cred_provider,
            )
        assert e.match(
            "'username' and 'password' cannot be passed along with "
            "'credential_provider'."
        )

    @pytest.mark.onlynoncluster
    def test_change_username_password_on_existing_connection(self, r, request):
        username = "origin_username"
        password = "origin_password"
        new_username = "new_username"
        new_password = "new_password"

        def teardown():
            r.acl_deluser(new_username)

        request.addfinalizer(teardown)

        init_acl_user(r, request, username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, username=username, password=password
        )
        assert r2.ping() is True
        conn = r2.connection_pool.get_connection("_")
        conn.send_command("PING")
        assert str_if_bytes(conn.read_response()) == "PONG"
        assert conn.username == username
        assert conn.password == password
        init_acl_user(r, request, new_username, new_password)
        conn.password = new_password
        conn.username = new_username
        conn.send_command("PING")
        assert str_if_bytes(conn.read_response()) == "PONG"


class TestUsernamePasswordCredentialProvider:
    def test_user_pass_credential_provider_acl_user_and_pass(self, r, request):
        username = "username"
        password = "password"
        provider = UsernamePasswordCredentialProvider(username, password)
        assert provider.username == username
        assert provider.password == password
        assert provider.get_credentials() == (username, password)
        init_acl_user(r, request, provider.username, provider.password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=provider
        )
        assert r2.ping() is True

    def test_user_pass_provider_only_password(self, r, request):
        password = "password"
        provider = UsernamePasswordCredentialProvider(password=password)
        assert provider.username == ""
        assert provider.password == password
        assert provider.get_credentials() == (password,)

        init_required_pass(r, request, password)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=provider
        )
        assert r2.auth(provider.password) is True
        assert r2.ping() is True


@pytest.mark.onlynoncluster
class TestStreamingCredentialProvider:
    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    def test_re_auth_all_connections(self, credential_provider):
        mock_connection = Mock(spec=ConnectionInterface)
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection, mock_another_connection]
        mock_pool._lock = threading.Lock()
        auth_token = None

        def re_auth_callback(token):
            nonlocal auth_token
            auth_token = token
            with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    conn.send_command('AUTH', token.try_get('oid'), token.get_value())
                    conn.read_response()

        mock_pool.re_auth_callback = re_auth_callback

        Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        credential_provider.get_credentials()
        sleep(0.5)

        mock_connection.send_command.assert_has_calls([
            call('AUTH', auth_token.try_get('oid'), auth_token.get_value())
        ])
        mock_another_connection.send_command.assert_has_calls([
            call('AUTH', auth_token.try_get('oid'), auth_token.get_value())
        ])

    @pytest.mark.parametrize(
        "credential_provider",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "cred_provider_kwargs": {"expiration_refresh_ratio": 0.00005},
                "mock_idp": True,
            }
        ],
        indirect=True,
    )
    def test_re_auth_partial_connections(self, credential_provider):
        mock_connection = Mock(spec=ConnectionInterface)
        mock_connection.retry = Retry(NoBackoff(), 3)
        mock_another_connection = Mock(spec=ConnectionInterface)
        mock_another_connection.retry = Retry(NoBackoff(), 3)
        mock_failed_connection = Mock(spec=ConnectionInterface)
        mock_failed_connection.read_response.side_effect = ConnectionError("Failed auth")
        mock_failed_connection.retry = Retry(NoBackoff(), 3)
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.connection_kwargs = {
            "credential_provider": credential_provider,
        }
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection, mock_another_connection, mock_failed_connection]
        mock_pool._lock = threading.Lock()

        def _raise(error: RedisError):
            pass

        def re_auth_callback(token):
            with mock_pool._lock:
                for conn in mock_pool._available_connections:
                    conn.retry.call_with_retry(
                        lambda: conn.send_command('AUTH', token.try_get('oid'), token.get_value()),
                        lambda error: _raise(error)
                    )
                    conn.retry.call_with_retry(
                        lambda: conn.read_response(),
                        lambda error: _raise(error)
                    )

        mock_pool.re_auth_callback = re_auth_callback

        Redis(
            connection_pool=mock_pool,
            credential_provider=credential_provider,
        )

        credential_provider.get_credentials()
        sleep(0.5)

        mock_connection.read_response.assert_has_calls([call()])
        mock_another_connection.read_response.assert_has_calls([call()])
        mock_failed_connection.read_response.assert_has_calls([call(), call(), call()])


@pytest.mark.cp_integration
class TestEntraIdCredentialsProvider:
    @pytest.mark.parametrize(
        "r",
        [
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": False
            },
            {
                "cred_provider_class": EntraIdCredentialsProvider,
                "single_connection_client": True
            },
        ],
        ids=['pool', 'single'],
        indirect=True,
    )
    @pytest.mark.cp_integration
    def test_auth_pool_with_credential_provider(self, r: redis.Redis):
        assert r.ping() is True
