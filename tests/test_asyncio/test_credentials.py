import functools
import random
import string
from typing import Optional, Tuple, Union

import pytest
import pytest_asyncio
import redis
from redis import AuthenticationError, DataError, ResponseError
from redis.credentials import CredentialProvider, UsernamePasswordCredentialProvider
from redis.utils import str_if_bytes
from tests.conftest import skip_if_redis_enterprise


@pytest_asyncio.fixture()
async def r_acl_teardown(r: redis.Redis):
    """
    A special fixture which removes the provided names from the database after use
    """
    usernames = []

    def factory(username):
        usernames.append(username)
        return r

    yield factory
    for username in usernames:
        await r.acl_deluser(username)


@pytest_asyncio.fixture()
async def r_required_pass_teardown(r: redis.Redis):
    """
    A special fixture which removes the provided password from the database after use
    """
    passwords = []

    def factory(username):
        passwords.append(username)
        return r

    yield factory
    for password in passwords:
        try:
            await r.auth(password)
        except (ResponseError, AuthenticationError):
            await r.auth("default", "")
        await r.config_set("requirepass", "")


class NoPassCredProvider(CredentialProvider):
    def get_credentials(self) -> Union[Tuple[str], Tuple[str, str]]:
        return "username", ""


class AsyncRandomAuthCredProvider(CredentialProvider):
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


async def init_acl_user(r, username, password):
    # reset the user
    await r.acl_deluser(username)
    if password:
        assert (
            await r.acl_setuser(
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
            await r.acl_setuser(
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


async def init_required_pass(r, password):
    await r.config_set("requirepass", password)


@pytest.mark.asyncio
class TestCredentialsProvider:
    @skip_if_redis_enterprise()
    async def test_only_pass_without_creds_provider(
        self, r_required_pass_teardown, create_redis
    ):
        # test for default user (`username` is supposed to be optional)
        password = "password"
        r = r_required_pass_teardown(password)
        await init_required_pass(r, password)
        assert await r.auth(password) is True

        r2 = await create_redis(flushdb=False, password=password)

        assert await r2.ping() is True

    @skip_if_redis_enterprise()
    async def test_user_and_pass_without_creds_provider(
        self, r_acl_teardown, create_redis
    ):
        """
        Test backward compatibility with username and password
        """
        # test for other users
        username = "username"
        password = "password"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, password)
        r2 = await create_redis(flushdb=False, username=username, password=password)

        assert await r2.ping() is True

    @pytest.mark.parametrize("username", ["username", None])
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    async def test_credential_provider_with_supplier(
        self, r_acl_teardown, r_required_pass_teardown, create_redis, username
    ):
        creds_provider = AsyncRandomAuthCredProvider(
            user=username,
            endpoint="localhost",
        )

        auth_args = creds_provider.get_credentials()
        password = auth_args[-1]

        if username:
            r = r_acl_teardown(username)
            await init_acl_user(r, username, password)
        else:
            r = r_required_pass_teardown(password)
            await init_required_pass(r, password)

        r2 = await create_redis(flushdb=False, credential_provider=creds_provider)

        assert await r2.ping() is True

    async def test_async_credential_provider_no_password_success(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "")
        r2 = await create_redis(
            flushdb=False,
            credential_provider=NoPassCredProvider(),
        )
        assert await r2.ping() is True

    @pytest.mark.onlynoncluster
    async def test_credential_provider_no_password_error(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "password")
        with pytest.raises(AuthenticationError) as e:
            await create_redis(
                flushdb=False,
                credential_provider=NoPassCredProvider(),
                single_connection_client=True,
            )
        assert e.match("invalid username-password")
        assert await r.acl_deluser(username)

    @pytest.mark.onlynoncluster
    async def test_password_and_username_together_with_cred_provider_raise_error(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, "password")
        cred_provider = UsernamePasswordCredentialProvider(
            username="username", password="password"
        )
        with pytest.raises(DataError) as e:
            await create_redis(
                flushdb=False,
                username="username",
                password="password",
                credential_provider=cred_provider,
                single_connection_client=True,
            )
        assert e.match(
            "'username' and 'password' cannot be passed along with "
            "'credential_provider'."
        )

    @pytest.mark.onlynoncluster
    async def test_change_username_password_on_existing_connection(
        self, r_acl_teardown, create_redis
    ):
        username = "origin_username"
        password = "origin_password"
        new_username = "new_username"
        new_password = "new_password"
        r = r_acl_teardown(username)
        await init_acl_user(r, username, password)
        r2 = await create_redis(flushdb=False, username=username, password=password)
        assert await r2.ping() is True
        conn = await r2.connection_pool.get_connection("_")
        await conn.send_command("PING")
        assert str_if_bytes(await conn.read_response()) == "PONG"
        assert conn.username == username
        assert conn.password == password
        await init_acl_user(r, new_username, new_password)
        conn.password = new_password
        conn.username = new_username
        await conn.send_command("PING")
        assert str_if_bytes(await conn.read_response()) == "PONG"


@pytest.mark.asyncio
class TestUsernamePasswordCredentialProvider:
    async def test_user_pass_credential_provider_acl_user_and_pass(
        self, r_acl_teardown, create_redis
    ):
        username = "username"
        password = "password"
        r = r_acl_teardown(username)
        provider = UsernamePasswordCredentialProvider(username, password)
        assert provider.username == username
        assert provider.password == password
        assert provider.get_credentials() == (username, password)
        await init_acl_user(r, provider.username, provider.password)
        r2 = await create_redis(flushdb=False, credential_provider=provider)
        assert await r2.ping() is True

    async def test_user_pass_provider_only_password(
        self, r_required_pass_teardown, create_redis
    ):
        password = "password"
        provider = UsernamePasswordCredentialProvider(password=password)
        r = r_required_pass_teardown(password)
        assert provider.username == ""
        assert provider.password == password
        assert provider.get_credentials() == (password,)

        await init_required_pass(r, password)

        r2 = await create_redis(flushdb=False, credential_provider=provider)
        assert await r2.auth(provider.password) is True
        assert await r2.ping() is True
