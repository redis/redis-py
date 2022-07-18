import random
import string

import redis
from redis import ResponseError
from redis.credentials import CredentialsProvider
from tests.conftest import _get_client, skip_if_redis_enterprise


class TestCredentialsProvider:
    @skip_if_redis_enterprise()
    def test_credentials_provider_without_supplier(self, r, request):
        # first, test for default user (`username` is supposed to be optional)
        default_username = "default"
        temp_pass = "temp_pass"
        creds_provider = CredentialsProvider(default_username, temp_pass)
        r.config_set("requirepass", temp_pass)
        creds = creds_provider.get_credentials()
        assert r.auth(creds[1], creds[0]) is True
        assert r.auth(creds_provider.get_password()) is True

        # test for other users
        username = "redis-py-auth"
        password = "strong_password"

        def teardown():
            try:
                r.auth(temp_pass)
            except ResponseError:
                r.auth("default", "")
            r.config_set("requirepass", "")
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        assert r.acl_setuser(
            username,
            enabled=True,
            passwords=["+" + password],
            keys="~*",
            commands=["+ping", "+command", "+info", "+select", "+flushdb", "+cluster"],
        )

        creds_provider2 = CredentialsProvider(username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credentials_provider=creds_provider2
        )

        assert r2.ping() is True

    @skip_if_redis_enterprise()
    def test_credentials_provider_with_supplier(self, r, request):
        import functools

        @functools.lru_cache(maxsize=10)
        def auth_supplier(user, endpoint):
            def get_random_string(length):
                letters = string.ascii_lowercase
                result_str = "".join(random.choice(letters) for i in range(length))
                return result_str

            auth_token = get_random_string(5) + user + "_" + endpoint
            return user, auth_token

        username = "redis-py-auth"
        creds_provider = CredentialsProvider(
            supplier=auth_supplier,
            user=username,
            endpoint="localhost",
        )
        password = creds_provider.get_password()

        assert r.acl_setuser(
            username,
            enabled=True,
            passwords=["+" + password],
            keys="~*",
            commands=["+ping", "+command", "+info", "+select", "+flushdb", "+cluster"],
        )

        def teardown():
            r.acl_deluser(username)

        request.addfinalizer(teardown)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credentials_provider=creds_provider
        )

        assert r2.ping() is True
