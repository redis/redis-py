import random
import string

import pytest

import redis
from redis import AuthenticationError, ResponseError
from redis.credentials import CredentialProvider, StaticCredentialProvider
from tests.conftest import _get_client, skip_if_redis_enterprise


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
    def test_credential_provider_without_supplier_only_pass(self, r, request):
        # test for default user (`username` is supposed to be optional)
        password = "password"
        creds_provider = CredentialProvider(password=password)
        init_required_pass(r, request, password)
        assert r.auth(creds_provider.password) is True

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=creds_provider
        )

        assert r2.ping()

    @skip_if_redis_enterprise()
    def test_credential_provider_without_supplier_acl_user_and_pass(self, r, request):
        # test for other users
        username = "username"
        password = "password"

        init_acl_user(r, request, username, password)
        creds_provider = CredentialProvider(username, password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=creds_provider
        )

        assert r2.ping() is True

    @pytest.mark.parametrize("username", ["username", ""])
    @skip_if_redis_enterprise()
    @pytest.mark.onlynoncluster
    def test_credential_provider_with_supplier(self, r, request, username):
        import functools

        @functools.lru_cache(maxsize=10)
        def auth_supplier(self, user, endpoint):
            def get_random_string(length):
                letters = string.ascii_lowercase
                result_str = "".join(random.choice(letters) for i in range(length))
                return result_str

            auth_token = get_random_string(5) + user + "_" + endpoint
            return user, auth_token

        creds_provider = CredentialProvider(
            supplier=auth_supplier,
            user=username,
            endpoint="localhost",
        )
        password = creds_provider.password

        if username:
            init_acl_user(r, request, username, password)
        else:
            init_required_pass(r, request, password)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=creds_provider
        )

        assert r2.ping() is True

    def test_credential_provider_no_password_success(self, r, request):
        def creds_provider(self):
            return "username", ""

        init_acl_user(r, request, "username", "")
        creds_provider = CredentialProvider(supplier=creds_provider)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=creds_provider
        )
        assert r2.ping() is True

    @pytest.mark.onlynoncluster
    def test_credential_provider_no_password_error(self, r, request):
        def bad_creds_provider(self):
            return "username", ""

        init_acl_user(r, request, "username", "password")
        creds_provider = CredentialProvider(supplier=bad_creds_provider)
        with pytest.raises(AuthenticationError) as e:
            _get_client(
                redis.Redis, request, flushdb=False, credential_provider=creds_provider
            )
        assert e.match("invalid username-password")


class TestStaticCredentialProvider:
    def test_static_credential_provider_acl_user_and_pass(self, r, request):
        username = "username"
        password = "password"
        provider = StaticCredentialProvider(username, password)
        assert provider.username == username
        assert provider.password == password
        assert provider.get_credentials() == (username, password)
        init_acl_user(r, request, provider.username, provider.password)
        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=provider
        )
        assert r2.ping() is True

    def test_static_credential_provider_only_password(self, r, request):
        password = "password"
        provider = StaticCredentialProvider(password=password)
        assert provider.username == ""
        assert provider.password == password
        assert provider.get_credentials() == (password,)

        init_required_pass(r, request, password)

        r2 = _get_client(
            redis.Redis, request, flushdb=False, credential_provider=provider
        )
        assert r2.auth(provider.password) is True
        assert r2.ping() is True
