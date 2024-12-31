import asyncio
from datetime import datetime, timezone
from time import sleep
from unittest.mock import Mock

import pytest
from redis.auth.err import RequestTokenErr, TokenRenewalErr
from redis.auth.idp import IdentityProviderInterface
from redis.auth.token import SimpleToken
from redis.auth.token_manager import (
    CredentialsListener,
    RetryPolicy,
    TokenManager,
    TokenManagerConfig,
)


class TestTokenManager:
    @pytest.mark.parametrize(
        "exp_refresh_ratio",
        [
            0.9,
            0.28,
        ],
        ids=[
            "Refresh ratio = 0.9",
            "Refresh ratio = 0.28",
        ],
    )
    def test_success_token_renewal(self, exp_refresh_ratio):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 150,
                (datetime.now(timezone.utc).timestamp() * 1000) + 50,
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 170,
                (datetime.now(timezone.utc).timestamp() * 1000) + 70,
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 190,
                (datetime.now(timezone.utc).timestamp() * 1000) + 90,
                {"oid": "test"},
            ),
        ]

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(exp_refresh_ratio, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)
        sleep(0.1)

        assert len(tokens) > 0

    @pytest.mark.parametrize(
        "exp_refresh_ratio",
        [
            (0.9),
            (0.28),
        ],
        ids=[
            "Refresh ratio = 0.9",
            "Refresh ratio = 0.28",
        ],
    )
    @pytest.mark.asyncio
    async def test_async_success_token_renewal(self, exp_refresh_ratio):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 130,
                (datetime.now(timezone.utc).timestamp() * 1000) + 30,
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 160,
                (datetime.now(timezone.utc).timestamp() * 1000) + 60,
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 190,
                (datetime.now(timezone.utc).timestamp() * 1000) + 90,
                {"oid": "test"},
            ),
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(exp_refresh_ratio, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        await asyncio.sleep(0.1)

        assert len(tokens) > 0

    @pytest.mark.parametrize(
        "block_for_initial,tokens_acquired",
        [
            (True, 1),
            (False, 0),
        ],
        ids=[
            "Block for initial, callback will triggered once",
            "Non blocked, callback wont be triggered",
        ],
    )
    @pytest.mark.asyncio
    async def test_async_request_token_blocking_behaviour(
        self, block_for_initial, tokens_acquired
    ):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 100,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        async def on_next(token):
            nonlocal tokens
            sleep(0.1)
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=block_for_initial)

        assert len(tokens) == tokens_acquired

    def test_token_renewal_with_skip_initial(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 50,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 150,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
        ]

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener, skip_initial=True)
        # Should be less than a 0.1, or it will be flacky due to
        # additional token renewal.
        sleep(0.1)

        assert len(tokens) > 0

    @pytest.mark.asyncio
    async def test_async_token_renewal_with_skip_initial(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 120,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 140,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, skip_initial=True)
        # Should be less than a 0.1, or it will be flacky
        # due to additional token renewal.
        await asyncio.sleep(0.2)

        assert len(tokens) > 0

    def test_success_token_renewal_with_retry(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
        ]

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)
        # Should be less than a 0.1, or it will be flacky
        # due to additional token renewal.
        sleep(0.08)

        assert mock_provider.request_token.call_count > 0
        assert len(tokens) > 0

    @pytest.mark.asyncio
    async def test_async_success_token_renewal_with_retry(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
            SimpleToken(
                "value",
                (datetime.now(timezone.utc).timestamp() * 1000) + 100,
                (datetime.now(timezone.utc).timestamp() * 1000),
                {"oid": "test"},
            ),
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = None

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        # Should be less than a 0.1, or it will be flacky
        # due to additional token renewal.
        await asyncio.sleep(0.08)

        assert mock_provider.request_token.call_count > 0
        assert len(tokens) > 0

    def test_no_token_renewal_on_process_complete(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)
        sleep(0.2)

        assert len(tokens) == 1

    @pytest.mark.asyncio
    async def test_async_no_token_renewal_on_process_complete(self):
        tokens = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(0.9, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        await asyncio.sleep(0.2)

        assert len(tokens) == 1

    def test_failed_token_renewal_with_retry(self):
        tokens = []
        exceptions = []

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
        ]

        def on_next(token):
            nonlocal tokens
            tokens.append(token)

        def on_error(exception):
            nonlocal exceptions
            exceptions.append(exception)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)
        sleep(0.1)

        assert mock_provider.request_token.call_count == 4
        assert len(tokens) == 0
        assert len(exceptions) == 1

    @pytest.mark.asyncio
    async def test_async_failed_token_renewal_with_retry(self):
        tokens = []
        exceptions = []

        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.side_effect = [
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
            RequestTokenErr,
        ]

        async def on_next(token):
            nonlocal tokens
            tokens.append(token)

        async def on_error(exception):
            nonlocal exceptions
            exceptions.append(exception)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(3, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)
        sleep(0.1)

        assert mock_provider.request_token.call_count == 4
        assert len(tokens) == 0
        assert len(exceptions) == 1

    def test_failed_renewal_on_expired_token(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) - 100,
            (datetime.now(timezone.utc).timestamp() * 1000) - 1000,
            {"oid": "test"},
        )

        def on_error(error: TokenRenewalErr):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Requested token is expired"

    @pytest.mark.asyncio
    async def test_async_failed_renewal_on_expired_token(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) - 100,
            (datetime.now(timezone.utc).timestamp() * 1000) - 1000,
            {"oid": "test"},
        )

        async def on_error(error: TokenRenewalErr):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Requested token is expired"

    def test_failed_renewal_on_callback_error(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        def on_next(token):
            raise Exception("Some exception")

        def on_error(error):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        mgr.start(mock_listener)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Some exception"

    @pytest.mark.asyncio
    async def test_async_failed_renewal_on_callback_error(self):
        errors = []
        mock_provider = Mock(spec=IdentityProviderInterface)
        mock_provider.request_token.return_value = SimpleToken(
            "value",
            (datetime.now(timezone.utc).timestamp() * 1000) + 1000,
            (datetime.now(timezone.utc).timestamp() * 1000),
            {"oid": "test"},
        )

        async def on_next(token):
            raise Exception("Some exception")

        async def on_error(error):
            nonlocal errors
            errors.append(error)

        mock_listener = Mock(spec=CredentialsListener)
        mock_listener.on_next = on_next
        mock_listener.on_error = on_error

        retry_policy = RetryPolicy(1, 10)
        config = TokenManagerConfig(1, 0, 1000, retry_policy)
        mgr = TokenManager(mock_provider, config)
        await mgr.start_async(mock_listener, block_for_initial=True)

        assert len(errors) == 1
        assert isinstance(errors[0], TokenRenewalErr)
        assert str(errors[0]) == "Some exception"
