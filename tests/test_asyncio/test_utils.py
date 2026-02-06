from datetime import datetime
import warnings
import pytest
import redis
from redis.utils import (
    deprecated_function,
    deprecated_args,
    experimental_method,
    experimental_args,
)


async def redis_server_time(client: redis.Redis):
    seconds, milliseconds = await client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)


# Async tests for deprecated_function decorator
class TestDeprecatedFunctionAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns(self):
        @deprecated_function(reason="use new_async_func", version="2.0.0")
        async def old_async_func():
            return "async_result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await old_async_func()
            assert result == "async_result"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_async_func" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_preserves_function_metadata(self):
        @deprecated_function()
        async def async_documented_func():
            """This is the async docstring."""
            pass

        assert async_documented_func.__name__ == "async_documented_func"
        assert async_documented_func.__doc__ == "This is the async docstring."


# Async tests for deprecated_args decorator
class TestDeprecatedArgsAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns_on_deprecated_arg(self):
        @deprecated_args(args_to_warn=["old_param"], reason="use new_param")
        async def async_func_with_args(new_param=None, old_param=None):
            return new_param or old_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_with_args(old_param="async_value")
            assert result == "async_value"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_param" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_function_no_warning_on_allowed_arg(self):
        @deprecated_args(args_to_warn=["*"], allowed_args=["allowed_param"])
        async def async_func_with_allowed(allowed_param=None):
            return allowed_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_with_allowed(allowed_param="async_value")
            assert result == "async_value"
            assert len(w) == 0

    @pytest.mark.asyncio
    async def test_async_wildcard_warns_all_args(self):
        @deprecated_args(args_to_warn=["*"])
        async def async_func_all_deprecated(param1=None, param2=None):
            return (param1, param2)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_all_deprecated(param1="a", param2="b")
            assert result == ("a", "b")
            assert len(w) == 1
            assert "param1" in str(w[0].message) or "param2" in str(w[0].message)


# Async tests for experimental_method decorator
class TestExperimentalMethodAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns(self):
        @experimental_method()
        async def async_experimental_func():
            return "async_experimental_result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_experimental_func()
            assert result == "async_experimental_result"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "async_experimental_func" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_preserves_function_metadata(self):
        @experimental_method()
        async def async_experimental_documented():
            """Experimental async docstring."""
            pass

        assert async_experimental_documented.__name__ == "async_experimental_documented"
        assert async_experimental_documented.__doc__ == "Experimental async docstring."


# Async tests for experimental_args decorator
class TestExperimentalArgsAsync:
    @pytest.mark.asyncio
    async def test_async_function_warns_on_experimental_arg(self):
        @experimental_args(args_to_warn=["beta_param"])
        async def async_func_with_experimental(stable_param=None, beta_param=None):
            return stable_param or beta_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_with_experimental(beta_param="async_beta")
            assert result == "async_beta"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "beta_param" in str(w[0].message)

    @pytest.mark.asyncio
    async def test_async_no_warning_when_no_args_provided(self):
        @experimental_args(args_to_warn=["beta_param"])
        async def async_func_no_args():
            return "no_args"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = await async_func_no_args()
            assert result == "no_args"
            assert len(w) == 0
