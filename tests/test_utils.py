from datetime import datetime
import warnings
import pytest
from redis.utils import (
    compare_versions,
    deprecated_function,
    deprecated_args,
    experimental_method,
    experimental_args,
)


@pytest.mark.parametrize(
    "version1,version2,expected_res",
    [
        ("1.0.0", "0.9.0", -1),
        ("1.0.0", "1.0.0", 0),
        ("0.9.0", "1.0.0", 1),
        ("1.09.0", "1.9.0", 0),
        ("1.090.0", "1.9.0", -1),
        ("1", "0.9.0", -1),
        ("1", "1.0.0", 0),
    ],
    ids=[
        "version1 > version2",
        "version1 == version2",
        "version1 < version2",
        "version1 == version2 - different minor format",
        "version1 > version2 - different minor format",
        "version1 > version2 - major version only",
        "version1 == version2 - major version only",
    ],
)
def test_compare_versions(version1, version2, expected_res):
    assert compare_versions(version1, version2) == expected_res


def redis_server_time(client):
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)


# Tests for deprecated_function decorator
class TestDeprecatedFunction:
    def test_sync_function_warns(self):
        @deprecated_function(reason="use new_func", version="1.0.0")
        def old_func():
            return "result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = old_func()
            assert result == "result"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_func" in str(w[0].message)
            assert "use new_func" in str(w[0].message)
            assert "1.0.0" in str(w[0].message)

    def test_preserves_function_metadata(self):
        @deprecated_function()
        def documented_func():
            """This is the docstring."""
            pass

        assert documented_func.__name__ == "documented_func"
        assert documented_func.__doc__ == "This is the docstring."


# Tests for deprecated_args decorator
class TestDeprecatedArgs:
    def test_sync_function_warns_on_deprecated_arg(self):
        @deprecated_args(args_to_warn=["old_param"], reason="use new_param")
        def func_with_args(new_param=None, old_param=None):
            return new_param or old_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_args(old_param="value")
            assert result == "value"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "old_param" in str(w[0].message)

    def test_sync_function_no_warning_on_allowed_arg(self):
        @deprecated_args(args_to_warn=["*"], allowed_args=["allowed_param"])
        def func_with_allowed(allowed_param=None):
            return allowed_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_allowed(allowed_param="value")
            assert result == "value"
            assert len(w) == 0

    def test_wildcard_warns_all_args(self):
        @deprecated_args(args_to_warn=["*"])
        def func_all_deprecated(param1=None, param2=None):
            return (param1, param2)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_all_deprecated(param1="a", param2="b")
            assert result == ("a", "b")
            assert len(w) == 1
            assert "param1" in str(w[0].message) or "param2" in str(w[0].message)


# Tests for experimental_method decorator
class TestExperimentalMethod:
    def test_sync_function_warns(self):
        @experimental_method()
        def experimental_func():
            return "experimental_result"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = experimental_func()
            assert result == "experimental_result"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "experimental_func" in str(w[0].message)


# Tests for experimental_args decorator
class TestExperimentalArgs:
    def test_sync_function_warns_on_experimental_arg(self):
        @experimental_args(args_to_warn=["beta_param"])
        def func_with_experimental(stable_param=None, beta_param=None):
            return stable_param or beta_param

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_with_experimental(beta_param="beta_value")
            assert result == "beta_value"
            assert len(w) == 1
            assert issubclass(w[0].category, UserWarning)
            assert "beta_param" in str(w[0].message)

    def test_no_warning_when_no_args_provided(self):
        @experimental_args(args_to_warn=["beta_param"])
        def func_no_args():
            return "no_args"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = func_no_args()
            assert result == "no_args"
            assert len(w) == 0
