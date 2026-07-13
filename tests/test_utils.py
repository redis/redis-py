from datetime import datetime, timedelta, timezone
import warnings
import pytest
from redis.exceptions import DataError
from redis.utils import (
    DEFAULT_RESP_VERSION,
    check_protocol_version,
    compare_versions,
    deprecated_function,
    deprecated_args,
    experimental_method,
    experimental_args,
    extract_expire_flags,
    truncate_text,
)


@pytest.mark.fixed_client
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


@pytest.mark.fixed_client
class TestCheckProtocolVersion:
    """``check_protocol_version`` underpins protocol-gated features
    (caching, maintenance notifications, callback selection). It must
    treat ``None`` as ``DEFAULT_RESP_VERSION`` so callers using the empty
    default get the same answer as if they had pinned the wire protocol."""

    def test_none_resolves_to_default(self):
        # ``DEFAULT_RESP_VERSION`` is the wire version selected when the
        # caller does not pass ``protocol``; ``check_protocol_version``
        # must therefore treat ``None`` as that version.
        assert check_protocol_version(None, DEFAULT_RESP_VERSION) is True
        other = 2 if DEFAULT_RESP_VERSION == 3 else 3
        assert check_protocol_version(None, other) is False

    @pytest.mark.parametrize("protocol", [3, "3"])
    def test_resp3_matches(self, protocol):
        assert check_protocol_version(protocol, 3) is True
        assert check_protocol_version(protocol, 2) is False

    @pytest.mark.parametrize("protocol", [2, "2"])
    def test_resp2_matches(self, protocol):
        assert check_protocol_version(protocol, 2) is True
        assert check_protocol_version(protocol, 3) is False

    def test_invalid_string_returns_false(self):
        assert check_protocol_version("not-a-number", 3) is False


def redis_server_time(client):
    seconds, milliseconds = client.time()
    timestamp = float(f"{seconds}.{milliseconds}")
    return datetime.fromtimestamp(timestamp)


# Tests for deprecated_function decorator
@pytest.mark.fixed_client
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
@pytest.mark.fixed_client
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
@pytest.mark.fixed_client
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
@pytest.mark.fixed_client
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


@pytest.mark.fixed_client
class TestExtractExpireFlags:
    def test_no_flags_returns_empty(self):
        assert extract_expire_flags() == []

    def test_ex_as_int(self):
        assert extract_expire_flags(ex=10) == ["EX", 10]

    def test_ex_as_timedelta_uses_total_seconds(self):
        assert extract_expire_flags(ex=timedelta(minutes=1)) == ["EX", 60]

    def test_ex_as_digit_string(self):
        assert extract_expire_flags(ex="60") == ["EX", 60]

    def test_ex_invalid_raises(self):
        with pytest.raises(DataError):
            extract_expire_flags(ex="not-a-number")

    def test_px_as_int(self):
        assert extract_expire_flags(px=500) == ["PX", 500]

    def test_px_as_timedelta_uses_milliseconds(self):
        assert extract_expire_flags(px=timedelta(seconds=2)) == ["PX", 2000]

    def test_exat_as_int(self):
        assert extract_expire_flags(exat=1700000000) == ["EXAT", 1700000000]

    def test_exat_as_datetime_uses_timestamp(self):
        when = datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)
        assert extract_expire_flags(exat=when) == ["EXAT", int(when.timestamp())]

    def test_pxat_as_int(self):
        assert extract_expire_flags(pxat=1700000000000) == ["PXAT", 1700000000000]

    def test_ex_takes_precedence_over_px(self):
        # The flags are checked in order; ex wins when several are given.
        assert extract_expire_flags(ex=5, px=999) == ["EX", 5]


@pytest.mark.fixed_client
class TestTruncateText:
    def test_short_text_unchanged(self):
        assert truncate_text("hi") == "hi"

    def test_long_text_gets_ellipsis(self):
        result = truncate_text("hello world this is long", 10)
        assert result == "hello..."
        assert len(result) <= 10

    def test_default_max_length_truncates_long_text(self):
        # Every real call site relies on the default width of 100; make sure a
        # text longer than that is shortened without an explicit width argument.
        text = "word " * 40  # 200 characters
        result = truncate_text(text)
        assert len(result) <= 100
        assert result.endswith("...")

    def test_single_long_token_collapses_to_placeholder(self):
        # A single unbroken token wider than ``max_length``: ``textwrap.shorten``
        # has no whitespace to truncate on and does not split within a word (its
        # ``break_long_words`` does not apply to this case), so it drops the word
        # entirely and returns just the placeholder.
        assert truncate_text("a" * 200) == "..."
