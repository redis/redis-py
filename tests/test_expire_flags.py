import datetime

import pytest

from redis.exceptions import DataError
from redis.utils import extract_expire_flags, truncate_text


class TestExtractExpireFlags:
    def test_no_flags_returns_empty(self):
        assert extract_expire_flags() == []

    def test_ex_as_int(self):
        assert extract_expire_flags(ex=10) == ["EX", 10]

    def test_ex_as_timedelta_uses_total_seconds(self):
        assert extract_expire_flags(ex=datetime.timedelta(minutes=1)) == ["EX", 60]

    def test_ex_as_digit_string(self):
        assert extract_expire_flags(ex="60") == ["EX", 60]

    def test_ex_invalid_raises(self):
        with pytest.raises(DataError):
            extract_expire_flags(ex="not-a-number")

    def test_px_as_int(self):
        assert extract_expire_flags(px=500) == ["PX", 500]

    def test_px_as_timedelta_uses_milliseconds(self):
        assert extract_expire_flags(px=datetime.timedelta(seconds=2)) == ["PX", 2000]

    def test_exat_as_int(self):
        assert extract_expire_flags(exat=1700000000) == ["EXAT", 1700000000]

    def test_exat_as_datetime_uses_timestamp(self):
        when = datetime.datetime(2023, 11, 14, 22, 13, 20, tzinfo=datetime.timezone.utc)
        assert extract_expire_flags(exat=when) == ["EXAT", int(when.timestamp())]

    def test_pxat_as_int(self):
        assert extract_expire_flags(pxat=1700000000000) == ["PXAT", 1700000000000]

    def test_ex_takes_precedence_over_px(self):
        # The flags are checked in order; ex wins when several are given.
        assert extract_expire_flags(ex=5, px=999) == ["EX", 5]


class TestTruncateText:
    def test_short_text_unchanged(self):
        assert truncate_text("hi") == "hi"

    def test_long_text_gets_ellipsis(self):
        result = truncate_text("hello world this is long", 10)
        assert result == "hello..."
        assert len(result) <= 10
