import time
from datetime import datetime, timedelta

import pytest
from tests.conftest import skip_if_server_version_lt


@skip_if_server_version_lt("7.3.240")
def test_hexpire_basic(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert r.hexpire("test:hash", 1, "field1") == [1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpire_with_timedelta(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert r.hexpire("test:hash", timedelta(seconds=1), "field1") == [1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpire_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    assert r.hexpire("test:hash", 2, "field1", xx=True) == [0]
    assert r.hexpire("test:hash", 2, "field1", nx=True) == [1]
    assert r.hexpire("test:hash", 1, "field1", xx=True) == [1]
    assert r.hexpire("test:hash", 2, "field1", nx=True) == [0]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    r.hset("test:hash", "field1", "value1")
    r.hexpire("test:hash", 2, "field1")
    assert r.hexpire("test:hash", 1, "field1", gt=True) == [0]
    assert r.hexpire("test:hash", 1, "field1", lt=True) == [1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.3.240")
def test_hexpire_nonexistent_key_or_field(r):
    r.delete("test:hash")
    assert r.hexpire("test:hash", 1, "field1") == [-2]
    r.hset("test:hash", "field1", "value1")
    assert r.hexpire("test:hash", 1, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
def test_hexpire_multiple_fields(r):
    r.delete("test:hash")
    r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert r.hexpire("test:hash", 1, "field1", "field2") == [1, 1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is False
    assert r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpire_multiple_condition_flags_error(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    with pytest.raises(ValueError) as e:
        r.hexpire("test:hash", 1, "field1", nx=True, xx=True)
    assert "Only one of" in str(e)


@skip_if_server_version_lt("7.3.240")
def test_hpexpire_basic(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert r.hpexpire("test:hash", 500, "field1") == [1]
    time.sleep(0.6)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hpexpire_with_timedelta(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert r.hpexpire("test:hash", timedelta(milliseconds=500), "field1") == [1]
    time.sleep(0.6)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hpexpire_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    assert r.hpexpire("test:hash", 1500, "field1", xx=True) == [0]
    assert r.hpexpire("test:hash", 1500, "field1", nx=True) == [1]
    assert r.hpexpire("test:hash", 500, "field1", xx=True) == [1]
    assert r.hpexpire("test:hash", 1500, "field1", nx=True) == [0]
    time.sleep(0.6)
    assert r.hexists("test:hash", "field1") is False
    r.hset("test:hash", "field1", "value1")
    r.hpexpire("test:hash", 1000, "field1")
    assert r.hpexpire("test:hash", 500, "field1", gt=True) == [0]
    assert r.hpexpire("test:hash", 500, "field1", lt=True) == [1]
    time.sleep(0.6)
    assert r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.3.240")
def test_hpexpire_nonexistent_key_or_field(r):
    r.delete("test:hash")
    assert r.hpexpire("test:hash", 500, "field1") == [-2]
    r.hset("test:hash", "field1", "value1")
    assert r.hpexpire("test:hash", 500, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
def test_hpexpire_multiple_fields(r):
    r.delete("test:hash")
    r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert r.hpexpire("test:hash", 500, "field1", "field2") == [1, 1]
    time.sleep(0.6)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is False
    assert r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.3.240")
def test_hpexpire_multiple_condition_flags_error(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    with pytest.raises(ValueError) as e:
        r.hpexpire("test:hash", 500, "field1", nx=True, xx=True)
    assert "Only one of" in str(e)


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_basic(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert r.hexpireat("test:hash", exp_time, "field1") == [1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_with_datetime(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(seconds=1)
    assert r.hexpireat("test:hash", exp_time, "field1") == [1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    future_exp_time = int((datetime.now() + timedelta(seconds=2)).timestamp())
    past_exp_time = int((datetime.now() - timedelta(seconds=1)).timestamp())
    assert r.hexpireat("test:hash", future_exp_time, "field1", xx=True) == [0]
    assert r.hexpireat("test:hash", future_exp_time, "field1", nx=True) == [1]
    assert r.hexpireat("test:hash", past_exp_time, "field1", gt=True) == [0]
    assert r.hexpireat("test:hash", past_exp_time, "field1", lt=True) == [2]
    assert r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_nonexistent_key_or_field(r):
    r.delete("test:hash")
    future_exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert r.hexpireat("test:hash", future_exp_time, "field1") == [-2]
    r.hset("test:hash", "field1", "value1")
    assert r.hexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_multiple_fields(r):
    r.delete("test:hash")
    r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert r.hexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    time.sleep(1.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is False
    assert r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_multiple_condition_flags_error(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    with pytest.raises(ValueError) as e:
        r.hexpireat("test:hash", exp_time, "field1", nx=True, xx=True)
    assert "Only one of" in str(e)


@skip_if_server_version_lt("7.3.240")
def test_hpexpireat_basic(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(milliseconds=400)).timestamp() * 1000)
    assert r.hpexpireat("test:hash", exp_time, "field1") == [1]
    time.sleep(0.5)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hpexpireat_with_datetime(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(milliseconds=400)
    assert r.hpexpireat("test:hash", exp_time, "field1") == [1]
    time.sleep(0.5)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hpexpireat_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    past_exp_time = int(
        (datetime.now() - timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert r.hpexpireat("test:hash", future_exp_time, "field1", xx=True) == [0]
    assert r.hpexpireat("test:hash", future_exp_time, "field1", nx=True) == [1]
    assert r.hpexpireat("test:hash", past_exp_time, "field1", gt=True) == [0]
    assert r.hpexpireat("test:hash", past_exp_time, "field1", lt=True) == [2]
    assert r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.3.240")
def test_hpexpireat_nonexistent_key_or_field(r):
    r.delete("test:hash")
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert r.hpexpireat("test:hash", future_exp_time, "field1") == [-2]
    r.hset("test:hash", "field1", "value1")
    assert r.hpexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
def test_hpexpireat_multiple_fields(r):
    r.delete("test:hash")
    r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(milliseconds=400)).timestamp() * 1000)
    assert r.hpexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    time.sleep(0.5)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is False
    assert r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.3.240")
def test_hpexpireat_multiple_condition_flags_error(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1"})
    exp_time = int((datetime.now() + timedelta(milliseconds=500)).timestamp())
    with pytest.raises(ValueError) as e:
        r.hpexpireat("test:hash", exp_time, "field1", nx=True, xx=True)
    assert "Only one of" in str(e)


@skip_if_server_version_lt("7.3.240")
def test_hpersist_multiple_fields(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    r.hexpire("test:hash", 5000, "field1")
    assert r.hpersist("test:hash", "field1", "field2", "field3") == [1, -1, -2]


@skip_if_server_version_lt("7.3.240")
def test_hpersist_nonexistent_key(r):
    r.delete("test:hash")
    assert r.hpersist("test:hash", "field1", "field2", "field3") == [-2, -2, -2]


@skip_if_server_version_lt("7.3.240")
def test_hexpiretime_multiple_fields_mixed_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    r.hexpireat("test:hash", future_time, "field1")
    result = r.hexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time - 10 < result[0] <= future_time
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
def test_hexpiretime_nonexistent_key(r):
    r.delete("test:hash")
    assert r.hexpiretime("test:hash", "field1", "field2", "field3") == [-2, -2, -2]


@skip_if_server_version_lt("7.3.240")
def test_hpexpiretime_multiple_fields_mixed_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    r.hexpireat("test:hash", future_time, "field1")
    result = r.hpexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time * 1000 - 10000 < result[0] <= future_time * 1000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
def test_hpexpiretime_nonexistent_key(r):
    r.delete("test:hash")
    assert r.hpexpiretime("test:hash", "field1", "field2", "field3") == [-2, -2, -2]


@skip_if_server_version_lt("7.3.240")
def test_httl_multiple_fields_mixed_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    r.hexpireat("test:hash", future_time, "field1")
    result = r.httl("test:hash", "field1", "field2", "field3")
    assert 30 * 60 - 10 < result[0] <= 30 * 60
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
def test_httl_nonexistent_key(r):
    r.delete("test:hash")
    assert r.httl("test:hash", "field1", "field2", "field3") == [-2, -2, -2]


@skip_if_server_version_lt("7.3.240")
def test_hpttl_multiple_fields_mixed_conditions(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    r.hexpireat("test:hash", future_time, "field1")
    result = r.hpttl("test:hash", "field1", "field2", "field3")
    assert 30 * 60000 - 10000 < result[0] <= 30 * 60000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
def test_hpttl_nonexistent_key(r):
    r.delete("test:hash")
    assert r.hpttl("test:hash", "field1", "field2", "field3") == [-2, -2, -2]
