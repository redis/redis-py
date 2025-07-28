import math
import time
from datetime import datetime, timedelta

import pytest
from redis import exceptions
from redis.commands.core import HashDataPersistOptions
from tests.conftest import skip_if_server_version_lt
from tests.test_utils import redis_server_time


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
    exp_time = math.ceil((datetime.now() + timedelta(seconds=1)).timestamp())
    assert r.hexpireat("test:hash", exp_time, "field1") == [1]
    time.sleep(2.1)
    assert r.hexists("test:hash", "field1") is False
    assert r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
def test_hexpireat_with_datetime(r):
    r.delete("test:hash")
    r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = (datetime.now() + timedelta(seconds=2)).replace(microsecond=0)
    assert r.hexpireat("test:hash", exp_time, "field1") == [1]
    time.sleep(2.1)
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
    exp_time = math.ceil((datetime.now() + timedelta(seconds=1)).timestamp())
    assert r.hexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    time.sleep(2.1)
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


@skip_if_server_version_lt("7.9.0")
def test_hgetdel(r):
    r.delete("test:hash")
    r.hset("test:hash", "foo", "bar", mapping={"1": 1, "2": 2})
    assert r.hgetdel("test:hash", "foo", "1") == [b"bar", b"1"]
    assert r.hget("test:hash", "foo") is None
    assert r.hget("test:hash", "1") is None
    assert r.hget("test:hash", "2") == b"2"
    assert r.hgetdel("test:hash", "foo", "1") == [None, None]
    assert r.hget("test:hash", "2") == b"2"

    with pytest.raises(exceptions.DataError):
        r.hgetdel("test:hash")


@skip_if_server_version_lt("7.9.0")
def test_hgetex_no_expiration(r):
    r.delete("test:hash")
    r.hset("b", "foo", "bar", mapping={"1": 1, "2": 2, "3": "three", "4": b"four"})

    assert r.hgetex("b", "foo", "1", "4") == [b"bar", b"1", b"four"]
    assert r.httl("b", "foo", "1", "4") == [-1, -1, -1]


@skip_if_server_version_lt("7.9.0")
def test_hgetex_expiration_configs(r):
    r.delete("test:hash")
    r.hset("test:hash", "foo", "bar", mapping={"1": 1, "3": "three", "4": b"four"})
    test_keys = ["foo", "1", "4"]

    # test get with multiple fields with expiration set through 'ex'
    assert r.hgetex("test:hash", *test_keys, ex=10) == [b"bar", b"1", b"four"]
    ttls = r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 10

    # test get with multiple fields removing expiration settings with 'persist'
    assert r.hgetex("test:hash", *test_keys, persist=True) == [
        b"bar",
        b"1",
        b"four",
    ]
    assert r.httl("test:hash", *test_keys) == [-1, -1, -1]

    # test get with multiple fields with expiration set through 'px'
    assert r.hgetex("test:hash", *test_keys, px=6000) == [b"bar", b"1", b"four"]
    ttls = r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 6

    # test get single field with expiration set through 'pxat'
    expire_at = redis_server_time(r) + timedelta(minutes=1)
    assert r.hgetex("test:hash", "foo", pxat=expire_at) == [b"bar"]
    assert r.httl("test:hash", "foo")[0] <= 61

    # test get single field with expiration set through 'exat'
    expire_at = redis_server_time(r) + timedelta(seconds=10)
    assert r.hgetex("test:hash", "foo", exat=expire_at) == [b"bar"]
    assert r.httl("test:hash", "foo")[0] <= 10


@skip_if_server_version_lt("7.9.0")
def test_hgetex_validate_expired_fields_removed(r):
    r.delete("test:hash")
    r.hset("test:hash", "foo", "bar", mapping={"1": 1, "3": "three", "4": b"four"})

    test_keys = ["foo", "1", "3"]
    # test get multiple fields with expiration set
    # validate that expired fields are removed
    assert r.hgetex("test:hash", *test_keys, ex=1) == [b"bar", b"1", b"three"]
    time.sleep(1.1)
    assert r.hgetex("test:hash", *test_keys) == [None, None, None]
    assert r.httl("test:hash", *test_keys) == [-2, -2, -2]
    assert r.hgetex("test:hash", "4") == [b"four"]


@skip_if_server_version_lt("7.9.0")
def test_hgetex_invalid_inputs(r):
    with pytest.raises(exceptions.DataError):
        r.hgetex("b", "foo", "1", "3", ex=10, persist=True)

    with pytest.raises(exceptions.DataError):
        r.hgetex("b", "foo", ex=10.0, persist=True)

    with pytest.raises(exceptions.DataError):
        r.hgetex("b", "foo", ex=10, px=6000)

    with pytest.raises(exceptions.DataError):
        r.hgetex("b", ex=10)


@skip_if_server_version_lt("7.9.0")
def test_hsetex_no_expiration(r):
    r.delete("test:hash")

    # # set items from mapping without expiration
    assert r.hsetex("test:hash", None, None, mapping={"1": 1, "4": b"four"}) == 1
    assert r.httl("test:hash", "foo", "1", "4") == [-2, -1, -1]
    assert r.hgetex("test:hash", "foo", "1") == [None, b"1"]


@skip_if_server_version_lt("7.9.0")
def test_hsetex_expiration_ex_and_keepttl(r):
    r.delete("test:hash")

    # set items from key/value provided
    # combined with mapping and items with expiration - testing ex field
    assert (
        r.hsetex(
            "test:hash",
            "foo",
            "bar",
            mapping={"1": 1, "2": "2"},
            items=["i1", 11, "i2", 22],
            ex=10,
        )
        == 1
    )
    ttls = r.httl("test:hash", "foo", "1", "2", "i1", "i2")
    for ttl in ttls:
        assert pytest.approx(ttl) == 10

    assert r.hgetex("test:hash", "foo", "1", "2", "i1", "i2") == [
        b"bar",
        b"1",
        b"2",
        b"11",
        b"22",
    ]
    time.sleep(1.1)
    # validate keepttl
    assert r.hsetex("test:hash", "foo", "bar1", keepttl=True) == 1
    assert r.httl("test:hash", "foo")[0] < 10


@skip_if_server_version_lt("7.9.0")
def test_hsetex_expiration_px(r):
    r.delete("test:hash")
    # set items from key/value provided and mapping
    # with expiration - testing px field
    assert (
        r.hsetex("test:hash", "foo", "bar", mapping={"1": 1, "2": "2"}, px=60000) == 1
    )
    test_keys = ["foo", "1", "2"]
    ttls = r.httl("test:hash", *test_keys)
    for ttl in ttls:
        assert pytest.approx(ttl) == 60
    assert r.hgetex("test:hash", *test_keys) == [b"bar", b"1", b"2"]


@skip_if_server_version_lt("7.9.0")
def test_hsetex_expiration_pxat_and_fnx(r):
    r.delete("test:hash")
    assert r.hsetex("test:hash", "foo", "bar", mapping={"1": 1, "2": "2"}, ex=30) == 1

    expire_at = redis_server_time(r) + timedelta(minutes=1)
    assert (
        r.hsetex(
            "test:hash",
            "foo",
            "bar1",
            mapping={"new": "ok"},
            pxat=expire_at,
            data_persist_option=HashDataPersistOptions.FNX,
        )
        == 0
    )
    ttls = r.httl("test:hash", "foo", "new")
    assert ttls[0] <= 30
    assert ttls[1] == -2

    assert r.hgetex("test:hash", "foo", "1", "new") == [b"bar", b"1", None]
    assert (
        r.hsetex(
            "test:hash",
            "foo_new",
            "bar1",
            mapping={"new": "ok"},
            pxat=expire_at,
            data_persist_option=HashDataPersistOptions.FNX,
        )
        == 1
    )
    ttls = r.httl("test:hash", "foo", "new")
    for ttl in ttls:
        assert ttl <= 61
    assert r.hgetex("test:hash", "foo", "foo_new", "new") == [
        b"bar",
        b"bar1",
        b"ok",
    ]


@skip_if_server_version_lt("7.9.0")
def test_hsetex_expiration_exat_and_fxx(r):
    r.delete("test:hash")
    assert r.hsetex("test:hash", "foo", "bar", mapping={"1": 1, "2": "2"}, ex=30) == 1

    expire_at = redis_server_time(r) + timedelta(seconds=10)
    assert (
        r.hsetex(
            "test:hash",
            "foo",
            "bar1",
            mapping={"new": "ok"},
            exat=expire_at,
            data_persist_option=HashDataPersistOptions.FXX,
        )
        == 0
    )
    ttls = r.httl("test:hash", "foo", "new")
    assert 10 < ttls[0] <= 30
    assert ttls[1] == -2

    assert r.hgetex("test:hash", "foo", "1", "new") == [b"bar", b"1", None]
    assert (
        r.hsetex(
            "test:hash",
            "foo",
            "bar1",
            mapping={"1": "new_value"},
            exat=expire_at,
            data_persist_option=HashDataPersistOptions.FXX,
        )
        == 1
    )
    assert r.hgetex("test:hash", "foo", "1") == [b"bar1", b"new_value"]


@skip_if_server_version_lt("7.9.0")
def test_hsetex_invalid_inputs(r):
    with pytest.raises(exceptions.DataError):
        r.hsetex("b", "foo", "bar", ex=10.0)

    with pytest.raises(exceptions.DataError):
        r.hsetex("b", None, None)

    with pytest.raises(exceptions.DataError):
        r.hsetex("b", "foo", "bar", items=["i1", 11, "i2"], px=6000)

    with pytest.raises(exceptions.DataError):
        r.hsetex("b", "foo", "bar", ex=10, keepttl=True)
