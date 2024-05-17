import asyncio
from datetime import datetime, timedelta

import pytest
from tests.conftest import skip_if_server_version_lt


@skip_if_server_version_lt("7.4.0")
async def test_hexpire_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hexpire("test:hash", 1, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hexpire_with_timedelta(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hexpire("test:hash", timedelta(seconds=1), "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hexpire_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    assert await r.hexpire("test:hash", 2, "field1", xx=True) == [0]
    assert await r.hexpire("test:hash", 2, "field1", nx=True) == [1]
    assert await r.hexpire("test:hash", 1, "field1", xx=True) == [1]
    assert await r.hexpire("test:hash", 2, "field1", nx=True) == [0]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    await r.hset("test:hash", "field1", "value1")
    await r.hexpire("test:hash", 2, "field1")
    assert await r.hexpire("test:hash", 1, "field1", gt=True) == [0]
    assert await r.hexpire("test:hash", 1, "field1", lt=True) == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.4.0")
async def test_hexpire_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hexpire("test:hash", 1, "field1") is None
    await r.hset("test:hash", "field1", "value1")
    assert await r.hexpire("test:hash", 1, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.4.0")
async def test_hexpire_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hexpire("test:hash", 1, "field1", "field2") == [1, 1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpexpire_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hpexpire("test:hash", 500, "field1") == [1]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpexpire_with_timedelta(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hpexpire("test:hash", timedelta(milliseconds=500), "field1") == [1]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpexpire_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    assert await r.hpexpire("test:hash", 1500, "field1", xx=True) == [0]
    assert await r.hpexpire("test:hash", 1500, "field1", nx=True) == [1]
    assert await r.hpexpire("test:hash", 500, "field1", xx=True) == [1]
    assert await r.hpexpire("test:hash", 1500, "field1", nx=True) == [0]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    await r.hset("test:hash", "field1", "value1")
    await r.hpexpire("test:hash", 1000, "field1")
    assert await r.hpexpire("test:hash", 500, "field1", gt=True) == [0]
    assert await r.hpexpire("test:hash", 500, "field1", lt=True) == [1]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.4.0")
async def test_hpexpire_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hpexpire("test:hash", 500, "field1") is None
    await r.hset("test:hash", "field1", "value1")
    assert await r.hpexpire("test:hash", 500, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.4.0")
async def test_hpexpire_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hpexpire("test:hash", 500, "field1", "field2") == [1, 1]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hexpireat_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hexpireat_with_datetime(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(seconds=1)
    assert await r.hexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hexpireat_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    future_exp_time = int((datetime.now() + timedelta(seconds=2)).timestamp())
    past_exp_time = int((datetime.now() - timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", future_exp_time, "field1", xx=True) == [0]
    assert await r.hexpireat("test:hash", future_exp_time, "field1", nx=True) == [1]
    assert await r.hexpireat("test:hash", past_exp_time, "field1", gt=True) == [0]
    assert await r.hexpireat("test:hash", past_exp_time, "field1", lt=True) == [2]
    assert await r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.4.0")
async def test_hexpireat_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    future_exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", future_exp_time, "field1") is None
    await r.hset("test:hash", "field1", "value1")
    assert await r.hexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.4.0")
async def test_hexpireat_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpexpireat_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(milliseconds=400)).timestamp() * 1000)
    assert await r.hpexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(0.5)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpexpireat_with_datetime(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(milliseconds=400)
    assert await r.hpexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(0.5)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpexpireat_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    past_exp_time = int(
        (datetime.now() - timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert await r.hpexpireat("test:hash", future_exp_time, "field1", xx=True) == [0]
    assert await r.hpexpireat("test:hash", future_exp_time, "field1", nx=True) == [1]
    assert await r.hpexpireat("test:hash", past_exp_time, "field1", gt=True) == [0]
    assert await r.hpexpireat("test:hash", past_exp_time, "field1", lt=True) == [2]
    assert await r.hexists("test:hash", "field1") is False


@skip_if_server_version_lt("7.4.0")
async def test_hpexpireat_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert await r.hpexpireat("test:hash", future_exp_time, "field1") is None
    await r.hset("test:hash", "field1", "value1")
    assert await r.hpexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.4.0")
async def test_hpexpireat_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(milliseconds=400)).timestamp() * 1000)
    assert await r.hpexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    await asyncio.sleep(0.5)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hpersist_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    await r.hexpire("test:hash", 5000, "field1")
    assert await r.hpersist("test:hash", "field1", "field2", "field3") == [1, -1, -2]


@skip_if_server_version_lt("7.4.0")
async def test_hexpiretime_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time - 10 < result[0] <= future_time
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.4.0")
async def test_hpexpiretime_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hpexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time * 1000 - 10000 < result[0] <= future_time * 1000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.4.0")
async def test_ttl_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.httl("test:hash", "field1", "field2", "field3")
    assert 30 * 60 - 10 < result[0] <= 30 * 60
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.4.0")
async def test_pttl_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hpttl("test:hash", "field1", "field2", "field3")
    assert 30 * 60000 - 10000 < result[0] <= 30 * 60000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_no_expiration_setting(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hgetf("test:hash", "field1", "field2") == [b"value1", b"value2"]


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_seconds(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hgetf("test:hash", "field1", "field2", seconds=1) == [
        b"value1",
        b"value2",
    ]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_seconds_timedelta(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hgetf(
        "test:hash", "field1", "field2", seconds=timedelta(seconds=1)
    ) == [
        b"value1",
        b"value2",
    ]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_milliseconds(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hgetf("test:hash", "field1", "field2", milliseconds=500) == [
        b"value1",
        b"value2",
    ]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_milliseconds_timedelta(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hgetf(
        "test:hash", "field1", "field2", milliseconds=timedelta(milliseconds=500)
    ) == [b"value1", b"value2"]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_unixtime_seconds(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hgetf(
        "test:hash", "field1", "field2", unix_time_seconds=exp_time
    ) == [
        b"value1",
        b"value2",
    ]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_unixtime_seconds_datetime(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = datetime.now() + timedelta(seconds=1)
    assert await r.hgetf(
        "test:hash", "field1", "field2", unix_time_seconds=exp_time
    ) == [
        b"value1",
        b"value2",
    ]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_unixtime_milliseconds(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(milliseconds=500)).timestamp())
    assert await r.hgetf(
        "test:hash", "field1", "field2", unix_time_milliseconds=exp_time
    ) == [b"value1", b"value2"]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_expiration_unixtime_milliseconds_timedelta(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = datetime.now() + timedelta(milliseconds=500)
    assert await r.hgetf(
        "test:hash", "field1", "field2", unix_time_milliseconds=exp_time
    ) == [b"value1", b"value2"]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_persist(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    await r.hexpire("test:hash", 10, "field1", "field2", "field3")
    ttls = await r.httl("test:hash", "field1", "field2", "field3")
    assert all(0 < ttl <= 10 for ttl in ttls)
    await r.hgetf("test:hash", "field1", "field2", persist=True)
    ttls = await r.httl("test:hash", "field1", "field2", "field3")
    assert ttls[:2] == [-1, -1]
    assert ttls[2] > 0


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_multiple_expiration_options_error(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    with pytest.raises(ValueError) as e:
        await r.hgetf("test:hash", "field1", seconds=10, milliseconds=10000)
    assert "Only one expiration setting" in str(e)


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_multiple_condition_flags_error(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1"})
    with pytest.raises(ValueError) as e:
        await r.hgetf("test:hash", "field1", nx=True, xx=True)
    assert "Only one of" in str(e)


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hgetf("test:hash", "field1") is None
    await r.hset("test:hash", "field1", "value1")
    assert await r.hgetf("test:hash", "nonexistent_field") == [None]


@skip_if_server_version_lt("7.4.0")
async def test_hgetf_conditions(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    expected_response = [b"value1", b"value2"]
    assert (
        await r.hgetf("test:hash", "field1", "field2", seconds=2, xx=True)
        == expected_response
    )
    assert (
        await r.hgetf("test:hash", "field1", "field2", seconds=2, nx=True)
        == expected_response
    )
    assert (
        await r.hgetf("test:hash", "field1", "field2", seconds=1, xx=True)
        == expected_response
    )
    assert (
        await r.hgetf("test:hash", "field1", "field2", seconds=2, nx=True)
        == expected_response
    )
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    await r.hexpire("test:hash", 2, "field1", "field2")
    assert (
        await r.hgetf("test:hash", "field1", "field2", seconds=1, gt=True)
        == expected_response
    )
    assert (
        await r.hgetf("test:hash", "field1", "field2", seconds=1, lt=True)
        == expected_response
    )
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_basic(r):
    await r.delete("test:hash")
    assert await r.hsetf("test:hash", {"field1": "value1", "field2": "value2"}) == [
        1,
        1,
    ]
    assert await r.hget("test:hash", "field1") == b"value1"
    assert await r.hget("test:hash", "field2") == b"value2"


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_seconds(r):
    await r.delete("test:hash")
    assert await r.hsetf(
        "test:hash", {"field1": "value1", "field2": "value2"}, seconds=1
    ) == [3, 3]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_seconds_timedelta(r):
    await r.delete("test:hash")
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        seconds=timedelta(seconds=1),
    ) == [3, 3]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_milliseconds(r):
    await r.delete("test:hash")
    assert await r.hsetf(
        "test:hash", {"field1": "value1", "field2": "value2"}, milliseconds=500
    ) == [3, 3]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_milliseconds_timedelta(r):
    await r.delete("test:hash")
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        milliseconds=timedelta(milliseconds=500),
    ) == [3, 3]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_unixtime_seconds(r):
    await r.delete("test:hash")
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        unix_time_seconds=exp_time,
    ) == [3, 3]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_unixtime_seconds_datetime(r):
    await r.delete("test:hash")
    exp_time = datetime.now() + timedelta(seconds=1)
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        unix_time_seconds=exp_time,
    ) == [3, 3]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_unixtime_milliseconds(r):
    await r.delete("test:hash")
    exp_time = int((datetime.now() + timedelta(milliseconds=500)).timestamp())
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        unix_time_milliseconds=exp_time,
    ) == [3, 3]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_expiration_unixtime_milliseconds_datetime(r):
    await r.delete("test:hash")
    exp_time = datetime.now() + timedelta(milliseconds=500)
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        unix_time_milliseconds=exp_time,
    ) == [3, 3]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_keep_ttl(r):
    await r.delete("test:hash")
    exp_time = datetime.now() + timedelta(seconds=10000)
    assert await r.hsetf(
        "test:hash",
        {"field1": "value1", "field2": "value2"},
        unix_time_seconds=exp_time,
    ) == [3, 3]
    assert await r.hexpiretime("test:hash", "field1") == [int(exp_time.timestamp())]
    assert await r.hexpiretime("test:hash", "field2") == [int(exp_time.timestamp())]
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        keep_ttl=True,
    ) == [1, 1]
    assert await r.hexpiretime("test:hash", "field1") == [int(exp_time.timestamp())]
    assert await r.hexpiretime("test:hash", "field2") == [int(exp_time.timestamp())]


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_multiple_field_update_flags_error(r):
    await r.delete("test:hash")
    with pytest.raises(ValueError) as e:
        await r.hsetf(
            "test:hash",
            {"field1": "new_value1"},
            no_create_fields=True,
            no_overwrite_fields=True,
        )
    assert "Only one of" in str(e.value)


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_multiple_return_flags_error(r):
    await r.delete("test:hash")
    with pytest.raises(ValueError) as e:
        await r.hsetf("test:hash", {"field1": "new_value1"}, get_old=True, get_new=True)
    assert "Only one of" in str(e.value)


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_multiple_condition_flags_error(r):
    await r.delete("test:hash")
    with pytest.raises(ValueError) as e:
        await r.hsetf("test:hash", {"field1": "new_value1"}, nx=True, xx=True)
    assert "Only one of" in str(e.value)


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_multiple_expiration_options_error(r):
    await r.delete("test:hash")
    with pytest.raises(ValueError) as e:
        await r.hsetf(
            "test:hash", {"field1": "new_value1"}, seconds=10, milliseconds=10000
        )
    assert "Only one expiration setting" in str(e.value)


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_nonexistent_key(r):
    await r.delete("test:hash")
    result = await r.hsetf(
        "test:hash", {"nonexistent_field": "value1"}, no_create_key=True
    )
    assert result is None
    assert await r.exists("test:hash") == 0
    assert await r.hsetf("test:hash", {"nonexistent_field": "value1"}) == [1]
    assert await r.hget("test:hash", "nonexistent_field") == b"value1"


@skip_if_server_version_lt("7.4.0")
async def test_hsetf_conditions(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        seconds=2,
        xx=True,
    ) == [1, 1]
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        seconds=2,
        nx=True,
    ) == [3, 3]
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        seconds=1,
        xx=True,
    ) == [3, 3]
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        seconds=2,
        nx=True,
    ) == [1, 1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    await r.hexpire("test:hash", 2, "field1", "field2")
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        seconds=1,
        gt=True,
    ) == [1, 1]
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        seconds=1,
        lt=True,
    ) == [3, 3]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@pytest.mark.skip_if_server_version_lt("7.4.0")
async def test_hsetf_field_update_flags(r):
    await r.delete("test:hash")
    assert await r.hset("test:hash", "field1", "value1") == 1
    assert await r.hsetf("test:hash", {"field2": "value2"}, no_create_fields=True) == [
        0
    ]
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "value2"},
        no_overwrite_fields=True,
    ) == [0, 1]
    assert await r.hget("test:hash", "field1") == b"value1"
    assert await r.hget("test:hash", "field2") == b"value2"


@skip_if_server_version_lt("7.4.0")
async def test_hsetf_get_old_and_new(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    assert await r.hsetf(
        "test:hash",
        {"field1": "new_value1", "field2": "new_value2"},
        get_old=True,
    ) == [b"value1", b"value2"]
    assert await r.hsetf(
        "test:hash",
        {"field1": "newer_value1", "field2": "newer_value2"},
        get_new=True,
    ) == [b"newer_value1", b"newer_value2"]
