import asyncio
from datetime import datetime, timedelta

from tests.conftest import skip_if_server_version_lt


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hexpire("test:hash", 1, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_with_timedelta(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hexpire("test:hash", timedelta(seconds=1), "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hexpire_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hexpire("test:hash", 1, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hexpire("test:hash", 1, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hpexpire("test:hash", 500, "field1") == [1]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_with_timedelta(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    assert await r.hpexpire("test:hash", timedelta(milliseconds=500), "field1") == [1]
    await asyncio.sleep(0.6)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hpexpire_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    assert await r.hpexpire("test:hash", 500, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hpexpire("test:hash", 500, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_with_datetime(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(seconds=1)
    assert await r.hexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(1.1)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    future_exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", future_exp_time, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
async def test_hexpireat_multiple_fields(r):
    await r.delete("test:hash")
    await r.hset(
        "test:hash",
        mapping={"field1": "value1", "field2": "value2", "field3": "value3"},
    )
    exp_time = int((datetime.now() + timedelta(seconds=1)).timestamp())
    assert await r.hexpireat("test:hash", exp_time, "field1", "field2") == [1, 1]
    await asyncio.sleep(1.5)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is False
    assert await r.hexists("test:hash", "field3") is True


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_basic(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = int((datetime.now() + timedelta(milliseconds=400)).timestamp() * 1000)
    assert await r.hpexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(0.5)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_with_datetime(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    exp_time = datetime.now() + timedelta(milliseconds=400)
    assert await r.hpexpireat("test:hash", exp_time, "field1") == [1]
    await asyncio.sleep(0.5)
    assert await r.hexists("test:hash", "field1") is False
    assert await r.hexists("test:hash", "field2") is True


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hpexpireat_nonexistent_key_or_field(r):
    await r.delete("test:hash")
    future_exp_time = int(
        (datetime.now() + timedelta(milliseconds=500)).timestamp() * 1000
    )
    assert await r.hpexpireat("test:hash", future_exp_time, "field1") == [-2]
    await r.hset("test:hash", "field1", "value1")
    assert await r.hpexpireat("test:hash", future_exp_time, "nonexistent_field") == [-2]


@skip_if_server_version_lt("7.3.240")
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


@skip_if_server_version_lt("7.3.240")
async def test_hpersist_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    await r.hexpire("test:hash", 5000, "field1")
    assert await r.hpersist("test:hash", "field1", "field2", "field3") == [1, -1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_hexpiretime_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time - 10 < result[0] <= future_time
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_hpexpiretime_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hpexpiretime("test:hash", "field1", "field2", "field3")
    assert future_time * 1000 - 10000 < result[0] <= future_time * 1000
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_ttl_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.httl("test:hash", "field1", "field2", "field3")
    assert 30 * 60 - 10 < result[0] <= 30 * 60
    assert result[1:] == [-1, -2]


@skip_if_server_version_lt("7.3.240")
async def test_pttl_multiple_fields_mixed_conditions(r):
    await r.delete("test:hash")
    await r.hset("test:hash", mapping={"field1": "value1", "field2": "value2"})
    future_time = int((datetime.now() + timedelta(minutes=30)).timestamp())
    await r.hexpireat("test:hash", future_time, "field1")
    result = await r.hpttl("test:hash", "field1", "field2", "field3")
    assert 30 * 60000 - 10000 < result[0] <= 30 * 60000
    assert result[1:] == [-1, -2]
