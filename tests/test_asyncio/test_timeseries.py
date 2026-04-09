import time
from time import sleep

import pytest
import pytest_asyncio
import redis.asyncio as redis
from tests.conftest import (
    is_resp2_connection,
    skip_if_server_version_gte,
    skip_if_server_version_lt,
    skip_ifmodversion_lt,
)

KEY1 = "key1"
KEY2 = "key2"
KEY3 = "key3"
KEY4 = "key4"
KEY5 = "key5"


@pytest_asyncio.fixture()
async def decoded_r(create_redis, stack_url):
    return await create_redis(decode_responses=True, url=stack_url)


@pytest.mark.redismod
async def test_create(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    assert await decoded_r.ts().create(KEY2, retention_msecs=5)
    assert await decoded_r.ts().create(KEY3, labels={"Redis": "Labs"})
    assert await decoded_r.ts().create(
        KEY4, retention_msecs=20, labels={"Time": "Series"}
    )
    info = await decoded_r.ts().info(KEY4)
    assert 20 == info.retention_msecs
    assert "Series" == info["labels"]["Time"]

    # Test for a chunk size of 128 Bytes
    assert await decoded_r.ts().create("time-serie-1", chunk_size=128)
    info = await decoded_r.ts().info("time-serie-1")
    assert 128 == info.chunk_size


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_create_duplicate_policy(decoded_r: redis.Redis):
    # Test for duplicate policy
    for duplicate_policy in ["block", "last", "first", "min", "max"]:
        ts_name = f"time-serie-ooo-{duplicate_policy}"
        assert await decoded_r.ts().create(ts_name, duplicate_policy=duplicate_policy)
        info = await decoded_r.ts().info(ts_name)
        assert duplicate_policy == info.get("duplicate_policy")


@pytest.mark.redismod
async def test_alter(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    info = await decoded_r.ts().info(KEY1)
    assert 0 == info.get("retention_msecs")
    assert await decoded_r.ts().alter(KEY1, retention_msecs=10)
    assert {} == (await decoded_r.ts().info(KEY1))["labels"]
    info = await decoded_r.ts().info(KEY1)
    assert 10 == info.get("retention_msecs")
    assert await decoded_r.ts().alter(KEY1, labels={"Time": "Series"})
    assert "Series" == (await decoded_r.ts().info(KEY1))["labels"]["Time"]
    info = await decoded_r.ts().info(KEY1)
    assert 10 == info.get("retention_msecs")


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_lt("7.9.0")
async def test_alter_duplicate_policy(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    info = await decoded_r.ts().info(KEY1)
    assert "block" == info.get("duplicate_policy")
    assert await decoded_r.ts().alter(KEY1, duplicate_policy="min")
    info = await decoded_r.ts().info(KEY1)
    assert "min" == info.get("duplicate_policy")


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_gte("7.9.0")
async def test_alter_duplicate_policy_prior_redis_8(decoded_r: redis.Redis):
    assert await decoded_r.ts().create(KEY1)
    info = await decoded_r.ts().info(KEY1)
    assert None is info.get("duplicate_policy")
    assert await decoded_r.ts().alter(KEY1, duplicate_policy="min")
    info = await decoded_r.ts().info(KEY1)
    assert "min" == info.get("duplicate_policy")


@pytest.mark.redismod
async def test_add(decoded_r: redis.Redis):
    assert 1 == await decoded_r.ts().add(KEY1, 1, 1)
    assert 2 == await decoded_r.ts().add(KEY2, 2, 3, retention_msecs=10)
    assert 3 == await decoded_r.ts().add(KEY3, 3, 2, labels={"Redis": "Labs"})
    assert 4 == await decoded_r.ts().add(
        KEY4, 4, 2, retention_msecs=10, labels={"Redis": "Labs", "Time": "Series"}
    )
    res = await decoded_r.ts().add(KEY5, "*", 1)
    assert abs(time.time() - round(float(res) / 1000)) < 1.0

    info = await decoded_r.ts().info(KEY4)
    assert 10 == info.get("retention_msecs")
    assert "Labs" == info["labels"]["Redis"]

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert await decoded_r.ts().add("time-serie-1", 1, 10.0, chunk_size=128)
    info = await decoded_r.ts().info("time-serie-1")
    assert 128 == info.get("chunk_size")


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_add_duplicate_policy(decoded_r: redis.Redis):
    # Test for duplicate policy BLOCK
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-block", 1, 5.0)
    with pytest.raises(Exception):
        await decoded_r.ts().add(
            "time-serie-add-ooo-block", 1, 5.0, on_duplicate="block"
        )

    # Test for duplicate policy LAST
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-last", 1, 10.0, on_duplicate="last"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-last")
    assert res is not None
    assert 10.0 == res[1]

    # Test for duplicate policy FIRST
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-first", 1, 10.0, on_duplicate="first"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-first")
    assert res is not None
    assert 5.0 == res[1]

    # Test for duplicate policy MAX
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-max", 1, 10.0, on_duplicate="max"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-max")
    assert res is not None
    assert 10.0 == res[1]

    # Test for duplicate policy MIN
    assert 1 == await decoded_r.ts().add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == await decoded_r.ts().add(
        "time-serie-add-ooo-min", 1, 10.0, on_duplicate="min"
    )
    res = await decoded_r.ts().get("time-serie-add-ooo-min")
    assert res is not None
    assert 5.0 == res[1]


@pytest.mark.redismod
async def test_madd(decoded_r: redis.Redis):
    await decoded_r.ts().create("a")
    assert [1, 2, 3] == await decoded_r.ts().madd(
        [("a", 1, 5), ("a", 2, 10), ("a", 3, 15)]
    )


@pytest.mark.redismod
async def test_incrby_decrby(decoded_r: redis.Redis):
    for _ in range(100):
        assert await decoded_r.ts().incrby(KEY1, 1)
        sleep(0.001)
    assert 100 == (await decoded_r.ts().get(KEY1))[1]
    for _ in range(100):
        assert await decoded_r.ts().decrby(KEY1, 1)
        sleep(0.001)
    assert 0 == (await decoded_r.ts().get(KEY1))[1]

    assert await decoded_r.ts().incrby(KEY2, 1.5, timestamp=5)
    assert [5, 1.5] == await decoded_r.ts().get(KEY2)
    assert await decoded_r.ts().incrby(KEY2, 2.25, timestamp=7)
    assert [7, 3.75] == await decoded_r.ts().get(KEY2)
    assert await decoded_r.ts().decrby(KEY2, 1.5, timestamp=15)
    assert [15, 2.25] == await decoded_r.ts().get(KEY2)

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert await decoded_r.ts().incrby("time-serie-1", 10, chunk_size=128)
    info = await decoded_r.ts().info("time-serie-1")
    assert 128 == info.get("chunk_size")

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert await decoded_r.ts().decrby("time-serie-2", 10, chunk_size=128)
    info = await decoded_r.ts().info("time-serie-2")
    assert 128 == info.get("chunk_size")


@pytest.mark.redismod
async def test_create_and_delete_rule(decoded_r: redis.Redis):
    # test rule creation
    time = 100
    await decoded_r.ts().create(KEY1)
    await decoded_r.ts().create(KEY2)
    await decoded_r.ts().createrule(KEY1, KEY2, "avg", 100)
    for i in range(50):
        await decoded_r.ts().add(KEY1, time + i * 2, 1)
        await decoded_r.ts().add(KEY1, time + i * 2 + 1, 2)
    await decoded_r.ts().add(KEY1, time * 2, 1.5)
    assert round((await decoded_r.ts().get(KEY2))[1], 5) == 1.5
    info = await decoded_r.ts().info(KEY1)
    assert info.rules[KEY2][0] == 100

    # test rule deletion
    await decoded_r.ts().deleterule(KEY1, KEY2)
    info = await decoded_r.ts().info(KEY1)
    assert not info["rules"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_del_range(decoded_r: redis.Redis):
    try:
        await decoded_r.ts().delete("test", 0, 100)
    except Exception as e:
        assert e.__str__() != ""

    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
    assert 22 == await decoded_r.ts().delete(KEY1, 0, 21)
    assert [] == await decoded_r.ts().range(KEY1, 0, 21)
    assert [[22, 1.0]] == await decoded_r.ts().range(KEY1, 22, 22)


@pytest.mark.redismod
async def test_range(decoded_r: redis.Redis):
    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
    assert 100 == len(await decoded_r.ts().range(KEY1, 0, 200))
    for i in range(100):
        await decoded_r.ts().add(KEY1, i + 200, i % 7)
    assert 200 == len(await decoded_r.ts().range(KEY1, 0, 500))
    # last sample isn't returned
    assert 20 == len(
        await decoded_r.ts().range(
            KEY1, 0, 500, aggregation_type="avg", bucket_size_msec=10
        )
    )
    assert 10 == len(await decoded_r.ts().range(KEY1, 0, 500, count=10))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_range_advanced(decoded_r: redis.Redis):
    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
        await decoded_r.ts().add(KEY1, i + 200, i % 7)

    assert 2 == len(
        await decoded_r.ts().range(
            KEY1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    res = await decoded_r.ts().range(
        KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert [[0, 10.0], [10, 1.0]] == res
    res = await decoded_r.ts().range(
        KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=5
    )
    assert [[0, 5.0], [5, 6.0]] == res
    res = await decoded_r.ts().range(
        KEY1, 0, 10, aggregation_type="twa", bucket_size_msec=10
    )
    assert [[0, 2.55], [10, 3.0]] == res


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_rev_range(decoded_r: redis.Redis):
    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
    assert 100 == len(await decoded_r.ts().range(KEY1, 0, 200))
    for i in range(100):
        await decoded_r.ts().add(KEY1, i + 200, i % 7)
    assert 200 == len(await decoded_r.ts().range(KEY1, 0, 500))
    # first sample isn't returned
    assert 20 == len(
        await decoded_r.ts().revrange(
            KEY1, 0, 500, aggregation_type="avg", bucket_size_msec=10
        )
    )
    assert 10 == len(await decoded_r.ts().revrange(KEY1, 0, 500, count=10))
    assert 2 == len(
        await decoded_r.ts().revrange(
            KEY1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert [[10, 1.0], [0, 10.0]] == await decoded_r.ts().revrange(
        KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert [[1, 10.0], [0, 1.0]] == await decoded_r.ts().revrange(
        KEY1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=1
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_multi_range(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This", "team": "ny"})
    await decoded_r.ts().create(
        KEY2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
        await decoded_r.ts().add(KEY2, i, i % 11)

    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    assert 100 == len(res[KEY1][2])

    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], count=10)
    assert 10 == len(res[KEY1][2])

    for i in range(100):
        await decoded_r.ts().add(KEY1, i + 200, i % 7)
    res = await decoded_r.ts().mrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    assert 20 == len(res[KEY1][2])

    # test withlabels
    assert {} == res[KEY1][0]
    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], with_labels=True)
    assert {"Test": "This", "team": "ny"} == res[KEY1][0]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_multi_range_advanced(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This", "team": "ny"})
    await decoded_r.ts().create(
        KEY2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
        await decoded_r.ts().add(KEY2, i, i % 11)

    # test with selected labels
    res = await decoded_r.ts().mrange(
        0, 200, filters=["Test=This"], select_labels=["team"]
    )
    assert {"team": "ny"} == res[KEY1][0]
    assert {"team": "sf"} == res[KEY2][0]

    # test with filterby
    res = await decoded_r.ts().mrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    assert [[15, 1.0], [16, 2.0]] == res[KEY1][2]

    # test groupby
    # Note: RESP3 includes extra reducers/sources metadata so samples are at
    # index [3] vs RESP2's [2].  Keep protocol branching for groupby only.
    res = await decoded_r.ts().mrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
    )
    samples_idx = 2 if is_resp2_connection(decoded_r) else 3
    assert [[0, 0.0], [1, 2.0], [2, 4.0], [3, 6.0]] == res["Test=This"][samples_idx]
    res = await decoded_r.ts().mrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="max"
    )
    assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["Test=This"][samples_idx]
    res = await decoded_r.ts().mrange(
        0, 3, filters=["Test=This"], groupby="team", reduce="min"
    )
    assert 2 == len(res)
    assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=ny"][samples_idx]
    assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=sf"][samples_idx]

    # test align
    res = await decoded_r.ts().mrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    assert [[0, 10.0], [10, 1.0]] == res[KEY1][2]
    res = await decoded_r.ts().mrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=5,
    )
    assert [[0, 5.0], [5, 6.0]] == res[KEY1][2]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
async def test_multi_reverse_range(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This", "team": "ny"})
    await decoded_r.ts().create(
        KEY2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    for i in range(100):
        await decoded_r.ts().add(KEY1, i, i % 7)
        await decoded_r.ts().add(KEY2, i, i % 11)

    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    assert 100 == len(res[KEY1][2])

    res = await decoded_r.ts().mrange(0, 200, filters=["Test=This"], count=10)
    assert 10 == len(res[KEY1][2])

    for i in range(100):
        await decoded_r.ts().add(KEY1, i + 200, i % 7)
    res = await decoded_r.ts().mrevrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    assert 20 == len(res[KEY1][2])
    assert {} == res[KEY1][0]

    # test withlabels
    res = await decoded_r.ts().mrevrange(
        0, 200, filters=["Test=This"], with_labels=True
    )
    assert {"Test": "This", "team": "ny"} == res[KEY1][0]

    # test with selected labels
    res = await decoded_r.ts().mrevrange(
        0, 200, filters=["Test=This"], select_labels=["team"]
    )
    assert {"team": "ny"} == res[KEY1][0]
    assert {"team": "sf"} == res[KEY2][0]

    # test filterby
    res = await decoded_r.ts().mrevrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    assert [[16, 2.0], [15, 1.0]] == res[KEY1][2]

    # test groupby
    # Note: RESP3 includes extra reducers/sources metadata so samples are at
    # index [3] vs RESP2's [2].  Keep protocol branching for groupby only.
    samples_idx = 2 if is_resp2_connection(decoded_r) else 3
    res = await decoded_r.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
    )
    assert [[3, 6.0], [2, 4.0], [1, 2.0], [0, 0.0]] == res["Test=This"][samples_idx]
    res = await decoded_r.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="max"
    )
    assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["Test=This"][samples_idx]
    res = await decoded_r.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="team", reduce="min"
    )
    assert 2 == len(res)
    assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=ny"][samples_idx]
    assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=sf"][samples_idx]

    # test align
    res = await decoded_r.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    assert [[10, 1.0], [0, 10.0]] == res[KEY1][2]
    res = await decoded_r.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=1,
    )
    assert [[1, 10.0], [0, 1.0]] == res[KEY1][2]


@pytest.mark.redismod
async def test_get(decoded_r: redis.Redis):
    name = "test"
    await decoded_r.ts().create(name)
    assert not await decoded_r.ts().get(name)
    await decoded_r.ts().add(name, 2, 3)
    assert 2 == (await decoded_r.ts().get(name))[0]
    await decoded_r.ts().add(name, 3, 4)
    assert 4 == (await decoded_r.ts().get(name))[1]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_mget(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This"})
    await decoded_r.ts().create(KEY2, labels={"Test": "This", "Taste": "That"})
    act_res = await decoded_r.ts().mget(["Test=This"])
    assert {KEY1: [{}, []], KEY2: [{}, []]} == act_res
    await decoded_r.ts().add(KEY1, "*", 15)
    await decoded_r.ts().add(KEY2, "*", 25)
    res = await decoded_r.ts().mget(["Test=This"])
    assert 15 == res[KEY1][1][1]
    assert 25 == res[KEY2][1][1]
    res = await decoded_r.ts().mget(["Taste=That"])
    assert 25 == res[KEY2][1][1]

    # test with_labels
    assert {} == res[KEY2][0]
    res = await decoded_r.ts().mget(["Taste=That"], with_labels=True)
    assert {"Taste": "That", "Test": "This"} == res[KEY2][0]


@pytest.mark.redismod
async def test_info(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        KEY1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await decoded_r.ts().info(KEY1)
    assert 5 == info.retention_msecs
    assert info["labels"]["currentLabel"] == "currentData"


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_lt("7.9.0")
async def test_info_duplicate_policy(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        KEY1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await decoded_r.ts().info(KEY1)
    assert "block" == info.duplicate_policy

    await decoded_r.ts().create("time-serie-2", duplicate_policy="min")
    info = await decoded_r.ts().info("time-serie-2")
    assert "min" == info.duplicate_policy


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
@skip_if_server_version_gte("7.9.0")
async def test_info_duplicate_policy_prior_redis_8(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        KEY1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await decoded_r.ts().info(KEY1)
    assert info.duplicate_policy is None

    await decoded_r.ts().create("time-serie-2", duplicate_policy="min")
    info = await decoded_r.ts().info("time-serie-2")
    assert "min" == info.duplicate_policy


@pytest.mark.onlynoncluster
@pytest.mark.redismod
async def test_query_index(decoded_r: redis.Redis):
    await decoded_r.ts().create(KEY1, labels={"Test": "This"})
    await decoded_r.ts().create(KEY2, labels={"Test": "This", "Taste": "That"})
    assert 2 == len(await decoded_r.ts().queryindex(["Test=This"]))
    assert 1 == len(await decoded_r.ts().queryindex(["Taste=That"]))
    assert [KEY2] == await decoded_r.ts().queryindex(["Taste=That"])


@pytest.mark.redismod
async def test_uncompressed(decoded_r: redis.Redis):
    await decoded_r.ts().create("compressed")
    await decoded_r.ts().create("uncompressed", uncompressed=True)
    for i in range(1000):
        await decoded_r.ts().add("compressed", i, i)
        await decoded_r.ts().add("uncompressed", i, i)
    compressed_info = await decoded_r.ts().info("compressed")
    uncompressed_info = await decoded_r.ts().info("uncompressed")
    assert compressed_info.memory_usage < uncompressed_info.memory_usage


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_create_with_insertion_filters(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1000 == await decoded_r.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == await decoded_r.ts().add("time-series-1", 1010, 11.0)
    assert 1010 == await decoded_r.ts().add("time-series-1", 1013, 10.0)
    assert 1020 == await decoded_r.ts().add("time-series-1", 1020, 11.5)
    assert 1021 == await decoded_r.ts().add("time-series-1", 1021, 22.0)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, 1.0], [1010, 11.0], [1020, 11.5], [1021, 22.0]] == data_points


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_alter_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == await decoded_r.ts().add("time-series-1", 1010, 11.0)
    assert 1013 == await decoded_r.ts().add("time-series-1", 1013, 10.0)

    await decoded_r.ts().alter(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1013 == await decoded_r.ts().add("time-series-1", 1015, 11.5)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, 1.0], [1010, 11.0], [1013, 10.0]] == data_points


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_add_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().add(
        "time-series-1",
        1000,
        1.0,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == await decoded_r.ts().add("time-series-1", 1004, 3.0)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, 1.0]] == data_points


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_incrby_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().incrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == await decoded_r.ts().incrby("time-series-1", 3.0, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, 1.0]] == data_points

    assert 1000 == await decoded_r.ts().incrby("time-series-1", 10.1, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, 11.1]] == data_points


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
async def test_decrby_with_insertion_filters(decoded_r: redis.Redis):
    assert 1000 == await decoded_r.ts().decrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == await decoded_r.ts().decrby("time-series-1", 3.0, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, -1.0]] == data_points

    assert 1000 == await decoded_r.ts().decrby("time-series-1", 10.1, timestamp=1000)

    data_points = await decoded_r.ts().range("time-series-1", "-", "+")
    assert [[1000, -11.1]] == data_points


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_range_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:2:32",
    )

    # Fill with values
    assert await decoded_r.ts().add("temperature:2:32", 1000, "NaN") == 1000
    assert await decoded_r.ts().add("temperature:2:32", 1003, 25) == 1003
    assert await decoded_r.ts().add("temperature:2:32", 1005, "NaN") == 1005
    assert await decoded_r.ts().add("temperature:2:32", 1006, "NaN") == 1006

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().range(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countNan",
        bucket_size_msec=1000,
    )
    assert [[1000, 3]] == data_points

    # Ensure we count ALL values
    data_points = await decoded_r.ts().range(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countAll",
        bucket_size_msec=1000,
    )
    assert [[1000, 4]] == data_points


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_rev_range_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:2:32",
    )

    # Fill with values
    assert await decoded_r.ts().add("temperature:2:32", 1000, "NaN") == 1000
    assert await decoded_r.ts().add("temperature:2:32", 1003, 25) == 1003
    assert await decoded_r.ts().add("temperature:2:32", 1005, "NaN") == 1005
    assert await decoded_r.ts().add("temperature:2:32", 1006, "NaN") == 1006

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().revrange(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countNan",
        bucket_size_msec=1000,
    )
    assert [[1000, 3]] == data_points

    # Ensure we count ALL values
    data_points = await decoded_r.ts().revrange(
        "temperature:2:32",
        1000,
        1006,
        aggregation_type="countAll",
        bucket_size_msec=1000,
    )
    assert [[1000, 4]] == data_points


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_mrange_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:A",
        labels={"type": "temperature", "name": "A"},
    )
    await decoded_r.ts().create(
        "temperature:B",
        labels={"type": "temperature", "name": "B"},
    )

    # Fill with values
    assert await decoded_r.ts().madd(
        [("temperature:A", 1000, "NaN"), ("temperature:A", 1001, 27)]
    )
    assert await decoded_r.ts().madd(
        [("temperature:B", 1000, "NaN"), ("temperature:B", 1001, 28)]
    )

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().mrange(
        1000,
        1001,
        aggregation_type="countNan",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert 2 == len(data_points)
    assert {} == data_points["temperature:A"][0]
    assert [[1000, 1.0]] == data_points["temperature:A"][2]
    assert {} == data_points["temperature:B"][0]
    assert [[1000, 1.0]] == data_points["temperature:B"][2]

    # Ensure we count ALL values
    data_points = await decoded_r.ts().mrange(
        1000,
        1001,
        aggregation_type="countAll",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert 2 == len(data_points)
    assert [[1000, 2.0]] == data_points["temperature:A"][2]
    assert [[1000, 2.0]] == data_points["temperature:B"][2]


@pytest.mark.redismod
@skip_if_server_version_lt("8.5.0")
async def test_mrevrange_with_count_nan_count_all_aggregators(decoded_r: redis.Redis):
    await decoded_r.ts().create(
        "temperature:A",
        labels={"type": "temperature", "name": "A"},
    )
    await decoded_r.ts().create(
        "temperature:B",
        labels={"type": "temperature", "name": "B"},
    )

    # Fill with values
    assert await decoded_r.ts().madd(
        [("temperature:A", 1000, "NaN"), ("temperature:A", 1001, 27)]
    )
    assert await decoded_r.ts().madd(
        [("temperature:B", 1000, "NaN"), ("temperature:B", 1001, 28)]
    )

    # Ensure we count only NaN values
    data_points = await decoded_r.ts().mrevrange(
        1000,
        1001,
        aggregation_type="countNan",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert 2 == len(data_points)
    assert {} == data_points["temperature:A"][0]
    assert [[1000, 1.0]] == data_points["temperature:A"][2]
    assert {} == data_points["temperature:B"][0]
    assert [[1000, 1.0]] == data_points["temperature:B"][2]

    # Ensure we count ALL values
    data_points = await decoded_r.ts().mrevrange(
        1000,
        1001,
        aggregation_type="countAll",
        bucket_size_msec=1000,
        filters=["type=temperature"],
    )
    assert 2 == len(data_points)
    assert [[1000, 2.0]] == data_points["temperature:A"][2]
    assert [[1000, 2.0]] == data_points["temperature:B"][2]
