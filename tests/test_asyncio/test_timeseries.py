import time
from time import sleep

import pytest

import redis.asyncio as redis
from tests.conftest import skip_ifmodversion_lt


@pytest.mark.redismod
async def test_create(r: redis.Redis):
    assert await r.ts().create(1)
    assert await r.ts().create(2, retention_msecs=5)
    assert await r.ts().create(3, labels={"Redis": "Labs"})
    assert await r.ts().create(4, retention_msecs=20, labels={"Time": "Series"})
    info = await r.ts().info(4)
    assert 20 == info.retention_msecs
    assert "Series" == info.labels["Time"]

    # Test for a chunk size of 128 Bytes
    assert await r.ts().create("time-serie-1", chunk_size=128)
    info = await r.ts().info("time-serie-1")
    assert 128, info.chunk_size


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_create_duplicate_policy(r: redis.Redis):
    # Test for duplicate policy
    for duplicate_policy in ["block", "last", "first", "min", "max"]:
        ts_name = f"time-serie-ooo-{duplicate_policy}"
        assert await r.ts().create(ts_name, duplicate_policy=duplicate_policy)
        info = await r.ts().info(ts_name)
        assert duplicate_policy == info.duplicate_policy


@pytest.mark.redismod
async def test_alter(r: redis.Redis):
    assert await r.ts().create(1)
    res = await r.ts().info(1)
    assert 0 == res.retention_msecs
    assert await r.ts().alter(1, retention_msecs=10)
    res = await r.ts().info(1)
    assert {} == res.labels
    res = await r.ts().info(1)
    assert 10 == res.retention_msecs
    assert await r.ts().alter(1, labels={"Time": "Series"})
    res = await r.ts().info(1)
    assert "Series" == res.labels["Time"]
    res = await r.ts().info(1)
    assert 10 == res.retention_msecs


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_alter_diplicate_policy(r: redis.Redis):
    assert await r.ts().create(1)
    info = await r.ts().info(1)
    assert info.duplicate_policy is None
    assert await r.ts().alter(1, duplicate_policy="min")
    info = await r.ts().info(1)
    assert "min" == info.duplicate_policy


@pytest.mark.redismod
async def test_add(r: redis.Redis):
    assert 1 == await r.ts().add(1, 1, 1)
    assert 2 == await r.ts().add(2, 2, 3, retention_msecs=10)
    assert 3 == await r.ts().add(3, 3, 2, labels={"Redis": "Labs"})
    assert 4 == await r.ts().add(
        4, 4, 2, retention_msecs=10, labels={"Redis": "Labs", "Time": "Series"}
    )
    res = await r.ts().add(5, "*", 1)
    assert abs(time.time() - round(float(res) / 1000)) < 1.0

    info = await r.ts().info(4)
    assert 10 == info.retention_msecs
    assert "Labs" == info.labels["Redis"]

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert await r.ts().add("time-serie-1", 1, 10.0, chunk_size=128)
    info = await r.ts().info("time-serie-1")
    assert 128 == info.chunk_size


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def test_add_duplicate_policy(r: redis.Redis):

    # Test for duplicate policy BLOCK
    assert 1 == await r.ts().add("time-serie-add-ooo-block", 1, 5.0)
    with pytest.raises(Exception):
        await r.ts().add(
            "time-serie-add-ooo-block", 1, 5.0, duplicate_policy="block"
        )

    # Test for duplicate policy LAST
    assert 1 == await r.ts().add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == await r.ts().add(
        "time-serie-add-ooo-last", 1, 10.0, duplicate_policy="last"
    )
    res = await r.ts().get("time-serie-add-ooo-last")
    assert 10.0 == res[1]

    # Test for duplicate policy FIRST
    assert 1 == await r.ts().add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == await r.ts().add(
        "time-serie-add-ooo-first", 1, 10.0, duplicate_policy="first"
    )
    res = await r.ts().get("time-serie-add-ooo-first")
    assert 5.0 == res[1]

    # Test for duplicate policy MAX
    assert 1 == await r.ts().add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == await r.ts().add(
        "time-serie-add-ooo-max", 1, 10.0, duplicate_policy="max"
    )
    res = await r.ts().get("time-serie-add-ooo-max")
    assert 10.0 == res[1]

    # Test for duplicate policy MIN
    assert 1 == await r.ts().add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == await r.ts().add(
        "time-serie-add-ooo-min", 1, 10.0, duplicate_policy="min"
    )
    res = await r.ts().get("time-serie-add-ooo-min")
    assert 5.0 == res[1]


@pytest.mark.redismod
async def test_madd(r: redis.Redis):
    await r.ts().create("a")
    assert [1, 2, 3] == await r.ts().madd(
        [("a", 1, 5), ("a", 2, 10), ("a", 3, 15)]
    )


@pytest.mark.redismod
async def test_incrby_decrby(r: redis.Redis):
    for _ in range(100):
        assert await r.ts().incrby(1, 1)
        sleep(0.001)
    assert 100 == (await r.ts().get(1))[1]
    for _ in range(100):
        assert await r.ts().decrby(1, 1)
        sleep(0.001)
    assert 0 == (await r.ts().get(1))[1]

    assert await r.ts().incrby(2, 1.5, timestamp=5)
    assert (5, 1.5) == await r.ts().get(2)
    assert await r.ts().incrby(2, 2.25, timestamp=7)
    assert (7, 3.75) == await r.ts().get(2)
    assert await r.ts().decrby(2, 1.5, timestamp=15)
    assert (15, 2.25) == await r.ts().get(2)

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert await r.ts().incrby("time-serie-1", 10, chunk_size=128)
    info = await r.ts().info("time-serie-1")
    assert 128 == info.chunk_size

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert await r.ts().decrby("time-serie-2", 10, chunk_size=128)
    info = await r.ts().info("time-serie-2")
    assert 128 == info.chunk_size


@pytest.mark.redismod
async def test_create_and_delete_rule(r: redis.Redis):
    # test rule creation
    time = 100
    await r.ts().create(1)
    await r.ts().create(2)
    await r.ts().createrule(1, 2, "avg", 100)
    for i in range(50):
        await r.ts().add(1, time + i * 2, 1)
        await r.ts().add(1, time + i * 2 + 1, 2)
    await r.ts().add(1, time * 2, 1.5)
    assert round((await r.ts().get(2))[1], 5) == 1.5
    info = await r.ts().info(1)
    assert info.rules[0][1] == 100

    # test rule deletion
    await r.ts().deleterule(1, 2)
    info = await r.ts().info(1)
    assert not info.rules


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "timeseries")
async def test_del_range(r: redis.Redis):
    try:
        await r.ts().delete("test", 0, 100)
    except Exception as e:
        assert e.__str__() != ""

    for i in range(100):
        await r.ts().add(1, i, i % 7)
    assert 22 == await r.ts().delete(1, 0, 21)
    assert [] == await r.ts().range(1, 0, 21)
    assert [(22, 1.0)] == await r.ts().range(1, 22, 22)


@pytest.mark.redismod
async def test_range(r: redis.Redis):
    for i in range(100):
        await r.ts().add(1, i, i % 7)
    assert 100 == len(await r.ts().range(1, 0, 200))
    for i in range(100):
        await r.ts().add(1, i + 200, i % 7)
    assert 200 == len(await r.ts().range(1, 0, 500))
    # last sample isn't returned
    assert 20 == len(
        await r.ts().range(
            1, 0, 500, aggregation_type="avg", bucket_size_msec=10
        )
    )
    assert 10 == len(await r.ts().range(1, 0, 500, count=10))


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "timeseries")
async def test_range_advanced(r: redis.Redis):
    for i in range(100):
        await r.ts().add(1, i, i % 7)
        await r.ts().add(1, i + 200, i % 7)

    assert 2 == len(
        await r.ts().range(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert [(0, 10.0), (10, 1.0)] == await r.ts().range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert [(0, 5.0), (5, 6.0)] == await r.ts().range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=5
    )
    assert [(0, 2.55), (10, 3.0)] == await r.ts().range(
        1, 0, 10, aggregation_type="twa", bucket_size_msec=10
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("99.99.99", "timeseries")
async def test_rev_range(r: redis.Redis):
    for i in range(100):
        await r.ts().add(1, i, i % 7)
    assert 100 == len(await r.ts().range(1, 0, 200))
    for i in range(100):
        await r.ts().add(1, i + 200, i % 7)
    assert 200 == len(await r.ts().range(1, 0, 500))
    # first sample isn't returned
    assert 20 == len(
        await r.ts().revrange(
            1, 0, 500, aggregation_type="avg", bucket_size_msec=10
        )
    )
    assert 10 == len(await r.ts().revrange(1, 0, 500, count=10))
    assert 2 == len(
        await r.ts().revrange(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert [(10, 1.0), (0, 10.0)] == await r.ts().revrange(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert [(1, 10.0), (0, 1.0)] == await r.ts().revrange(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=1
    )


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def testMultiRange(r: redis.Redis):
    await r.ts().create(1, labels={"Test": "This", "team": "ny"})
    await r.ts().create(
        2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    for i in range(100):
        await r.ts().add(1, i, i % 7)
        await r.ts().add(2, i, i % 11)

    res = await r.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    assert 100 == len(res[0]["1"][1])

    res = await r.ts().mrange(0, 200, filters=["Test=This"], count=10)
    assert 10 == len(res[0]["1"][1])

    for i in range(100):
        await r.ts().add(1, i + 200, i % 7)
    res = await r.ts().mrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    assert 20 == len(res[0]["1"][1])

    # test withlabels
    assert {} == res[0]["1"][0]
    res = await r.ts().mrange(0, 200, filters=["Test=This"], with_labels=True)
    assert {"Test": "This", "team": "ny"} == res[0]["1"][0]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("99.99.99", "timeseries")
async def test_multi_range_advanced(r: redis.Redis):
    await r.ts().create(1, labels={"Test": "This", "team": "ny"})
    await r.ts().create(
        2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    for i in range(100):
        await r.ts().add(1, i, i % 7)
        await r.ts().add(2, i, i % 11)

    # test with selected labels
    res = await r.ts().mrange(
        0, 200, filters=["Test=This"], select_labels=["team"]
    )
    assert {"team": "ny"} == res[0]["1"][0]
    assert {"team": "sf"} == res[1]["2"][0]

    # test with filterby
    res = await r.ts().mrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    assert [(15, 1.0), (16, 2.0)] == res[0]["1"][1]

    # test groupby
    res = await r.ts().mrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
    )
    assert [(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)] == res[0]["Test=This"][1]
    res = await r.ts().mrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="max"
    )
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["Test=This"][1]
    res = await r.ts().mrange(
        0, 3, filters=["Test=This"], groupby="team", reduce="min"
    )
    assert 2 == len(res)
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["team=ny"][1]
    assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[1]["team=sf"][1]

    # test align
    res = await r.ts().mrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    assert [(0, 10.0), (10, 1.0)] == res[0]["1"][1]
    res = await r.ts().mrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=5,
    )
    assert [(0, 5.0), (5, 6.0)] == res[0]["1"][1]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("99.99.99", "timeseries")
async def test_multi_reverse_range(r: redis.Redis):
    await r.ts().create(1, labels={"Test": "This", "team": "ny"})
    await r.ts().create(
        2, labels={"Test": "This", "Taste": "That", "team": "sf"}
    )
    for i in range(100):
        await r.ts().add(1, i, i % 7)
        await r.ts().add(2, i, i % 11)

    res = await r.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    assert 100 == len(res[0]["1"][1])

    res = await r.ts().mrange(0, 200, filters=["Test=This"], count=10)
    assert 10 == len(res[0]["1"][1])

    for i in range(100):
        await r.ts().add(1, i + 200, i % 7)
    res = await r.ts().mrevrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    assert 20 == len(res[0]["1"][1])
    assert {} == res[0]["1"][0]

    # test withlabels
    res = await r.ts().mrevrange(
        0, 200, filters=["Test=This"], with_labels=True
    )
    assert {"Test": "This", "team": "ny"} == res[0]["1"][0]

    # test with selected labels
    res = await r.ts().mrevrange(
        0, 200, filters=["Test=This"], select_labels=["team"]
    )
    assert {"team": "ny"} == res[0]["1"][0]
    assert {"team": "sf"} == res[1]["2"][0]

    # test filterby
    res = await r.ts().mrevrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    assert [(16, 2.0), (15, 1.0)] == res[0]["1"][1]

    # test groupby
    res = await r.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
    )
    assert [(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)] == res[0]["Test=This"][1]
    res = await r.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="max"
    )
    assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["Test=This"][1]
    res = await r.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="team", reduce="min"
    )
    assert 2 == len(res)
    assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["team=ny"][1]
    assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[1]["team=sf"][1]

    # test align
    res = await r.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    assert [(10, 1.0), (0, 10.0)] == res[0]["1"][1]
    res = await r.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=1,
    )
    assert [(1, 10.0), (0, 1.0)] == res[0]["1"][1]


@pytest.mark.redismod
async def test_get(r: redis.Redis):
    name = "test"
    await r.ts().create(name)
    assert await r.ts().get(name) is None
    await r.ts().add(name, 2, 3)
    assert 2 == (await r.ts().get(name))[0]
    await r.ts().add(name, 3, 4)
    assert 4 == (await r.ts().get(name))[1]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_mget(r: redis.Redis):
    await r.ts().create(1, labels={"Test": "This"})
    await r.ts().create(2, labels={"Test": "This", "Taste": "That"})
    act_res = await r.ts().mget(["Test=This"])
    exp_res = [{"1": [{}, None, None]}, {"2": [{}, None, None]}]
    assert act_res == exp_res
    await r.ts().add(1, "*", 15)
    await r.ts().add(2, "*", 25)
    res = await r.ts().mget(["Test=This"])
    assert 15 == res[0]["1"][2]
    assert 25 == res[1]["2"][2]
    res = await r.ts().mget(["Taste=That"])
    assert 25 == res[0]["2"][2]

    # test with_labels
    assert {} == res[0]["2"][0]
    res = await r.ts().mget(["Taste=That"], with_labels=True)
    assert {"Taste": "That", "Test": "This"} == res[0]["2"][0]


@pytest.mark.redismod
async def test_info(r: redis.Redis):
    await r.ts().create(
        1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await r.ts().info(1)
    assert 5 == info.retention_msecs
    assert info.labels["currentLabel"] == "currentData"


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
async def testInfoDuplicatePolicy(r: redis.Redis):
    await r.ts().create(
        1, retention_msecs=5, labels={"currentLabel": "currentData"}
    )
    info = await r.ts().info(1)
    assert info.duplicate_policy is None

    await r.ts().create("time-serie-2", duplicate_policy="min")
    info = await r.ts().info("time-serie-2")
    assert "min" == info.duplicate_policy


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_query_index(r: redis.Redis):
    await r.ts().create(1, labels={"Test": "This"})
    await r.ts().create(2, labels={"Test": "This", "Taste": "That"})
    assert 2 == len(await r.ts().queryindex(["Test=This"]))
    assert 1 == len(await r.ts().queryindex(["Taste=That"]))
    assert [2] == await r.ts().queryindex(["Taste=That"])


# @pytest.mark.redismod
# async def test_pipeline(r: redis.Redis):
#     pipeline = await r.ts().pipeline()
#     pipeline.create("with_pipeline")
#     for i in range(100):
#         pipeline.add("with_pipeline", i, 1.1 * i)
#     pipeline.execute()

#     info = await r.ts().info("with_pipeline")
#     assert info.lastTimeStamp == 99
#     assert info.total_samples == 100
#     assert await r.ts().get("with_pipeline")[1] == 99 * 1.1


@pytest.mark.redismod
async def test_uncompressed(r: redis.Redis):
    await r.ts().create("compressed")
    await r.ts().create("uncompressed", uncompressed=True)
    compressed_info = await r.ts().info("compressed")
    uncompressed_info = await r.ts().info("uncompressed")
    assert compressed_info.memory_usage != uncompressed_info.memory_usage
