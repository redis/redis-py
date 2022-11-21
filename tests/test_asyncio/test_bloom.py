from math import inf

import pytest

import redis.asyncio as redis
from redis.exceptions import ModuleError, RedisError
from redis.utils import HIREDIS_AVAILABLE
from tests.conftest import skip_ifmodversion_lt


def intlist(obj):
    return [int(v) for v in obj]


# @pytest.fixture
# async def client(modclient):
#     assert isinstance(modawait modclient.bf(), redis.commands.bf.BFBloom)
#     assert isinstance(modawait modclient.cf(), redis.commands.bf.CFBloom)
#     assert isinstance(modawait modclient.cms(), redis.commands.bf.CMSBloom)
#     assert isinstance(modawait modclient.tdigest(), redis.commands.bf.TDigestBloom)
#     assert isinstance(modawait modclient.topk(), redis.commands.bf.TOPKBloom)

#     modawait modclient.flushdb()
#     return modclient


@pytest.mark.redismod
async def test_create(modclient: redis.Redis):
    """Test CREATE/RESERVE calls"""
    assert await modclient.bf().create("bloom", 0.01, 1000)
    assert await modclient.bf().create("bloom_e", 0.01, 1000, expansion=1)
    assert await modclient.bf().create("bloom_ns", 0.01, 1000, noScale=True)
    assert await modclient.cf().create("cuckoo", 1000)
    assert await modclient.cf().create("cuckoo_e", 1000, expansion=1)
    assert await modclient.cf().create("cuckoo_bs", 1000, bucket_size=4)
    assert await modclient.cf().create("cuckoo_mi", 1000, max_iterations=10)
    assert await modclient.cms().initbydim("cmsDim", 100, 5)
    assert await modclient.cms().initbyprob("cmsProb", 0.01, 0.01)
    assert await modclient.topk().reserve("topk", 5, 100, 5, 0.9)


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_create(modclient: redis.Redis):
    assert await modclient.tdigest().create("tDigest", 100)


# region Test Bloom Filter
@pytest.mark.redismod
async def test_bf_add(modclient: redis.Redis):
    assert await modclient.bf().create("bloom", 0.01, 1000)
    assert 1 == await modclient.bf().add("bloom", "foo")
    assert 0 == await modclient.bf().add("bloom", "foo")
    assert [0] == intlist(await modclient.bf().madd("bloom", "foo"))
    assert [0, 1] == await modclient.bf().madd("bloom", "foo", "bar")
    assert [0, 0, 1] == await modclient.bf().madd("bloom", "foo", "bar", "baz")
    assert 1 == await modclient.bf().exists("bloom", "foo")
    assert 0 == await modclient.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(await modclient.bf().mexists("bloom", "foo", "noexist"))


@pytest.mark.redismod
async def test_bf_insert(modclient: redis.Redis):
    assert await modclient.bf().create("bloom", 0.01, 1000)
    assert [1] == intlist(await modclient.bf().insert("bloom", ["foo"]))
    assert [0, 1] == intlist(await modclient.bf().insert("bloom", ["foo", "bar"]))
    assert [1] == intlist(await modclient.bf().insert("captest", ["foo"], capacity=10))
    assert [1] == intlist(await modclient.bf().insert("errtest", ["foo"], error=0.01))
    assert 1 == await modclient.bf().exists("bloom", "foo")
    assert 0 == await modclient.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(await modclient.bf().mexists("bloom", "foo", "noexist"))
    info = await modclient.bf().info("bloom")
    assert 2 == info.insertedNum
    assert 1000 == info.capacity
    assert 1 == info.filterNum


@pytest.mark.redismod
async def test_bf_scandump_and_loadchunk(modclient: redis.Redis):
    # Store a filter
    await modclient.bf().create("myBloom", "0.0001", "1000")

    # test is probabilistic and might fail. It is OK to change variables if
    # certain to not break anything
    async def do_verify():
        res = 0
        for x in range(1000):
            await modclient.bf().add("myBloom", x)
            rv = await modclient.bf().exists("myBloom", x)
            assert rv
            rv = await modclient.bf().exists("myBloom", f"nonexist_{x}")
            res += rv == x
        assert res < 5

    await do_verify()
    cmds = []
    if HIREDIS_AVAILABLE:
        with pytest.raises(ModuleError):
            cur = await modclient.bf().scandump("myBloom", 0)
        return

    cur = await modclient.bf().scandump("myBloom", 0)
    first = cur[0]
    cmds.append(cur)

    while True:
        cur = await modclient.bf().scandump("myBloom", first)
        first = cur[0]
        if first == 0:
            break
        else:
            cmds.append(cur)
    prev_info = await modclient.bf().execute_command("bf.debug", "myBloom")

    # Remove the filter
    await modclient.bf().client.delete("myBloom")

    # Now, load all the commands:
    for cmd in cmds:
        await modclient.bf().loadchunk("myBloom", *cmd)

    cur_info = await modclient.bf().execute_command("bf.debug", "myBloom")
    assert prev_info == cur_info
    await do_verify()

    await modclient.bf().client.delete("myBloom")
    await modclient.bf().create("myBloom", "0.0001", "10000000")


@pytest.mark.redismod
async def test_bf_info(modclient: redis.Redis):
    expansion = 4
    # Store a filter
    await modclient.bf().create("nonscaling", "0.0001", "1000", noScale=True)
    info = await modclient.bf().info("nonscaling")
    assert info.expansionRate is None

    await modclient.bf().create("expanding", "0.0001", "1000", expansion=expansion)
    info = await modclient.bf().info("expanding")
    assert info.expansionRate == 4

    try:
        # noScale mean no expansion
        await modclient.bf().create(
            "myBloom", "0.0001", "1000", expansion=expansion, noScale=True
        )
        assert False
    except RedisError:
        assert True


# region Test Cuckoo Filter
@pytest.mark.redismod
async def test_cf_add_and_insert(modclient: redis.Redis):
    assert await modclient.cf().create("cuckoo", 1000)
    assert await modclient.cf().add("cuckoo", "filter")
    assert not await modclient.cf().addnx("cuckoo", "filter")
    assert 1 == await modclient.cf().addnx("cuckoo", "newItem")
    assert [1] == await modclient.cf().insert("captest", ["foo"])
    assert [1] == await modclient.cf().insert("captest", ["foo"], capacity=1000)
    assert [1] == await modclient.cf().insertnx("captest", ["bar"])
    assert [1] == await modclient.cf().insertnx("captest", ["food"], nocreate="1")
    assert [0, 0, 1] == await modclient.cf().insertnx("captest", ["foo", "bar", "baz"])
    assert [0] == await modclient.cf().insertnx("captest", ["bar"], capacity=1000)
    assert [1] == await modclient.cf().insert("empty1", ["foo"], capacity=1000)
    assert [1] == await modclient.cf().insertnx("empty2", ["bar"], capacity=1000)
    info = await modclient.cf().info("captest")
    assert 5 == info.insertedNum
    assert 0 == info.deletedNum
    assert 1 == info.filterNum


@pytest.mark.redismod
async def test_cf_exists_and_del(modclient: redis.Redis):
    assert await modclient.cf().create("cuckoo", 1000)
    assert await modclient.cf().add("cuckoo", "filter")
    assert await modclient.cf().exists("cuckoo", "filter")
    assert not await modclient.cf().exists("cuckoo", "notexist")
    assert 1 == await modclient.cf().count("cuckoo", "filter")
    assert 0 == await modclient.cf().count("cuckoo", "notexist")
    assert await modclient.cf().delete("cuckoo", "filter")
    assert 0 == await modclient.cf().count("cuckoo", "filter")


# region Test Count-Min Sketch
@pytest.mark.redismod
async def test_cms(modclient: redis.Redis):
    assert await modclient.cms().initbydim("dim", 1000, 5)
    assert await modclient.cms().initbyprob("prob", 0.01, 0.01)
    assert await modclient.cms().incrby("dim", ["foo"], [5])
    assert [0] == await modclient.cms().query("dim", "notexist")
    assert [5] == await modclient.cms().query("dim", "foo")
    assert [10, 15] == await modclient.cms().incrby("dim", ["foo", "bar"], [5, 15])
    assert [10, 15] == await modclient.cms().query("dim", "foo", "bar")
    info = await modclient.cms().info("dim")
    assert 1000 == info.width
    assert 5 == info.depth
    assert 25 == info.count


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_cms_merge(modclient: redis.Redis):
    assert await modclient.cms().initbydim("A", 1000, 5)
    assert await modclient.cms().initbydim("B", 1000, 5)
    assert await modclient.cms().initbydim("C", 1000, 5)
    assert await modclient.cms().incrby("A", ["foo", "bar", "baz"], [5, 3, 9])
    assert await modclient.cms().incrby("B", ["foo", "bar", "baz"], [2, 3, 1])
    assert [5, 3, 9] == await modclient.cms().query("A", "foo", "bar", "baz")
    assert [2, 3, 1] == await modclient.cms().query("B", "foo", "bar", "baz")
    assert await modclient.cms().merge("C", 2, ["A", "B"])
    assert [7, 6, 10] == await modclient.cms().query("C", "foo", "bar", "baz")
    assert await modclient.cms().merge("C", 2, ["A", "B"], ["1", "2"])
    assert [9, 9, 11] == await modclient.cms().query("C", "foo", "bar", "baz")
    assert await modclient.cms().merge("C", 2, ["A", "B"], ["2", "3"])
    assert [16, 15, 21] == await modclient.cms().query("C", "foo", "bar", "baz")


# endregion


# region Test Top-K
@pytest.mark.redismod
async def test_topk(modclient: redis.Redis):
    # test list with empty buckets
    assert await modclient.topk().reserve("topk", 3, 50, 4, 0.9)
    assert [
        None,
        None,
        None,
        "A",
        "C",
        "D",
        None,
        None,
        "E",
        None,
        "B",
        "C",
        None,
        None,
        None,
        "D",
        None,
    ] == await modclient.topk().add(
        "topk",
        "A",
        "B",
        "C",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
        1,
    )
    assert [1, 1, 0, 0, 1, 0, 0] == await modclient.topk().query(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )
    with pytest.deprecated_call():
        assert [4, 3, 2, 3, 3, 0, 1] == await modclient.topk().count(
            "topk", "A", "B", "C", "D", "E", "F", "G"
        )

    # test full list
    assert await modclient.topk().reserve("topklist", 3, 50, 3, 0.9)
    assert await modclient.topk().add(
        "topklist",
        "A",
        "B",
        "C",
        "D",
        "E",
        "A",
        "A",
        "B",
        "C",
        "G",
        "D",
        "B",
        "D",
        "A",
        "E",
        "E",
    )
    assert ["A", "B", "E"] == await modclient.topk().list("topklist")
    res = await modclient.topk().list("topklist", withcount=True)
    assert ["A", 4, "B", 3, "E", 3] == res
    info = await modclient.topk().info("topklist")
    assert 3 == info.k
    assert 50 == info.width
    assert 3 == info.depth
    assert 0.9 == round(float(info.decay), 1)


@pytest.mark.redismod
async def test_topk_incrby(modclient: redis.Redis):
    await modclient.flushdb()
    assert await modclient.topk().reserve("topk", 3, 10, 3, 1)
    assert [None, None, None] == await modclient.topk().incrby(
        "topk", ["bar", "baz", "42"], [3, 6, 2]
    )
    res = await modclient.topk().incrby("topk", ["42", "xyzzy"], [8, 4])
    assert [None, "bar"] == res
    with pytest.deprecated_call():
        assert [3, 6, 10, 4, 0] == await modclient.topk().count(
            "topk", "bar", "baz", "42", "xyzzy", 4
        )


# region Test T-Digest
@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_reset(modclient: redis.Redis):
    assert await modclient.tdigest().create("tDigest", 10)
    # reset on empty histogram
    assert await modclient.tdigest().reset("tDigest")
    # insert data-points into sketch
    assert await modclient.tdigest().add("tDigest", list(range(10)))

    assert await modclient.tdigest().reset("tDigest")
    # assert we have 0 unmerged nodes
    assert 0 == (await modclient.tdigest().info("tDigest")).unmerged_nodes


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_merge(modclient: redis.Redis):
    assert await modclient.tdigest().create("to-tDigest", 10)
    assert await modclient.tdigest().create("from-tDigest", 10)
    # insert data-points into sketch
    assert await modclient.tdigest().add("from-tDigest", [1.0] * 10)
    assert await modclient.tdigest().add("to-tDigest", [2.0] * 10)
    # merge from-tdigest into to-tdigest
    assert await modclient.tdigest().merge("to-tDigest", 1, "from-tDigest")
    # we should now have 110 weight on to-histogram
    info = await modclient.tdigest().info("to-tDigest")
    total_weight_to = float(info.merged_weight) + float(info.unmerged_weight)
    assert 20.0 == total_weight_to
    # test override
    assert await modclient.tdigest().create("from-override", 10)
    assert await modclient.tdigest().create("from-override-2", 10)
    assert await modclient.tdigest().add("from-override", [3.0] * 10)
    assert await modclient.tdigest().add("from-override-2", [4.0] * 10)
    assert await modclient.tdigest().merge(
        "to-tDigest", 2, "from-override", "from-override-2", override=True
    )
    assert 3.0 == await modclient.tdigest().min("to-tDigest")
    assert 4.0 == await modclient.tdigest().max("to-tDigest")


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_min_and_max(modclient: redis.Redis):
    assert await modclient.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert await modclient.tdigest().add("tDigest", [1, 2, 3])
    # min/max
    assert 3 == await modclient.tdigest().max("tDigest")
    assert 1 == await modclient.tdigest().min("tDigest")


@pytest.mark.redismod
@pytest.mark.experimental
@skip_ifmodversion_lt("2.4.0", "bf")
async def test_tdigest_quantile(modclient: redis.Redis):
    assert await modclient.tdigest().create("tDigest", 500)
    # insert data-points into sketch
    assert await modclient.tdigest().add(
        "tDigest", list([x * 0.01 for x in range(1, 10000)])
    )
    # assert min min/max have same result as quantile 0 and 1
    assert (
        await modclient.tdigest().max("tDigest")
        == (await modclient.tdigest().quantile("tDigest", 1))[0]
    )
    assert (
        await modclient.tdigest().min("tDigest")
        == (await modclient.tdigest().quantile("tDigest", 0.0))[0]
    )

    assert 1.0 == round((await modclient.tdigest().quantile("tDigest", 0.01))[0], 2)
    assert 99.0 == round((await modclient.tdigest().quantile("tDigest", 0.99))[0], 2)

    # test multiple quantiles
    assert await modclient.tdigest().create("t-digest", 100)
    assert await modclient.tdigest().add("t-digest", [1, 2, 3, 4, 5])
    res = await modclient.tdigest().quantile("t-digest", 0.5, 0.8)
    assert [3.0, 5.0] == res


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_cdf(modclient: redis.Redis):
    assert await modclient.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert await modclient.tdigest().add("tDigest", list(range(1, 10)))
    assert 0.1 == round((await modclient.tdigest().cdf("tDigest", 1.0))[0], 1)
    assert 0.9 == round((await modclient.tdigest().cdf("tDigest", 9.0))[0], 1)
    res = await modclient.tdigest().cdf("tDigest", 1.0, 9.0)
    assert [0.1, 0.9] == [round(x, 1) for x in res]


@pytest.mark.redismod
@pytest.mark.experimental
@skip_ifmodversion_lt("2.4.0", "bf")
async def test_tdigest_trimmed_mean(modclient: redis.Redis):
    assert await modclient.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert await modclient.tdigest().add("tDigest", list(range(1, 10)))
    assert 5 == await modclient.tdigest().trimmed_mean("tDigest", 0.1, 0.9)
    assert 4.5 == await modclient.tdigest().trimmed_mean("tDigest", 0.4, 0.5)


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_rank(modclient: redis.Redis):
    assert await modclient.tdigest().create("t-digest", 500)
    assert await modclient.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == (await modclient.tdigest().rank("t-digest", -1))[0]
    assert 0 == (await modclient.tdigest().rank("t-digest", 0))[0]
    assert 10 == (await modclient.tdigest().rank("t-digest", 10))[0]
    assert [-1, 20, 9] == await modclient.tdigest().rank("t-digest", -20, 20, 9)


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_revrank(modclient: redis.Redis):
    assert await modclient.tdigest().create("t-digest", 500)
    assert await modclient.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == (await modclient.tdigest().revrank("t-digest", 20))[0]
    assert 19 == (await modclient.tdigest().revrank("t-digest", 0))[0]
    assert [-1, 19, 9] == await modclient.tdigest().revrank("t-digest", 21, 0, 10)


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_byrank(modclient: redis.Redis):
    assert await modclient.tdigest().create("t-digest", 500)
    assert await modclient.tdigest().add("t-digest", list(range(1, 11)))
    assert 1 == (await modclient.tdigest().byrank("t-digest", 0))[0]
    assert 10 == (await modclient.tdigest().byrank("t-digest", 9))[0]
    assert (await modclient.tdigest().byrank("t-digest", 100))[0] == inf
    with pytest.raises(redis.ResponseError):
        (await modclient.tdigest().byrank("t-digest", -1))[0]


@pytest.mark.redismod
@pytest.mark.experimental
async def test_tdigest_byrevrank(modclient: redis.Redis):
    assert await modclient.tdigest().create("t-digest", 500)
    assert await modclient.tdigest().add("t-digest", list(range(1, 11)))
    assert 10 == (await modclient.tdigest().byrevrank("t-digest", 0))[0]
    assert 1 == (await modclient.tdigest().byrevrank("t-digest", 9))[0]
    assert (await modclient.tdigest().byrevrank("t-digest", 100))[0] == -inf
    with pytest.raises(redis.ResponseError):
        (await modclient.tdigest().byrevrank("t-digest", -1))[0]


# @pytest.mark.redismod
# async def test_pipeline(modclient: redis.Redis):
#     pipeline = await modclient.bf().pipeline()
#     assert not await modclient.bf().execute_command("get pipeline")
#
#     assert await modclient.bf().create("pipeline", 0.01, 1000)
#     for i in range(100):
#         pipeline.add("pipeline", i)
#     for i in range(100):
#         assert not (await modclient.bf().exists("pipeline", i))
#
#     pipeline.execute()
#
#     for i in range(100):
#         assert await modclient.bf().exists("pipeline", i)
