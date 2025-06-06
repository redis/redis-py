from math import inf

import pytest
import redis.commands.bf
from redis.exceptions import RedisError

from .conftest import (
    _get_client,
    assert_resp_response,
    is_resp2_connection,
    skip_ifmodversion_lt,
)


@pytest.fixture()
def decoded_r(request, stack_url):
    with _get_client(
        redis.Redis, request, decode_responses=True, from_url=stack_url
    ) as client:
        yield client


def intlist(obj):
    return [int(v) for v in obj]


@pytest.fixture
def client(decoded_r):
    assert isinstance(decoded_r.bf(), redis.commands.bf.BFBloom)
    assert isinstance(decoded_r.cf(), redis.commands.bf.CFBloom)
    assert isinstance(decoded_r.cms(), redis.commands.bf.CMSBloom)
    assert isinstance(decoded_r.tdigest(), redis.commands.bf.TDigestBloom)
    assert isinstance(decoded_r.topk(), redis.commands.bf.TOPKBloom)

    decoded_r.flushdb()
    return decoded_r


@pytest.mark.redismod
def test_create(client):
    """Test CREATE/RESERVE calls"""
    assert client.bf().create("bloom", 0.01, 1000)
    assert client.bf().create("bloom_e", 0.01, 1000, expansion=1)
    assert client.bf().create("bloom_ns", 0.01, 1000, noScale=True)
    assert client.cf().create("cuckoo", 1000)
    assert client.cf().create("cuckoo_e", 1000, expansion=1)
    assert client.cf().create("cuckoo_bs", 1000, bucket_size=4)
    assert client.cf().create("cuckoo_mi", 1000, max_iterations=10)
    assert client.cms().initbydim("cmsDim", 100, 5)
    assert client.cms().initbyprob("cmsProb", 0.01, 0.01)
    assert client.topk().reserve("topk", 5, 100, 5, 0.9)


@pytest.mark.redismod
def test_bf_reserve(client):
    """Testing BF.RESERVE"""
    assert client.bf().reserve("bloom", 0.01, 1000)
    assert client.bf().reserve("bloom_e", 0.01, 1000, expansion=1)
    assert client.bf().reserve("bloom_ns", 0.01, 1000, noScale=True)
    assert client.cf().reserve("cuckoo", 1000)
    assert client.cf().reserve("cuckoo_e", 1000, expansion=1)
    assert client.cf().reserve("cuckoo_bs", 1000, bucket_size=4)
    assert client.cf().reserve("cuckoo_mi", 1000, max_iterations=10)
    assert client.cms().initbydim("cmsDim", 100, 5)
    assert client.cms().initbyprob("cmsProb", 0.01, 0.01)
    assert client.topk().reserve("topk", 5, 100, 5, 0.9)


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_create(client):
    assert client.tdigest().create("tDigest", 100)


@pytest.mark.redismod
def test_bf_add(client):
    assert client.bf().create("bloom", 0.01, 1000)
    assert 1 == client.bf().add("bloom", "foo")
    assert 0 == client.bf().add("bloom", "foo")
    assert [0] == intlist(client.bf().madd("bloom", "foo"))
    assert [0, 1] == client.bf().madd("bloom", "foo", "bar")
    assert [0, 0, 1] == client.bf().madd("bloom", "foo", "bar", "baz")
    assert 1 == client.bf().exists("bloom", "foo")
    assert 0 == client.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(client.bf().mexists("bloom", "foo", "noexist"))


@pytest.mark.redismod
def test_bf_insert(client):
    assert client.bf().create("bloom", 0.01, 1000)
    assert [1] == intlist(client.bf().insert("bloom", ["foo"]))
    assert [0, 1] == intlist(client.bf().insert("bloom", ["foo", "bar"]))
    assert [1] == intlist(client.bf().insert("captest", ["foo"], capacity=10))
    assert [1] == intlist(client.bf().insert("errtest", ["foo"], error=0.01))
    assert 1 == client.bf().exists("bloom", "foo")
    assert 0 == client.bf().exists("bloom", "noexist")
    assert [1, 0] == intlist(client.bf().mexists("bloom", "foo", "noexist"))
    info = client.bf().info("bloom")
    assert_resp_response(
        client,
        2,
        info.get("insertedNum"),
        info.get("Number of items inserted"),
    )
    assert_resp_response(
        client,
        1000,
        info.get("capacity"),
        info.get("Capacity"),
    )
    assert_resp_response(
        client,
        1,
        info.get("filterNum"),
        info.get("Number of filters"),
    )


@pytest.mark.redismod
def test_bf_scandump_and_loadchunk(client):
    # Store a filter
    client.bf().create("myBloom", "0.0001", "1000")

    # test is probabilistic and might fail. It is OK to change variables if
    # certain to not break anything
    def do_verify():
        res = 0
        for x in range(1000):
            client.bf().add("myBloom", x)
            rv = client.bf().exists("myBloom", x)
            assert rv
            rv = client.bf().exists("myBloom", f"nonexist_{x}")
            res += rv == x
        assert res < 5

    do_verify()
    cmds = []

    cur = client.bf().scandump("myBloom", 0)
    first = cur[0]
    cmds.append(cur)

    while True:
        cur = client.bf().scandump("myBloom", first)
        first = cur[0]
        if first == 0:
            break
        else:
            cmds.append(cur)
    prev_info = client.bf().execute_command("bf.debug", "myBloom")

    # Remove the filter
    client.bf().client.delete("myBloom")

    # Now, load all the commands:
    for cmd in cmds:
        client.bf().loadchunk("myBloom", *cmd)

    cur_info = client.bf().execute_command("bf.debug", "myBloom")
    assert prev_info == cur_info
    do_verify()

    client.bf().client.delete("myBloom")
    client.bf().create("myBloom", "0.0001", "10000000")


@pytest.mark.redismod
def test_bf_info(client):
    expansion = 4
    # Store a filter
    client.bf().create("nonscaling", "0.0001", "1000", noScale=True)
    info = client.bf().info("nonscaling")
    assert_resp_response(
        client,
        None,
        info.get("expansionRate"),
        info.get("Expansion rate"),
    )

    client.bf().create("expanding", "0.0001", "1000", expansion=expansion)
    info = client.bf().info("expanding")
    assert_resp_response(
        client,
        4,
        info.get("expansionRate"),
        info.get("Expansion rate"),
    )

    try:
        # noScale mean no expansion
        client.bf().create(
            "myBloom", "0.0001", "1000", expansion=expansion, noScale=True
        )
        assert False
    except RedisError:
        assert True


@pytest.mark.redismod
def test_bf_card(client):
    # return 0 if the key does not exist
    assert client.bf().card("not_exist") == 0

    # Store a filter
    assert client.bf().add("bf1", "item_foo") == 1
    assert client.bf().card("bf1") == 1

    # Error when key is of a type other than Bloom filter.
    with pytest.raises(redis.ResponseError):
        client.set("setKey", "value")
        client.bf().card("setKey")


@pytest.mark.redismod
def test_cf_add_and_insert(client):
    assert client.cf().create("cuckoo", 1000)
    assert client.cf().add("cuckoo", "filter")
    assert not client.cf().addnx("cuckoo", "filter")
    assert 1 == client.cf().addnx("cuckoo", "newItem")
    assert [1] == client.cf().insert("captest", ["foo"])
    assert [1] == client.cf().insert("captest", ["foo"], capacity=1000)
    assert [1] == client.cf().insertnx("captest", ["bar"])
    assert [1] == client.cf().insertnx("captest", ["food"], nocreate="1")
    assert [0, 0, 1] == client.cf().insertnx("captest", ["foo", "bar", "baz"])
    assert [0] == client.cf().insertnx("captest", ["bar"], capacity=1000)
    assert [1] == client.cf().insert("empty1", ["foo"], capacity=1000)
    assert [1] == client.cf().insertnx("empty2", ["bar"], capacity=1000)
    info = client.cf().info("captest")
    assert_resp_response(
        client, 5, info.get("insertedNum"), info.get("Number of items inserted")
    )
    assert_resp_response(
        client, 0, info.get("deletedNum"), info.get("Number of items deleted")
    )
    assert_resp_response(
        client, 1, info.get("filterNum"), info.get("Number of filters")
    )


@pytest.mark.redismod
def test_cf_exists_and_del(client):
    assert client.cf().create("cuckoo", 1000)
    assert client.cf().add("cuckoo", "filter")
    assert client.cf().exists("cuckoo", "filter")
    assert not client.cf().exists("cuckoo", "notexist")
    assert [1, 0] == client.cf().mexists("cuckoo", "filter", "notexist")
    assert 1 == client.cf().count("cuckoo", "filter")
    assert 0 == client.cf().count("cuckoo", "notexist")
    assert client.cf().delete("cuckoo", "filter")
    assert 0 == client.cf().count("cuckoo", "filter")


@pytest.mark.redismod
def test_cms(client):
    assert client.cms().initbydim("dim", 1000, 5)
    assert client.cms().initbyprob("prob", 0.01, 0.01)
    assert client.cms().incrby("dim", ["foo"], [5])
    assert [0] == client.cms().query("dim", "notexist")
    assert [5] == client.cms().query("dim", "foo")
    assert [10, 15] == client.cms().incrby("dim", ["foo", "bar"], [5, 15])
    assert [10, 15] == client.cms().query("dim", "foo", "bar")
    info = client.cms().info("dim")
    assert info["width"]
    assert 1000 == info["width"]
    assert 5 == info["depth"]
    assert 25 == info["count"]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_cms_merge(client):
    assert client.cms().initbydim("A", 1000, 5)
    assert client.cms().initbydim("B", 1000, 5)
    assert client.cms().initbydim("C", 1000, 5)
    assert client.cms().incrby("A", ["foo", "bar", "baz"], [5, 3, 9])
    assert client.cms().incrby("B", ["foo", "bar", "baz"], [2, 3, 1])
    assert [5, 3, 9] == client.cms().query("A", "foo", "bar", "baz")
    assert [2, 3, 1] == client.cms().query("B", "foo", "bar", "baz")
    assert client.cms().merge("C", 2, ["A", "B"])
    assert [7, 6, 10] == client.cms().query("C", "foo", "bar", "baz")
    assert client.cms().merge("C", 2, ["A", "B"], ["1", "2"])
    assert [9, 9, 11] == client.cms().query("C", "foo", "bar", "baz")
    assert client.cms().merge("C", 2, ["A", "B"], ["2", "3"])
    assert [16, 15, 21] == client.cms().query("C", "foo", "bar", "baz")


@pytest.mark.redismod
def test_topk(client):
    # test list with empty buckets
    assert client.topk().reserve("topk", 3, 50, 4, 0.9)
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
    ] == client.topk().add(
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
    assert [1, 1, 0, 0, 1, 0, 0] == client.topk().query(
        "topk", "A", "B", "C", "D", "E", "F", "G"
    )
    with pytest.deprecated_call():
        assert [4, 3, 2, 3, 3, 0, 1] == client.topk().count(
            "topk", "A", "B", "C", "D", "E", "F", "G"
        )

    # test full list
    assert client.topk().reserve("topklist", 3, 50, 3, 0.9)
    assert client.topk().add(
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
    assert ["A", "B", "E"] == client.topk().list("topklist")
    assert ["A", 4, "B", 3, "E", 3] == client.topk().list("topklist", withcount=True)
    info = client.topk().info("topklist")
    assert 3 == info["k"]
    assert 50 == info["width"]
    assert 3 == info["depth"]
    assert 0.9 == round(float(info["decay"]), 1)


@pytest.mark.redismod
def test_topk_list_with_special_words(client):
    # test list with empty buckets
    assert client.topk().reserve("topklist:specialwords", 5, 20, 4, 0.9)
    assert client.topk().add(
        "topklist:specialwords",
        "infinity",
        "B",
        "nan",
        "D",
        "-infinity",
        "infinity",
        "infinity",
        "B",
        "nan",
        "G",
        "D",
        "B",
        "D",
        "infinity",
        "-infinity",
        "-infinity",
    )
    assert ["infinity", "B", "D", "-infinity", "nan"] == client.topk().list(
        "topklist:specialwords"
    )


@pytest.mark.redismod
def test_topk_incrby(client):
    client.flushdb()
    assert client.topk().reserve("topk", 3, 10, 3, 1)
    assert [None, None, None] == client.topk().incrby(
        "topk", ["bar", "baz", "42"], [3, 6, 2]
    )
    assert [None, "bar"] == client.topk().incrby("topk", ["42", "xyzzy"], [8, 4])
    with pytest.deprecated_call():
        assert [3, 6, 10, 4, 0] == client.topk().count(
            "topk", "bar", "baz", "42", "xyzzy", 4
        )


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_reset(client):
    assert client.tdigest().create("tDigest", 10)
    # reset on empty histogram
    assert client.tdigest().reset("tDigest")
    # insert data-points into sketch
    assert client.tdigest().add("tDigest", list(range(10)))

    assert client.tdigest().reset("tDigest")
    # assert we have 0 unmerged
    info = client.tdigest().info("tDigest")
    assert_resp_response(
        client, 0, info.get("unmerged_nodes"), info.get("Unmerged nodes")
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_tdigest_merge(client):
    assert client.tdigest().create("to-tDigest", 10)
    assert client.tdigest().create("from-tDigest", 10)
    # insert data-points into sketch
    assert client.tdigest().add("from-tDigest", [1.0] * 10)
    assert client.tdigest().add("to-tDigest", [2.0] * 10)
    # merge from-tdigest into to-tdigest
    assert client.tdigest().merge("to-tDigest", 1, "from-tDigest")
    # we should now have 110 weight on to-histogram
    info = client.tdigest().info("to-tDigest")
    if is_resp2_connection(client):
        assert 20 == float(info["merged_weight"]) + float(info["unmerged_weight"])
    else:
        assert 20 == float(info["Merged weight"]) + float(info["Unmerged weight"])
    # test override
    assert client.tdigest().create("from-override", 10)
    assert client.tdigest().create("from-override-2", 10)
    assert client.tdigest().add("from-override", [3.0] * 10)
    assert client.tdigest().add("from-override-2", [4.0] * 10)
    assert client.tdigest().merge(
        "to-tDigest", 2, "from-override", "from-override-2", override=True
    )
    assert 3.0 == client.tdigest().min("to-tDigest")
    assert 4.0 == client.tdigest().max("to-tDigest")


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_min_and_max(client):
    assert client.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert client.tdigest().add("tDigest", [1, 2, 3])
    # min/max
    assert 3 == client.tdigest().max("tDigest")
    assert 1 == client.tdigest().min("tDigest")


@pytest.mark.experimental
@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.0", "bf")
def test_tdigest_quantile(client):
    assert client.tdigest().create("tDigest", 500)
    # insert data-points into sketch
    assert client.tdigest().add("tDigest", list([x * 0.01 for x in range(1, 10000)]))
    # assert min min/max have same result as quantile 0 and 1
    res = client.tdigest().quantile("tDigest", 1.0)
    assert client.tdigest().max("tDigest") == res[0]
    res = client.tdigest().quantile("tDigest", 0.0)
    assert client.tdigest().min("tDigest") == res[0]

    assert 1.0 == round(client.tdigest().quantile("tDigest", 0.01)[0], 2)
    assert 99.0 == round(client.tdigest().quantile("tDigest", 0.99)[0], 2)

    # test multiple quantiles
    assert client.tdigest().create("t-digest", 100)
    assert client.tdigest().add("t-digest", [1, 2, 3, 4, 5])
    assert [3.0, 5.0] == client.tdigest().quantile("t-digest", 0.5, 0.8)


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_cdf(client):
    assert client.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert client.tdigest().add("tDigest", list(range(1, 10)))
    assert 0.1 == round(client.tdigest().cdf("tDigest", 1.0)[0], 1)
    assert 0.9 == round(client.tdigest().cdf("tDigest", 9.0)[0], 1)
    res = client.tdigest().cdf("tDigest", 1.0, 9.0)
    assert [0.1, 0.9] == [round(x, 1) for x in res]


@pytest.mark.experimental
@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.0", "bf")
def test_tdigest_trimmed_mean(client):
    assert client.tdigest().create("tDigest", 100)
    # insert data-points into sketch
    assert client.tdigest().add("tDigest", list(range(1, 10)))
    assert 5 == client.tdigest().trimmed_mean("tDigest", 0.1, 0.9)
    assert 4.5 == client.tdigest().trimmed_mean("tDigest", 0.4, 0.5)


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_rank(client):
    assert client.tdigest().create("t-digest", 500)
    assert client.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == client.tdigest().rank("t-digest", -1)[0]
    assert 0 == client.tdigest().rank("t-digest", 0)[0]
    assert 10 == client.tdigest().rank("t-digest", 10)[0]
    assert [-1, 20, 9] == client.tdigest().rank("t-digest", -20, 20, 9)


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_revrank(client):
    assert client.tdigest().create("t-digest", 500)
    assert client.tdigest().add("t-digest", list(range(0, 20)))
    assert -1 == client.tdigest().revrank("t-digest", 20)[0]
    assert 19 == client.tdigest().revrank("t-digest", 0)[0]
    assert [-1, 19, 9] == client.tdigest().revrank("t-digest", 21, 0, 10)


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_byrank(client):
    assert client.tdigest().create("t-digest", 500)
    assert client.tdigest().add("t-digest", list(range(1, 11)))
    assert 1 == client.tdigest().byrank("t-digest", 0)[0]
    assert 10 == client.tdigest().byrank("t-digest", 9)[0]
    assert client.tdigest().byrank("t-digest", 100)[0] == inf
    with pytest.raises(redis.ResponseError):
        client.tdigest().byrank("t-digest", -1)[0]


@pytest.mark.experimental
@pytest.mark.redismod
def test_tdigest_byrevrank(client):
    assert client.tdigest().create("t-digest", 500)
    assert client.tdigest().add("t-digest", list(range(1, 11)))
    assert 10 == client.tdigest().byrevrank("t-digest", 0)[0]
    assert 1 == client.tdigest().byrevrank("t-digest", 9)[0]
    assert client.tdigest().byrevrank("t-digest", 100)[0] == -inf
    with pytest.raises(redis.ResponseError):
        client.tdigest().byrevrank("t-digest", -1)[0]


# # def test_pipeline(client):
#     pipeline = client.bf().pipeline()
#     assert not client.bf().execute_command("get pipeline")
#
#     assert client.bf().create("pipeline", 0.01, 1000)
#     for i in range(100):
#         pipeline.add("pipeline", i)
#     for i in range(100):
#         assert not (client.bf().exists("pipeline", i))
#
#     pipeline.execute()
#
#     for i in range(100):
#         assert client.bf().exists("pipeline", i)
