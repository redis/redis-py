import math
import time
from time import sleep

import pytest
import redis

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


@pytest.fixture
def client(decoded_r):
    decoded_r.flushdb()
    return decoded_r


@pytest.mark.redismod
def test_create(client):
    assert client.ts().create(1)
    assert client.ts().create(2, retention_msecs=5)
    assert client.ts().create(3, labels={"Redis": "Labs"})
    assert client.ts().create(4, retention_msecs=20, labels={"Time": "Series"})
    info = client.ts().info(4)
    assert_resp_response(
        client, 20, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert "Series" == info["labels"]["Time"]

    # Test for a chunk size of 128 Bytes
    assert client.ts().create("time-serie-1", chunk_size=128)
    info = client.ts().info("time-serie-1")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
def test_create_duplicate_policy(client):
    # Test for duplicate policy
    for duplicate_policy in ["block", "last", "first", "min", "max"]:
        ts_name = f"time-serie-ooo-{duplicate_policy}"
        assert client.ts().create(ts_name, duplicate_policy=duplicate_policy)
        info = client.ts().info(ts_name)
        assert_resp_response(
            client,
            duplicate_policy,
            info.get("duplicate_policy"),
            info.get("duplicatePolicy"),
        )


@pytest.mark.redismod
def test_alter(client):
    assert client.ts().create(1)
    info = client.ts().info(1)
    assert_resp_response(
        client, 0, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert client.ts().alter(1, retention_msecs=10)
    assert {} == client.ts().info(1)["labels"]
    info = client.ts().info(1)
    assert_resp_response(
        client, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert client.ts().alter(1, labels={"Time": "Series"})
    assert "Series" == client.ts().info(1)["labels"]["Time"]
    info = client.ts().info(1)
    assert_resp_response(
        client, 10, info.get("retention_msecs"), info.get("retentionTime")
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
def test_alter_duplicate_policy(client):
    assert client.ts().create(1)
    info = client.ts().info(1)
    assert_resp_response(
        client, None, info.get("duplicate_policy"), info.get("duplicatePolicy")
    )
    assert client.ts().alter(1, duplicate_policy="min")
    info = client.ts().info(1)
    assert_resp_response(
        client, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
def test_add(client):
    assert 1 == client.ts().add(1, 1, 1)
    assert 2 == client.ts().add(2, 2, 3, retention_msecs=10)
    assert 3 == client.ts().add(3, 3, 2, labels={"Redis": "Labs"})
    assert 4 == client.ts().add(
        4, 4, 2, retention_msecs=10, labels={"Redis": "Labs", "Time": "Series"}
    )

    assert abs(time.time() - float(client.ts().add(5, "*", 1)) / 1000) < 1.0

    info = client.ts().info(4)
    assert_resp_response(
        client, 10, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert "Labs" == info["labels"]["Redis"]

    # Test for a chunk size of 128 Bytes on TS.ADD
    assert client.ts().add("time-serie-1", 1, 10.0, chunk_size=128)
    info = client.ts().info("time-serie-1")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
def test_add_on_duplicate(client):
    # Test for duplicate policy BLOCK
    assert 1 == client.ts().add("time-serie-add-ooo-block", 1, 5.0)
    with pytest.raises(Exception):
        client.ts().add("time-serie-add-ooo-block", 1, 5.0, on_duplicate="block")

    # Test for duplicate policy LAST
    assert 1 == client.ts().add("time-serie-add-ooo-last", 1, 5.0)
    assert 1 == client.ts().add("time-serie-add-ooo-last", 1, 10.0, on_duplicate="last")
    assert 10.0 == client.ts().get("time-serie-add-ooo-last")[1]

    # Test for duplicate policy FIRST
    assert 1 == client.ts().add("time-serie-add-ooo-first", 1, 5.0)
    assert 1 == client.ts().add(
        "time-serie-add-ooo-first", 1, 10.0, on_duplicate="first"
    )
    assert 5.0 == client.ts().get("time-serie-add-ooo-first")[1]

    # Test for duplicate policy MAX
    assert 1 == client.ts().add("time-serie-add-ooo-max", 1, 5.0)
    assert 1 == client.ts().add("time-serie-add-ooo-max", 1, 10.0, on_duplicate="max")
    assert 10.0 == client.ts().get("time-serie-add-ooo-max")[1]

    # Test for duplicate policy MIN
    assert 1 == client.ts().add("time-serie-add-ooo-min", 1, 5.0)
    assert 1 == client.ts().add("time-serie-add-ooo-min", 1, 10.0, on_duplicate="min")
    assert 5.0 == client.ts().get("time-serie-add-ooo-min")[1]


@pytest.mark.redismod
def test_madd(client):
    client.ts().create("a")
    assert [1, 2, 3] == client.ts().madd([("a", 1, 5), ("a", 2, 10), ("a", 3, 15)])


@pytest.mark.redismod
def test_madd_missing_timeseries(client):
    response = client.ts().madd([("a", 1, 5), ("a", 2, 10)])
    assert isinstance(response, list)
    assert len(response) == 2
    assert isinstance(response[0], redis.ResponseError)
    assert isinstance(response[1], redis.ResponseError)


@pytest.mark.redismod
def test_incrby_decrby(client):
    for _ in range(100):
        assert client.ts().incrby(1, 1)
        sleep(0.001)
    assert 100 == client.ts().get(1)[1]
    for _ in range(100):
        assert client.ts().decrby(1, 1)
        sleep(0.001)
    assert 0 == client.ts().get(1)[1]

    assert client.ts().incrby(2, 1.5, timestamp=5)
    assert_resp_response(client, client.ts().get(2), (5, 1.5), [5, 1.5])
    assert client.ts().incrby(2, 2.25, timestamp=7)
    assert_resp_response(client, client.ts().get(2), (7, 3.75), [7, 3.75])
    assert client.ts().decrby(2, 1.5, timestamp=15)
    assert_resp_response(client, client.ts().get(2), (15, 2.25), [15, 2.25])

    # Test for a chunk size of 128 Bytes on TS.INCRBY
    assert client.ts().incrby("time-serie-1", 10, chunk_size=128)
    info = client.ts().info("time-serie-1")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))

    # Test for a chunk size of 128 Bytes on TS.DECRBY
    assert client.ts().decrby("time-serie-2", 10, chunk_size=128)
    info = client.ts().info("time-serie-2")
    assert_resp_response(client, 128, info.get("chunk_size"), info.get("chunkSize"))


@pytest.mark.redismod
def test_create_and_delete_rule(client):
    # test rule creation
    time = 100
    client.ts().create(1)
    client.ts().create(2)
    client.ts().createrule(1, 2, "avg", 100)
    for i in range(50):
        client.ts().add(1, time + i * 2, 1)
        client.ts().add(1, time + i * 2 + 1, 2)
    client.ts().add(1, time * 2, 1.5)
    assert round(client.ts().get(2)[1], 5) == 1.5
    info = client.ts().info(1)
    if is_resp2_connection(client):
        assert info.rules[0][1] == 100
    else:
        assert info["rules"]["2"][0] == 100

    # test rule deletion
    client.ts().deleterule(1, 2)
    info = client.ts().info(1)
    assert not info["rules"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_del_range(client):
    try:
        client.ts().delete("test", 0, 100)
    except redis.ResponseError as e:
        assert "key does not exist" in str(e)

    for i in range(100):
        client.ts().add(1, i, i % 7)
    assert 22 == client.ts().delete(1, 0, 21)
    assert [] == client.ts().range(1, 0, 21)
    assert_resp_response(client, client.ts().range(1, 22, 22), [(22, 1.0)], [[22, 1.0]])


@pytest.mark.redismod
def test_range(client):
    for i in range(100):
        client.ts().add(1, i, i % 7)
    assert 100 == len(client.ts().range(1, 0, 200))
    for i in range(100):
        client.ts().add(1, i + 200, i % 7)
    assert 200 == len(client.ts().range(1, 0, 500))
    # last sample isn't returned
    assert 20 == len(
        client.ts().range(1, 0, 500, aggregation_type="avg", bucket_size_msec=10)
    )
    assert 10 == len(client.ts().range(1, 0, 500, count=10))


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_range_advanced(client):
    for i in range(100):
        client.ts().add(1, i, i % 7)
        client.ts().add(1, i + 200, i % 7)

    assert 2 == len(
        client.ts().range(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    res = client.ts().range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
    )
    assert_resp_response(client, res, [(0, 10.0), (10, 1.0)], [[0, 10.0], [10, 1.0]])
    res = client.ts().range(
        1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=5
    )
    assert_resp_response(client, res, [(0, 5.0), (5, 6.0)], [[0, 5.0], [5, 6.0]])
    res = client.ts().range(1, 0, 10, aggregation_type="twa", bucket_size_msec=10)
    assert_resp_response(client, res, [(0, 2.55), (10, 3.0)], [[0, 2.55], [10, 3.0]])


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_range_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2")
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    assert_resp_response(
        client,
        timeseries.range("t1", 0, 20),
        [(1, 1.0), (2, 3.0), (11, 7.0), (13, 1.0)],
        [[1, 1.0], [2, 3.0], [11, 7.0], [13, 1.0]],
    )
    assert_resp_response(client, timeseries.range("t2", 0, 10), [(0, 4.0)], [[0, 4.0]])
    res = timeseries.range("t2", 0, 10, latest=True)
    assert_resp_response(client, res, [(0, 4.0), (10, 8.0)], [[0, 4.0], [10, 8.0]])
    assert_resp_response(
        client, timeseries.range("t2", 0, 9, latest=True), [(0, 4.0)], [[0, 4.0]]
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_range_bucket_timestamp(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.range(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(10, 4.0), (50, 3.0), (70, 5.0)],
        [[10, 4.0], [50, 3.0], [70, 5.0]],
    )
    assert_resp_response(
        client,
        timeseries.range(
            "t1",
            0,
            100,
            align=0,
            aggregation_type="max",
            bucket_size_msec=10,
            bucket_timestamp="+",
        ),
        [(20, 4.0), (60, 3.0), (80, 5.0)],
        [[20, 4.0], [60, 3.0], [80, 5.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_range_empty(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.range(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(10, 4.0), (50, 3.0), (70, 5.0)],
        [[10, 4.0], [50, 3.0], [70, 5.0]],
    )
    res = timeseries.range(
        "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10, empty=True
    )
    for i in range(len(res)):
        if math.isnan(res[i][1]):
            res[i] = (res[i][0], None)
    resp2_expected = [
        (10, 4.0),
        (20, None),
        (30, None),
        (40, None),
        (50, 3.0),
        (60, None),
        (70, 5.0),
    ]
    resp3_expected = [
        [10, 4.0],
        (20, None),
        (30, None),
        (40, None),
        [50, 3.0],
        (60, None),
        [70, 5.0],
    ]
    assert_resp_response(client, res, resp2_expected, resp3_expected)


@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_rev_range(client):
    for i in range(100):
        client.ts().add(1, i, i % 7)
    assert 100 == len(client.ts().range(1, 0, 200))
    for i in range(100):
        client.ts().add(1, i + 200, i % 7)
    assert 200 == len(client.ts().range(1, 0, 500))
    # first sample isn't returned
    assert 20 == len(
        client.ts().revrange(1, 0, 500, aggregation_type="avg", bucket_size_msec=10)
    )
    assert 10 == len(client.ts().revrange(1, 0, 500, count=10))
    assert 2 == len(
        client.ts().revrange(
            1,
            0,
            500,
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
    )
    assert_resp_response(
        client,
        client.ts().revrange(
            1, 0, 10, aggregation_type="count", bucket_size_msec=10, align="+"
        ),
        [(10, 1.0), (0, 10.0)],
        [[10, 1.0], [0, 10.0]],
    )
    assert_resp_response(
        client,
        client.ts().revrange(
            1, 0, 10, aggregation_type="count", bucket_size_msec=10, align=1
        ),
        [(1, 10.0), (0, 1.0)],
        [[1, 10.0], [0, 1.0]],
    )
    assert_resp_response(
        client,
        client.ts().revrange(1, 0, 10, aggregation_type="twa", bucket_size_msec=10),
        [(10, 3.0), (0, 2.55)],
        [[10, 3.0], [0, 2.55]],
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_revrange_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2")
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    res = timeseries.revrange("t2", 0, 10)
    assert_resp_response(client, res, [(0, 4.0)], [[0, 4.0]])
    res = timeseries.revrange("t2", 0, 10, latest=True)
    assert_resp_response(client, res, [(10, 8.0), (0, 4.0)], [[10, 8.0], [0, 4.0]])
    res = timeseries.revrange("t2", 0, 9, latest=True)
    assert_resp_response(client, res, [(0, 4.0)], [[0, 4.0]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_revrange_bucket_timestamp(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.revrange(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(70, 5.0), (50, 3.0), (10, 4.0)],
        [[70, 5.0], [50, 3.0], [10, 4.0]],
    )
    assert_resp_response(
        client,
        timeseries.range(
            "t1",
            0,
            100,
            align=0,
            aggregation_type="max",
            bucket_size_msec=10,
            bucket_timestamp="+",
        ),
        [(20, 4.0), (60, 3.0), (80, 5.0)],
        [[20, 4.0], [60, 3.0], [80, 5.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_revrange_empty(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.add("t1", 15, 1)
    timeseries.add("t1", 17, 4)
    timeseries.add("t1", 51, 3)
    timeseries.add("t1", 73, 5)
    timeseries.add("t1", 75, 3)
    assert_resp_response(
        client,
        timeseries.revrange(
            "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10
        ),
        [(70, 5.0), (50, 3.0), (10, 4.0)],
        [[70, 5.0], [50, 3.0], [10, 4.0]],
    )
    res = timeseries.revrange(
        "t1", 0, 100, align=0, aggregation_type="max", bucket_size_msec=10, empty=True
    )
    for i in range(len(res)):
        if math.isnan(res[i][1]):
            res[i] = (res[i][0], None)
    resp2_expected = [
        (70, 5.0),
        (60, None),
        (50, 3.0),
        (40, None),
        (30, None),
        (20, None),
        (10, 4.0),
    ]
    resp3_expected = [
        [70, 5.0],
        (60, None),
        [50, 3.0],
        (40, None),
        (30, None),
        (20, None),
        [10, 4.0],
    ]
    assert_resp_response(client, res, resp2_expected, resp3_expected)


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_mrange(client):
    client.ts().create(1, labels={"Test": "This", "team": "ny"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    for i in range(100):
        client.ts().add(1, i, i % 7)
        client.ts().add(2, i, i % 11)

    res = client.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    if is_resp2_connection(client):
        assert 100 == len(res[0]["1"][1])

        res = client.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert 10 == len(res[0]["1"][1])

        for i in range(100):
            client.ts().add(1, i + 200, i % 7)
        res = client.ts().mrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert 2 == len(res)
        assert 20 == len(res[0]["1"][1])

        # test withlabels
        assert {} == res[0]["1"][0]
        res = client.ts().mrange(0, 200, filters=["Test=This"], with_labels=True)
        assert {"Test": "This", "team": "ny"} == res[0]["1"][0]
    else:
        assert 100 == len(res["1"][2])

        res = client.ts().mrange(0, 200, filters=["Test=This"], count=10)
        assert 10 == len(res["1"][2])

        for i in range(100):
            client.ts().add(1, i + 200, i % 7)
        res = client.ts().mrange(
            0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
        )
        assert 2 == len(res)
        assert 20 == len(res["1"][2])

        # test withlabels
        assert {} == res["1"][0]
        res = client.ts().mrange(0, 200, filters=["Test=This"], with_labels=True)
        assert {"Test": "This", "team": "ny"} == res["1"][0]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_multi_range_advanced(client):
    client.ts().create(1, labels={"Test": "This", "team": "ny"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    for i in range(100):
        client.ts().add(1, i, i % 7)
        client.ts().add(2, i, i % 11)

    # test with selected labels
    res = client.ts().mrange(0, 200, filters=["Test=This"], select_labels=["team"])
    if is_resp2_connection(client):
        assert {"team": "ny"} == res[0]["1"][0]
        assert {"team": "sf"} == res[1]["2"][0]

        # test with filterby
        res = client.ts().mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert [(15, 1.0), (16, 2.0)] == res[0]["1"][1]

        # test groupby
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert [(0, 0.0), (1, 2.0), (2, 4.0), (3, 6.0)] == res[0]["Test=This"][1]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["Test=This"][1]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert 2 == len(res)
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[0]["team=ny"][1]
        assert [(0, 0.0), (1, 1.0), (2, 2.0), (3, 3.0)] == res[1]["team=sf"][1]

        # test align
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert [(0, 10.0), (10, 1.0)] == res[0]["1"][1]
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=5,
        )
        assert [(0, 5.0), (5, 6.0)] == res[0]["1"][1]
    else:
        assert {"team": "ny"} == res["1"][0]
        assert {"team": "sf"} == res["2"][0]

        # test with filterby
        res = client.ts().mrange(
            0,
            200,
            filters=["Test=This"],
            filter_by_ts=[i for i in range(10, 20)],
            filter_by_min_value=1,
            filter_by_max_value=2,
        )
        assert [[15, 1.0], [16, 2.0]] == res["1"][2]

        # test groupby
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
        )
        assert [[0, 0.0], [1, 2.0], [2, 4.0], [3, 6.0]] == res["Test=This"][3]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="Test", reduce="max"
        )
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["Test=This"][3]
        res = client.ts().mrange(
            0, 3, filters=["Test=This"], groupby="team", reduce="min"
        )
        assert 2 == len(res)
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=ny"][3]
        assert [[0, 0.0], [1, 1.0], [2, 2.0], [3, 3.0]] == res["team=sf"][3]

        # test align
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align="-",
        )
        assert [[0, 10.0], [10, 1.0]] == res["1"][2]
        res = client.ts().mrange(
            0,
            10,
            filters=["team=ny"],
            aggregation_type="count",
            bucket_size_msec=10,
            align=5,
        )
        assert [[0, 5.0], [5, 6.0]] == res["1"][2]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_mrange_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2", labels={"is_compaction": "true"})
    timeseries.create("t3")
    timeseries.create("t4", labels={"is_compaction": "true"})
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.createrule("t3", "t4", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    timeseries.add("t3", 1, 1)
    timeseries.add("t3", 2, 3)
    timeseries.add("t3", 11, 7)
    timeseries.add("t3", 13, 1)
    assert_resp_response(
        client,
        client.ts().mrange(0, 10, filters=["is_compaction=true"], latest=True),
        [{"t2": [{}, [(0, 4.0), (10, 8.0)]]}, {"t4": [{}, [(0, 4.0), (10, 8.0)]]}],
        {
            "t2": [{}, {"aggregators": []}, [[0, 4.0], [10, 8.0]]],
            "t4": [{}, {"aggregators": []}, [[0, 4.0], [10, 8.0]]],
        },
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.10.0", "timeseries")
def test_multi_reverse_range(client):
    client.ts().create(1, labels={"Test": "This", "team": "ny"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That", "team": "sf"})
    for i in range(100):
        client.ts().add(1, i, i % 7)
        client.ts().add(2, i, i % 11)

    res = client.ts().mrange(0, 200, filters=["Test=This"])
    assert 2 == len(res)
    if is_resp2_connection(client):
        assert 100 == len(res[0]["1"][1])
    else:
        assert 100 == len(res["1"][2])

    res = client.ts().mrange(0, 200, filters=["Test=This"], count=10)
    if is_resp2_connection(client):
        assert 10 == len(res[0]["1"][1])
    else:
        assert 10 == len(res["1"][2])

    for i in range(100):
        client.ts().add(1, i + 200, i % 7)
    res = client.ts().mrevrange(
        0, 500, filters=["Test=This"], aggregation_type="avg", bucket_size_msec=10
    )
    assert 2 == len(res)
    if is_resp2_connection(client):
        assert 20 == len(res[0]["1"][1])
        assert {} == res[0]["1"][0]
    else:
        assert 20 == len(res["1"][2])
        assert {} == res["1"][0]

    # test withlabels
    res = client.ts().mrevrange(0, 200, filters=["Test=This"], with_labels=True)
    if is_resp2_connection(client):
        assert {"Test": "This", "team": "ny"} == res[0]["1"][0]
    else:
        assert {"Test": "This", "team": "ny"} == res["1"][0]

    # test with selected labels
    res = client.ts().mrevrange(0, 200, filters=["Test=This"], select_labels=["team"])
    if is_resp2_connection(client):
        assert {"team": "ny"} == res[0]["1"][0]
        assert {"team": "sf"} == res[1]["2"][0]
    else:
        assert {"team": "ny"} == res["1"][0]
        assert {"team": "sf"} == res["2"][0]

    # test filterby
    res = client.ts().mrevrange(
        0,
        200,
        filters=["Test=This"],
        filter_by_ts=[i for i in range(10, 20)],
        filter_by_min_value=1,
        filter_by_max_value=2,
    )
    if is_resp2_connection(client):
        assert [(16, 2.0), (15, 1.0)] == res[0]["1"][1]
    else:
        assert [[16, 2.0], [15, 1.0]] == res["1"][2]

    # test groupby
    res = client.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="sum"
    )
    if is_resp2_connection(client):
        assert [(3, 6.0), (2, 4.0), (1, 2.0), (0, 0.0)] == res[0]["Test=This"][1]
    else:
        assert [[3, 6.0], [2, 4.0], [1, 2.0], [0, 0.0]] == res["Test=This"][3]
    res = client.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="Test", reduce="max"
    )
    if is_resp2_connection(client):
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["Test=This"][1]
    else:
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["Test=This"][3]
    res = client.ts().mrevrange(
        0, 3, filters=["Test=This"], groupby="team", reduce="min"
    )
    assert 2 == len(res)
    if is_resp2_connection(client):
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[0]["team=ny"][1]
        assert [(3, 3.0), (2, 2.0), (1, 1.0), (0, 0.0)] == res[1]["team=sf"][1]
    else:
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=ny"][3]
        assert [[3, 3.0], [2, 2.0], [1, 1.0], [0, 0.0]] == res["team=sf"][3]

    # test align
    res = client.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align="-",
    )
    if is_resp2_connection(client):
        assert [(10, 1.0), (0, 10.0)] == res[0]["1"][1]
    else:
        assert [[10, 1.0], [0, 10.0]] == res["1"][2]
    res = client.ts().mrevrange(
        0,
        10,
        filters=["team=ny"],
        aggregation_type="count",
        bucket_size_msec=10,
        align=1,
    )
    if is_resp2_connection(client):
        assert [(1, 10.0), (0, 1.0)] == res[0]["1"][1]
    else:
        assert [[1, 10.0], [0, 1.0]] == res["1"][2]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_mrevrange_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2", labels={"is_compaction": "true"})
    timeseries.create("t3")
    timeseries.create("t4", labels={"is_compaction": "true"})
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.createrule("t3", "t4", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    timeseries.add("t3", 1, 1)
    timeseries.add("t3", 2, 3)
    timeseries.add("t3", 11, 7)
    timeseries.add("t3", 13, 1)
    assert_resp_response(
        client,
        client.ts().mrevrange(0, 10, filters=["is_compaction=true"], latest=True),
        [{"t2": [{}, [(10, 8.0), (0, 4.0)]]}, {"t4": [{}, [(10, 8.0), (0, 4.0)]]}],
        {
            "t2": [{}, {"aggregators": []}, [[10, 8.0], [0, 4.0]]],
            "t4": [{}, {"aggregators": []}, [[10, 8.0], [0, 4.0]]],
        },
    )


@pytest.mark.redismod
def test_get(client):
    name = "test"
    client.ts().create(name)
    assert not client.ts().get(name)
    client.ts().add(name, 2, 3)
    assert 2 == client.ts().get(name)[0]
    client.ts().add(name, 3, 4)
    assert 4 == client.ts().get(name)[1]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_get_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2")
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    assert_resp_response(client, timeseries.get("t2"), (0, 4.0), [0, 4.0])
    assert_resp_response(
        client, timeseries.get("t2", latest=True), (10, 8.0), [10, 8.0]
    )


@pytest.mark.onlynoncluster
@pytest.mark.redismod
def test_mget(client):
    client.ts().create(1, labels={"Test": "This"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That"})
    act_res = client.ts().mget(["Test=This"])
    exp_res = [{"1": [{}, None, None]}, {"2": [{}, None, None]}]
    exp_res_resp3 = {"1": [{}, []], "2": [{}, []]}
    assert_resp_response(client, act_res, exp_res, exp_res_resp3)
    client.ts().add(1, "*", 15)
    client.ts().add(2, "*", 25)
    res = client.ts().mget(["Test=This"])
    if is_resp2_connection(client):
        assert 15 == res[0]["1"][2]
        assert 25 == res[1]["2"][2]
    else:
        assert 15 == res["1"][1][1]
        assert 25 == res["2"][1][1]
    res = client.ts().mget(["Taste=That"])
    if is_resp2_connection(client):
        assert 25 == res[0]["2"][2]
    else:
        assert 25 == res["2"][1][1]

    # test with_labels
    if is_resp2_connection(client):
        assert {} == res[0]["2"][0]
    else:
        assert {} == res["2"][0]
    res = client.ts().mget(["Taste=That"], with_labels=True)
    if is_resp2_connection(client):
        assert {"Taste": "That", "Test": "This"} == res[0]["2"][0]
    else:
        assert {"Taste": "That", "Test": "This"} == res["2"][0]


@pytest.mark.onlynoncluster
@pytest.mark.redismod
@skip_ifmodversion_lt("1.8.0", "timeseries")
def test_mget_latest(client: redis.Redis):
    timeseries = client.ts()
    timeseries.create("t1")
    timeseries.create("t2", labels={"is_compaction": "true"})
    timeseries.createrule("t1", "t2", aggregation_type="sum", bucket_size_msec=10)
    timeseries.add("t1", 1, 1)
    timeseries.add("t1", 2, 3)
    timeseries.add("t1", 11, 7)
    timeseries.add("t1", 13, 1)
    res = timeseries.mget(filters=["is_compaction=true"])
    assert_resp_response(client, res, [{"t2": [{}, 0, 4.0]}], {"t2": [{}, [0, 4.0]]})
    res = timeseries.mget(filters=["is_compaction=true"], latest=True)
    assert_resp_response(client, res, [{"t2": [{}, 10, 8.0]}], {"t2": [{}, [10, 8.0]]})


@pytest.mark.redismod
def test_info(client):
    client.ts().create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.ts().info(1)
    assert_resp_response(
        client, 5, info.get("retention_msecs"), info.get("retentionTime")
    )
    assert info["labels"]["currentLabel"] == "currentData"


@pytest.mark.redismod
@skip_ifmodversion_lt("1.4.0", "timeseries")
def test_info_duplicate_policy(client):
    client.ts().create(1, retention_msecs=5, labels={"currentLabel": "currentData"})
    info = client.ts().info(1)
    assert_resp_response(
        client, None, info.get("duplicate_policy"), info.get("duplicatePolicy")
    )

    client.ts().create("time-serie-2", duplicate_policy="min")
    info = client.ts().info("time-serie-2")
    assert_resp_response(
        client, "min", info.get("duplicate_policy"), info.get("duplicatePolicy")
    )


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_query_index(client):
    client.ts().create(1, labels={"Test": "This"})
    client.ts().create(2, labels={"Test": "This", "Taste": "That"})
    assert 2 == len(client.ts().queryindex(["Test=This"]))
    assert 1 == len(client.ts().queryindex(["Taste=That"]))
    assert_resp_response(client, client.ts().queryindex(["Taste=That"]), [2], ["2"])


@pytest.mark.redismod
def test_pipeline(client):
    pipeline = client.ts().pipeline()
    pipeline.create("with_pipeline")
    for i in range(100):
        pipeline.add("with_pipeline", i, 1.1 * i)
    pipeline.execute()

    info = client.ts().info("with_pipeline")
    assert_resp_response(
        client, 99, info.get("last_timestamp"), info.get("lastTimestamp")
    )
    assert_resp_response(
        client, 100, info.get("total_samples"), info.get("totalSamples")
    )
    assert client.ts().get("with_pipeline")[1] == 99 * 1.1


@pytest.mark.redismod
def test_uncompressed(client):
    client.ts().create("compressed")
    client.ts().create("uncompressed", uncompressed=True)
    for i in range(1000):
        client.ts().add("compressed", i, i)
        client.ts().add("uncompressed", i, i)
    compressed_info = client.ts().info("compressed")
    uncompressed_info = client.ts().info("uncompressed")
    if is_resp2_connection(client):
        assert compressed_info.memory_usage < uncompressed_info.memory_usage
    else:
        assert compressed_info["memoryUsage"] < uncompressed_info["memoryUsage"]


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_create_with_insertion_filters(client):
    client.ts().create(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1000 == client.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == client.ts().add("time-series-1", 1010, 11.0)
    assert 1010 == client.ts().add("time-series-1", 1013, 10.0)
    assert 1020 == client.ts().add("time-series-1", 1020, 11.5)
    assert 1021 == client.ts().add("time-series-1", 1021, 22.0)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1020, 11.5), (1021, 22.0)],
        [[1000, 1.0], [1010, 11.0], [1020, 11.5], [1021, 22.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_create_with_insertion_filters_other_duplicate_policy(client):
    client.ts().create(
        "time-series-1",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1000 == client.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == client.ts().add("time-series-1", 1010, 11.0)
    # Still accepted because the duplicate_policy is not `last`.
    assert 1013 == client.ts().add("time-series-1", 1013, 10.0)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1013, 10)],
        [[1000, 1.0], [1010, 11.0], [1013, 10]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_alter_with_insertion_filters(client):
    assert 1000 == client.ts().add("time-series-1", 1000, 1.0)
    assert 1010 == client.ts().add("time-series-1", 1010, 11.0)
    assert 1013 == client.ts().add("time-series-1", 1013, 10.0)

    client.ts().alter(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1013 == client.ts().add("time-series-1", 1015, 11.5)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1000, 1.0), (1010, 11.0), (1013, 10.0)],
        [[1000, 1.0], [1010, 11.0], [1013, 10.0]],
    )


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_add_with_insertion_filters(client):
    assert 1000 == client.ts().add(
        "time-series-1",
        1000,
        1.0,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == client.ts().add("time-series-1", 1004, 3.0)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, 1.0)], [[1000, 1.0]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_incrby_with_insertion_filters(client):
    assert 1000 == client.ts().incrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == client.ts().incrby("time-series-1", 3.0, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, 1.0)], [[1000, 1.0]])

    assert 1000 == client.ts().incrby("time-series-1", 10.1, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, 11.1)], [[1000, 11.1]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_decrby_with_insertion_filters(client):
    assert 1000 == client.ts().decrby(
        "time-series-1",
        1.0,
        timestamp=1000,
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )

    assert 1000 == client.ts().decrby("time-series-1", 3.0, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, -1.0)], [[1000, -1.0]])

    assert 1000 == client.ts().decrby("time-series-1", 10.1, timestamp=1000)

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(client, data_points, [(1000, -11.1)], [[1000, -11.1]])


@pytest.mark.redismod
@skip_ifmodversion_lt("1.12.0", "timeseries")
def test_madd_with_insertion_filters(client):
    client.ts().create(
        "time-series-1",
        duplicate_policy="last",
        ignore_max_time_diff=5,
        ignore_max_val_diff=10.0,
    )
    assert 1010 == client.ts().add("time-series-1", 1010, 1.0)
    assert [1010, 1010, 1020, 1021] == client.ts().madd(
        [
            ("time-series-1", 1011, 11.0),
            ("time-series-1", 1013, 10.0),
            ("time-series-1", 1020, 2.0),
            ("time-series-1", 1021, 22.0),
        ]
    )

    data_points = client.ts().range("time-series-1", "-", "+")
    assert_resp_response(
        client,
        data_points,
        [(1010, 1.0), (1020, 2.0), (1021, 22.0)],
        [[1010, 1.0], [1020, 2.0], [1021, 22.0]],
    )
