import bz2
import csv
import os
import time
from io import TextIOWrapper

import numpy as np
import pytest
import pytest_asyncio
import redis.asyncio as redis
import redis.commands.search
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search import AsyncSearch
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
from redis.commands.search.suggestion import Suggestion
from tests.conftest import (
    is_resp2_connection,
    skip_if_redis_enterprise,
    skip_if_resp_version,
    skip_ifmodversion_lt,
)

WILL_PLAY_TEXT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "testdata", "will_play_text.csv.bz2")
)

TITLES_CSV = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "testdata", "titles.csv")
)


@pytest_asyncio.fixture()
async def decoded_r(create_redis, stack_url):
    return await create_redis(decode_responses=True, url=stack_url)


async def waitForIndex(env, idx, timeout=None):
    delay = 0.1
    while True:
        res = await env.execute_command("FT.INFO", idx)
        try:
            if int(res[res.index("indexing") + 1]) == 0:
                break
        except ValueError:
            break
        except AttributeError:
            try:
                if int(res["indexing"]) == 0:
                    break
            except ValueError:
                break

        time.sleep(delay)
        if timeout is not None:
            timeout -= delay
            if timeout <= 0:
                break


def getClient(decoded_r: redis.Redis):
    """
    Gets a client client attached to an index name which is ready to be
    created
    """
    return decoded_r


async def createIndex(decoded_r, num_docs=100, definition=None):
    try:
        await decoded_r.create_index(
            (TextField("play", weight=5.0), TextField("txt"), NumericField("chapter")),
            definition=definition,
        )
    except redis.ResponseError:
        await decoded_r.dropindex(delete_documents=True)
        return createIndex(decoded_r, num_docs=num_docs, definition=definition)

    chapters = {}
    bzfp = TextIOWrapper(bz2.BZ2File(WILL_PLAY_TEXT), encoding="utf8")

    r = csv.reader(bzfp, delimiter=";")
    for n, line in enumerate(r):
        play, chapter, _, text = line[1], line[2], line[4], line[5]

        key = f"{play}:{chapter}".lower()
        d = chapters.setdefault(key, {})
        d["play"] = play
        d["txt"] = d.get("txt", "") + " " + text
        d["chapter"] = int(chapter or 0)
        if len(chapters) == num_docs:
            break

    indexer = decoded_r.batch_indexer(chunk_size=50)
    assert isinstance(indexer, AsyncSearch.BatchIndexer)
    assert 50 == indexer.chunk_size

    for key, doc in chapters.items():
        await indexer.client.client.hset(key, mapping=doc)
    await indexer.commit()


@pytest.mark.redismod
async def test_client(decoded_r: redis.Redis):
    num_docs = 500
    await createIndex(decoded_r.ft(), num_docs=num_docs)
    await waitForIndex(decoded_r, "idx")
    # verify info
    info = await decoded_r.ft().info()
    for k in [
        "index_name",
        "index_options",
        "attributes",
        "num_docs",
        "max_doc_id",
        "num_terms",
        "num_records",
        "inverted_sz_mb",
        "offset_vectors_sz_mb",
        "doc_table_size_mb",
        "key_table_size_mb",
        "records_per_doc_avg",
        "bytes_per_record_avg",
        "offsets_per_term_avg",
        "offset_bits_per_record_avg",
    ]:
        assert k in info

    assert decoded_r.ft().index_name == info["index_name"]
    assert num_docs == int(info["num_docs"])

    res = await decoded_r.ft().search("henry iv")
    if is_resp2_connection(decoded_r):
        assert isinstance(res, Result)
        assert 225 == res.total
        assert 10 == len(res.docs)
        assert res.duration > 0

        for doc in res.docs:
            assert doc.id
            assert doc.play == "Henry IV"
            assert len(doc.txt) > 0

        # test no content
        res = await decoded_r.ft().search(Query("king").no_content())
        assert 194 == res.total
        assert 10 == len(res.docs)
        for doc in res.docs:
            assert "txt" not in doc.__dict__
            assert "play" not in doc.__dict__

        # test verbatim vs no verbatim
        total = (await decoded_r.ft().search(Query("kings").no_content())).total
        vtotal = (
            await decoded_r.ft().search(Query("kings").no_content().verbatim())
        ).total
        assert total > vtotal

        # test in fields
        txt_total = (
            await decoded_r.ft().search(Query("henry").no_content().limit_fields("txt"))
        ).total
        play_total = (
            await decoded_r.ft().search(
                Query("henry").no_content().limit_fields("play")
            )
        ).total
        both_total = (
            await decoded_r.ft().search(
                Query("henry").no_content().limit_fields("play", "txt")
            )
        ).total
        assert 129 == txt_total
        assert 494 == play_total
        assert 494 == both_total

        # test load_document
        doc = await decoded_r.ft().load_document("henry vi part 3:62")
        assert doc is not None
        assert "henry vi part 3:62" == doc.id
        assert doc.play == "Henry VI Part 3"
        assert len(doc.txt) > 0

        # test in-keys
        ids = [x.id for x in (await decoded_r.ft().search(Query("henry"))).docs]
        assert 10 == len(ids)
        subset = ids[:5]
        docs = await decoded_r.ft().search(Query("henry").limit_ids(*subset))
        assert len(subset) == docs.total
        ids = [x.id for x in docs.docs]
        assert set(ids) == set(subset)

        # test slop and in order
        assert 193 == (await decoded_r.ft().search(Query("henry king"))).total
        assert (
            3
            == (
                await decoded_r.ft().search(Query("henry king").slop(0).in_order())
            ).total
        )
        assert (
            52
            == (
                await decoded_r.ft().search(Query("king henry").slop(0).in_order())
            ).total
        )
        assert 53 == (await decoded_r.ft().search(Query("henry king").slop(0))).total
        assert 167 == (await decoded_r.ft().search(Query("henry king").slop(100))).total

        # test delete document
        await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = await decoded_r.ft().search(Query("death of a salesman"))
        assert 1 == res.total

        assert 1 == await decoded_r.ft().delete_document("doc-5ghs2")
        res = await decoded_r.ft().search(Query("death of a salesman"))
        assert 0 == res.total
        assert 0 == await decoded_r.ft().delete_document("doc-5ghs2")

        await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = await decoded_r.ft().search(Query("death of a salesman"))
        assert 1 == res.total
        await decoded_r.ft().delete_document("doc-5ghs2")
    else:
        assert isinstance(res, dict)
        assert 225 == res["total_results"]
        assert 10 == len(res["results"])

        for doc in res["results"]:
            assert doc["id"]
            assert doc["extra_attributes"]["play"] == "Henry IV"
            assert len(doc["extra_attributes"]["txt"]) > 0

        # test no content
        res = await decoded_r.ft().search(Query("king").no_content())
        assert 194 == res["total_results"]
        assert 10 == len(res["results"])
        for doc in res["results"]:
            assert "extra_attributes" not in doc.keys()

        # test verbatim vs no verbatim
        total = (await decoded_r.ft().search(Query("kings").no_content()))[
            "total_results"
        ]
        vtotal = (await decoded_r.ft().search(Query("kings").no_content().verbatim()))[
            "total_results"
        ]
        assert total > vtotal

        # test in fields
        txt_total = (
            await decoded_r.ft().search(Query("henry").no_content().limit_fields("txt"))
        )["total_results"]
        play_total = (
            await decoded_r.ft().search(
                Query("henry").no_content().limit_fields("play")
            )
        )["total_results"]
        both_total = (
            await decoded_r.ft().search(
                Query("henry").no_content().limit_fields("play", "txt")
            )
        )["total_results"]
        assert 129 == txt_total
        assert 494 == play_total
        assert 494 == both_total

        # test load_document
        doc = await decoded_r.ft().load_document("henry vi part 3:62")
        assert doc is not None
        assert "henry vi part 3:62" == doc.id
        assert doc.play == "Henry VI Part 3"
        assert len(doc.txt) > 0

        # test in-keys
        ids = [
            x["id"] for x in (await decoded_r.ft().search(Query("henry")))["results"]
        ]
        assert 10 == len(ids)
        subset = ids[:5]
        docs = await decoded_r.ft().search(Query("henry").limit_ids(*subset))
        assert len(subset) == docs["total_results"]
        ids = [x["id"] for x in docs["results"]]
        assert set(ids) == set(subset)

        # test slop and in order
        assert (
            193 == (await decoded_r.ft().search(Query("henry king")))["total_results"]
        )
        assert (
            3
            == (await decoded_r.ft().search(Query("henry king").slop(0).in_order()))[
                "total_results"
            ]
        )
        assert (
            52
            == (await decoded_r.ft().search(Query("king henry").slop(0).in_order()))[
                "total_results"
            ]
        )
        assert (
            53
            == (await decoded_r.ft().search(Query("henry king").slop(0)))[
                "total_results"
            ]
        )
        assert (
            167
            == (await decoded_r.ft().search(Query("henry king").slop(100)))[
                "total_results"
            ]
        )

        # test delete document
        await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = await decoded_r.ft().search(Query("death of a salesman"))
        assert 1 == res["total_results"]

        assert 1 == await decoded_r.ft().delete_document("doc-5ghs2")
        res = await decoded_r.ft().search(Query("death of a salesman"))
        assert 0 == res["total_results"]
        assert 0 == await decoded_r.ft().delete_document("doc-5ghs2")

        await decoded_r.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = await decoded_r.ft().search(Query("death of a salesman"))
        assert 1 == res["total_results"]
        await decoded_r.ft().delete_document("doc-5ghs2")


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_scores(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("txt"),))

    await decoded_r.hset("doc1", mapping={"txt": "foo baz"})
    await decoded_r.hset("doc2", mapping={"txt": "foo bar"})

    q = Query("foo ~bar").with_scores()
    res = await decoded_r.ft().search(q)
    if is_resp2_connection(decoded_r):
        assert 2 == res.total
        assert "doc2" == res.docs[0].id
        assert 3.0 == res.docs[0].score
        assert "doc1" == res.docs[1].id
    else:
        assert 2 == res["total_results"]
        assert "doc2" == res["results"][0]["id"]
        assert 3.0 == res["results"][0]["score"]
        assert "doc1" == res["results"][1]["id"]


@pytest.mark.redismod
async def test_stopwords(decoded_r: redis.Redis):
    stopwords = ["foo", "bar", "baz"]
    await decoded_r.ft().create_index((TextField("txt"),), stopwords=stopwords)
    await decoded_r.hset("doc1", mapping={"txt": "foo bar"})
    await decoded_r.hset("doc2", mapping={"txt": "hello world"})
    await waitForIndex(decoded_r, "idx")

    q1 = Query("foo bar").no_content()
    q2 = Query("foo bar hello world").no_content()
    res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)
    if is_resp2_connection(decoded_r):
        assert 0 == res1.total
        assert 1 == res2.total
    else:
        assert 0 == res1["total_results"]
        assert 1 == res2["total_results"]


@pytest.mark.redismod
async def test_filters(decoded_r: redis.Redis):
    await decoded_r.ft().create_index(
        (TextField("txt"), NumericField("num"), GeoField("loc"))
    )
    await decoded_r.hset(
        "doc1", mapping={"txt": "foo bar", "num": 3.141, "loc": "-0.441,51.458"}
    )
    await decoded_r.hset(
        "doc2", mapping={"txt": "foo baz", "num": 2, "loc": "-0.1,51.2"}
    )

    await waitForIndex(decoded_r, "idx")
    # Test numerical filter
    q1 = Query("foo").add_filter(NumericFilter("num", 0, 2)).no_content()
    q2 = (
        Query("foo")
        .add_filter(NumericFilter("num", 2, NumericFilter.INF, minExclusive=True))
        .no_content()
    )
    res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)

    if is_resp2_connection(decoded_r):
        assert 1 == res1.total
        assert 1 == res2.total
        assert "doc2" == res1.docs[0].id
        assert "doc1" == res2.docs[0].id
    else:
        assert 1 == res1["total_results"]
        assert 1 == res2["total_results"]
        assert "doc2" == res1["results"][0]["id"]
        assert "doc1" == res2["results"][0]["id"]

    # Test geo filter
    q1 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 10)).no_content()
    q2 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 100)).no_content()
    res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)

    if is_resp2_connection(decoded_r):
        assert 1 == res1.total
        assert 2 == res2.total
        assert "doc1" == res1.docs[0].id

        # Sort results, after RDB reload order may change
        res = [res2.docs[0].id, res2.docs[1].id]
        res.sort()
        assert ["doc1", "doc2"] == res
    else:
        assert 1 == res1["total_results"]
        assert 2 == res2["total_results"]
        assert "doc1" == res1["results"][0]["id"]

        # Sort results, after RDB reload order may change
        res = [res2["results"][0]["id"], res2["results"][1]["id"]]
        res.sort()
        assert ["doc1", "doc2"] == res


@pytest.mark.redismod
async def test_sort_by(decoded_r: redis.Redis):
    await decoded_r.ft().create_index(
        (TextField("txt"), NumericField("num", sortable=True))
    )
    await decoded_r.hset("doc1", mapping={"txt": "foo bar", "num": 1})
    await decoded_r.hset("doc2", mapping={"txt": "foo baz", "num": 2})
    await decoded_r.hset("doc3", mapping={"txt": "foo qux", "num": 3})

    # Test sort
    q1 = Query("foo").sort_by("num", asc=True).no_content()
    q2 = Query("foo").sort_by("num", asc=False).no_content()
    res1, res2 = await decoded_r.ft().search(q1), await decoded_r.ft().search(q2)

    if is_resp2_connection(decoded_r):
        assert 3 == res1.total
        assert "doc1" == res1.docs[0].id
        assert "doc2" == res1.docs[1].id
        assert "doc3" == res1.docs[2].id
        assert 3 == res2.total
        assert "doc1" == res2.docs[2].id
        assert "doc2" == res2.docs[1].id
        assert "doc3" == res2.docs[0].id
    else:
        assert 3 == res1["total_results"]
        assert "doc1" == res1["results"][0]["id"]
        assert "doc2" == res1["results"][1]["id"]
        assert "doc3" == res1["results"][2]["id"]
        assert 3 == res2["total_results"]
        assert "doc1" == res2["results"][2]["id"]
        assert "doc2" == res2["results"][1]["id"]
        assert "doc3" == res2["results"][0]["id"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_drop_index(decoded_r: redis.Redis):
    """
    Ensure the index gets dropped by data remains by default
    """
    for x in range(20):
        for keep_docs in [[True, {}], [False, {"name": "haveit"}]]:
            idx = "HaveIt"
            index = getClient(decoded_r)
            await index.hset("index:haveit", mapping={"name": "haveit"})
            idef = IndexDefinition(prefix=["index:"])
            await index.ft(idx).create_index((TextField("name"),), definition=idef)
            await waitForIndex(index, idx)
            await index.ft(idx).dropindex(delete_documents=keep_docs[0])
            i = await index.hgetall("index:haveit")
            assert i == keep_docs[1]


@pytest.mark.redismod
async def test_example(decoded_r: redis.Redis):
    # Creating the index definition and schema
    await decoded_r.ft().create_index(
        (TextField("title", weight=5.0), TextField("body"))
    )

    # Indexing a document
    await decoded_r.hset(
        "doc1",
        mapping={
            "title": "RediSearch",
            "body": "Redisearch impements a search engine on top of redis",
        },
    )

    # Searching with complex parameters:
    q = Query("search engine").verbatim().no_content().paging(0, 5)

    res = await decoded_r.ft().search(q)
    assert res is not None


@pytest.mark.redismod
async def test_auto_complete(decoded_r: redis.Redis):
    n = 0
    with open(TITLES_CSV) as f:
        cr = csv.reader(f)

        for row in cr:
            n += 1
            term, score = row[0], float(row[1])
            assert n == await decoded_r.ft().sugadd("ac", Suggestion(term, score=score))

    assert n == await decoded_r.ft().suglen("ac")
    ret = await decoded_r.ft().sugget("ac", "bad", with_scores=True)
    assert 2 == len(ret)
    assert "badger" == ret[0].string
    assert isinstance(ret[0].score, float)
    assert 1.0 != ret[0].score
    assert "badalte rishtey" == ret[1].string
    assert isinstance(ret[1].score, float)
    assert 1.0 != ret[1].score

    ret = await decoded_r.ft().sugget("ac", "bad", fuzzy=True, num=10)
    assert 10 == len(ret)
    assert 1.0 == ret[0].score
    strs = {x.string for x in ret}

    for sug in strs:
        assert 1 == await decoded_r.ft().sugdel("ac", sug)
    # make sure a second delete returns 0
    for sug in strs:
        assert 0 == await decoded_r.ft().sugdel("ac", sug)

    # make sure they were actually deleted
    ret2 = await decoded_r.ft().sugget("ac", "bad", fuzzy=True, num=10)
    for sug in ret2:
        assert sug.string not in strs

    # Test with payload
    await decoded_r.ft().sugadd("ac", Suggestion("pay1", payload="pl1"))
    await decoded_r.ft().sugadd("ac", Suggestion("pay2", payload="pl2"))
    await decoded_r.ft().sugadd("ac", Suggestion("pay3", payload="pl3"))

    sugs = await decoded_r.ft().sugget(
        "ac", "pay", with_payloads=True, with_scores=True
    )
    assert 3 == len(sugs)
    for sug in sugs:
        assert sug.payload
        assert sug.payload.startswith("pl")


@pytest.mark.redismod
async def test_no_index(decoded_r: redis.Redis):
    await decoded_r.ft().create_index(
        (
            TextField("field"),
            TextField("text", no_index=True, sortable=True),
            NumericField("numeric", no_index=True, sortable=True),
            GeoField("geo", no_index=True, sortable=True),
            TagField("tag", no_index=True, sortable=True),
        )
    )

    await decoded_r.hset(
        "doc1",
        mapping={"field": "aaa", "text": "1", "numeric": "1", "geo": "1,1", "tag": "1"},
    )
    await decoded_r.hset(
        "doc2",
        mapping={"field": "aab", "text": "2", "numeric": "2", "geo": "2,2", "tag": "2"},
    )
    await waitForIndex(decoded_r, "idx")

    if is_resp2_connection(decoded_r):
        res = await decoded_r.ft().search(Query("@text:aa*"))
        assert 0 == res.total

        res = await decoded_r.ft().search(Query("@field:aa*"))
        assert 2 == res.total

        res = await decoded_r.ft().search(Query("*").sort_by("text", asc=False))
        assert 2 == res.total
        assert "doc2" == res.docs[0].id

        res = await decoded_r.ft().search(Query("*").sort_by("text", asc=True))
        assert "doc1" == res.docs[0].id

        res = await decoded_r.ft().search(Query("*").sort_by("numeric", asc=True))
        assert "doc1" == res.docs[0].id

        res = await decoded_r.ft().search(Query("*").sort_by("geo", asc=True))
        assert "doc1" == res.docs[0].id

        res = await decoded_r.ft().search(Query("*").sort_by("tag", asc=True))
        assert "doc1" == res.docs[0].id
    else:
        res = await decoded_r.ft().search(Query("@text:aa*"))
        assert 0 == res["total_results"]

        res = await decoded_r.ft().search(Query("@field:aa*"))
        assert 2 == res["total_results"]

        res = await decoded_r.ft().search(Query("*").sort_by("text", asc=False))
        assert 2 == res["total_results"]
        assert "doc2" == res["results"][0]["id"]

        res = await decoded_r.ft().search(Query("*").sort_by("text", asc=True))
        assert "doc1" == res["results"][0]["id"]

        res = await decoded_r.ft().search(Query("*").sort_by("numeric", asc=True))
        assert "doc1" == res["results"][0]["id"]

        res = await decoded_r.ft().search(Query("*").sort_by("geo", asc=True))
        assert "doc1" == res["results"][0]["id"]

        res = await decoded_r.ft().search(Query("*").sort_by("tag", asc=True))
        assert "doc1" == res["results"][0]["id"]

    # Ensure exception is raised for non-indexable, non-sortable fields
    with pytest.raises(Exception):
        TextField("name", no_index=True, sortable=False)
    with pytest.raises(Exception):
        NumericField("name", no_index=True, sortable=False)
    with pytest.raises(Exception):
        GeoField("name", no_index=True, sortable=False)
    with pytest.raises(Exception):
        TagField("name", no_index=True, sortable=False)


@pytest.mark.redismod
async def test_explain(decoded_r: redis.Redis):
    await decoded_r.ft().create_index(
        (TextField("f1"), TextField("f2"), TextField("f3"))
    )
    res = await decoded_r.ft().explain("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert res


@pytest.mark.redismod
async def test_explaincli(decoded_r: redis.Redis):
    with pytest.raises(NotImplementedError):
        await decoded_r.ft().explain_cli("foo")


@pytest.mark.redismod
async def test_summarize(decoded_r: redis.Redis):
    await createIndex(decoded_r.ft())
    await waitForIndex(decoded_r, "idx")

    q = Query("king henry").paging(0, 1)
    q.highlight(fields=("play", "txt"), tags=("<b>", "</b>"))
    q.summarize("txt")

    if is_resp2_connection(decoded_r):
        doc = sorted((await decoded_r.ft().search(q)).docs)[0]
        assert "<b>Henry</b> IV" == doc.play
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc.txt
        )

        q = Query("king henry").paging(0, 1).summarize().highlight()

        doc = sorted((await decoded_r.ft().search(q)).docs)[0]
        assert "<b>Henry</b> ... " == doc.play
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc.txt
        )
    else:
        doc = sorted((await decoded_r.ft().search(q))["results"])[0]
        assert "<b>Henry</b> IV" == doc["extra_attributes"]["play"]
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc["extra_attributes"]["txt"]
        )

        q = Query("king henry").paging(0, 1).summarize().highlight()

        doc = sorted((await decoded_r.ft().search(q))["results"])[0]
        assert "<b>Henry</b> ... " == doc["extra_attributes"]["play"]
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc["extra_attributes"]["txt"]
        )


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
async def test_alias(decoded_r: redis.Redis):
    index1 = getClient(decoded_r)
    index2 = getClient(decoded_r)

    def1 = IndexDefinition(prefix=["index1:"])
    def2 = IndexDefinition(prefix=["index2:"])

    ftindex1 = index1.ft("testAlias")
    ftindex2 = index2.ft("testAlias2")
    await ftindex1.create_index((TextField("name"),), definition=def1)
    await ftindex2.create_index((TextField("name"),), definition=def2)

    await index1.hset("index1:lonestar", mapping={"name": "lonestar"})
    await index2.hset("index2:yogurt", mapping={"name": "yogurt"})

    if is_resp2_connection(decoded_r):
        res = (await ftindex1.search("*")).docs[0]
        assert "index1:lonestar" == res.id

        # create alias and check for results
        await ftindex1.aliasadd("spaceballs")
        alias_client = getClient(decoded_r).ft("spaceballs")
        res = (await alias_client.search("*")).docs[0]
        assert "index1:lonestar" == res.id

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            await ftindex2.aliasadd("spaceballs")

        # update alias and ensure new results
        await ftindex2.aliasupdate("spaceballs")
        alias_client2 = getClient(decoded_r).ft("spaceballs")

        res = (await alias_client2.search("*")).docs[0]
        assert "index2:yogurt" == res.id
    else:
        res = (await ftindex1.search("*"))["results"][0]
        assert "index1:lonestar" == res["id"]

        # create alias and check for results
        await ftindex1.aliasadd("spaceballs")
        alias_client = getClient(await decoded_r).ft("spaceballs")
        res = (await alias_client.search("*"))["results"][0]
        assert "index1:lonestar" == res["id"]

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            await ftindex2.aliasadd("spaceballs")

        # update alias and ensure new results
        await ftindex2.aliasupdate("spaceballs")
        alias_client2 = getClient(await decoded_r).ft("spaceballs")

        res = (await alias_client2.search("*"))["results"][0]
        assert "index2:yogurt" == res["id"]

    await ftindex2.aliasdel("spaceballs")
    with pytest.raises(Exception):
        (await alias_client2.search("*")).docs[0]


@pytest.mark.redismod
@pytest.mark.xfail(strict=False)
async def test_alias_basic(decoded_r: redis.Redis):
    # Creating a client with one index
    client = getClient(decoded_r)
    await client.flushdb()
    index1 = getClient(decoded_r).ft("testAlias")

    await index1.create_index((TextField("txt"),))
    await index1.client.hset("doc1", mapping={"txt": "text goes here"})

    index2 = getClient(decoded_r).ft("testAlias2")
    await index2.create_index((TextField("txt"),))
    await index2.client.hset("doc2", mapping={"txt": "text goes here"})

    # add the actual alias and check
    await index1.aliasadd("myalias")
    alias_client = getClient(decoded_r).ft("myalias")
    if is_resp2_connection(decoded_r):
        res = sorted((await alias_client.search("*")).docs, key=lambda x: x.id)
        assert "doc1" == res[0].id

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            await index2.aliasadd("myalias")

        # update the alias and ensure we get doc2
        await index2.aliasupdate("myalias")
        alias_client2 = getClient(decoded_r).ft("myalias")
        res = sorted((await alias_client2.search("*")).docs, key=lambda x: x.id)
        assert "doc1" == res[0].id
    else:
        res = sorted((await alias_client.search("*"))["results"], key=lambda x: x["id"])
        assert "doc1" == res[0]["id"]

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            await index2.aliasadd("myalias")

        # update the alias and ensure we get doc2
        await index2.aliasupdate("myalias")
        alias_client2 = getClient(client).ft("myalias")
        res = sorted(
            (await alias_client2.search("*"))["results"], key=lambda x: x["id"]
        )
        assert "doc1" == res[0]["id"]

    # delete the alias and expect an error if we try to query again
    await index2.aliasdel("myalias")
    with pytest.raises(Exception):
        _ = (await alias_client2.search("*")).docs[0]


@pytest.mark.redismod
async def test_tags(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("txt"), TagField("tags")))
    tags = "foo,foo bar,hello;world"
    tags2 = "soba,ramen"

    await decoded_r.hset("doc1", mapping={"txt": "fooz barz", "tags": tags})
    await decoded_r.hset("doc2", mapping={"txt": "noodles", "tags": tags2})
    await waitForIndex(decoded_r, "idx")

    q = Query("@tags:{foo}")
    if is_resp2_connection(decoded_r):
        res = await decoded_r.ft().search(q)
        assert 1 == res.total

        q = Query("@tags:{foo bar}")
        res = await decoded_r.ft().search(q)
        assert 1 == res.total

        q = Query("@tags:{foo\\ bar}")
        res = await decoded_r.ft().search(q)
        assert 1 == res.total

        q = Query("@tags:{hello\\;world}")
        res = await decoded_r.ft().search(q)
        assert 1 == res.total

        q2 = await decoded_r.ft().tagvals("tags")
        assert (tags.split(",") + tags2.split(",")).sort() == q2.sort()
    else:
        res = await decoded_r.ft().search(q)
        assert 1 == res["total_results"]

        q = Query("@tags:{foo bar}")
        res = await decoded_r.ft().search(q)
        assert 1 == res["total_results"]

        q = Query("@tags:{foo\\ bar}")
        res = await decoded_r.ft().search(q)
        assert 1 == res["total_results"]

        q = Query("@tags:{hello\\;world}")
        res = await decoded_r.ft().search(q)
        assert 1 == res["total_results"]

        q2 = await decoded_r.ft().tagvals("tags")
        assert set(tags.split(",") + tags2.split(",")) == set(q2)


@pytest.mark.redismod
async def test_textfield_sortable_nostem(decoded_r: redis.Redis):
    # Creating the index definition with sortable and no_stem
    await decoded_r.ft().create_index((TextField("txt", sortable=True, no_stem=True),))

    # Now get the index info to confirm its contents
    response = await decoded_r.ft().info()
    if is_resp2_connection(decoded_r):
        assert "SORTABLE" in response["attributes"][0]
        assert "NOSTEM" in response["attributes"][0]
    else:
        assert "SORTABLE" in response["attributes"][0]["flags"]
        assert "NOSTEM" in response["attributes"][0]["flags"]


@pytest.mark.redismod
async def test_alter_schema_add(decoded_r: redis.Redis):
    # Creating the index definition and schema
    await decoded_r.ft().create_index(TextField("title"))

    # Using alter to add a field
    await decoded_r.ft().alter_schema_add(TextField("body"))

    # Indexing a document
    await decoded_r.hset(
        "doc1", mapping={"title": "MyTitle", "body": "Some content only in the body"}
    )

    # Searching with parameter only in the body (the added field)
    q = Query("only in the body")

    # Ensure we find the result searching on the added body field
    res = await decoded_r.ft().search(q)
    if is_resp2_connection(decoded_r):
        assert 1 == res.total
    else:
        assert 1 == res["total_results"]


@pytest.mark.redismod
async def test_spell_check(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("f1"), TextField("f2")))

    await decoded_r.hset(
        "doc1", mapping={"f1": "some valid content", "f2": "this is sample text"}
    )
    await decoded_r.hset("doc2", mapping={"f1": "very important", "f2": "lorem ipsum"})
    await waitForIndex(decoded_r, "idx")

    if is_resp2_connection(decoded_r):
        # test spellcheck
        res = await decoded_r.ft().spellcheck("impornant")
        assert "important" == res["impornant"][0]["suggestion"]

        res = await decoded_r.ft().spellcheck("contnt")
        assert "content" == res["contnt"][0]["suggestion"]

        # test spellcheck with Levenshtein distance
        res = await decoded_r.ft().spellcheck("vlis")
        assert res == {}
        res = await decoded_r.ft().spellcheck("vlis", distance=2)
        assert "valid" == res["vlis"][0]["suggestion"]

        # test spellcheck include
        await decoded_r.ft().dict_add("dict", "lore", "lorem", "lorm")
        res = await decoded_r.ft().spellcheck("lorm", include="dict")
        assert len(res["lorm"]) == 3
        assert (
            res["lorm"][0]["suggestion"],
            res["lorm"][1]["suggestion"],
            res["lorm"][2]["suggestion"],
        ) == ("lorem", "lore", "lorm")
        assert (res["lorm"][0]["score"], res["lorm"][1]["score"]) == ("0.5", "0")

        # test spellcheck exclude
        res = await decoded_r.ft().spellcheck("lorm", exclude="dict")
        assert res == {}
    else:
        # test spellcheck
        res = await decoded_r.ft().spellcheck("impornant")
        assert "important" in res["results"]["impornant"][0].keys()

        res = await decoded_r.ft().spellcheck("contnt")
        assert "content" in res["results"]["contnt"][0].keys()

        # test spellcheck with Levenshtein distance
        res = await decoded_r.ft().spellcheck("vlis")
        assert res == {"results": {"vlis": []}}
        res = await decoded_r.ft().spellcheck("vlis", distance=2)
        assert "valid" in res["results"]["vlis"][0].keys()

        # test spellcheck include
        await decoded_r.ft().dict_add("dict", "lore", "lorem", "lorm")
        res = await decoded_r.ft().spellcheck("lorm", include="dict")
        assert len(res["results"]["lorm"]) == 3
        assert "lorem" in res["results"]["lorm"][0].keys()
        assert "lore" in res["results"]["lorm"][1].keys()
        assert "lorm" in res["results"]["lorm"][2].keys()
        assert (
            res["results"]["lorm"][0]["lorem"],
            res["results"]["lorm"][1]["lore"],
        ) == (0.5, 0)

        # test spellcheck exclude
        res = await decoded_r.ft().spellcheck("lorm", exclude="dict")
        assert res == {"results": {}}


@pytest.mark.redismod
async def test_dict_operations(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("f1"), TextField("f2")))
    # Add three items
    res = await decoded_r.ft().dict_add("custom_dict", "item1", "item2", "item3")
    assert 3 == res

    # Remove one item
    res = await decoded_r.ft().dict_del("custom_dict", "item2")
    assert 1 == res

    # Dump dict and inspect content
    res = await decoded_r.ft().dict_dump("custom_dict")
    assert res == ["item1", "item3"]

    # Remove rest of the items before reload
    await decoded_r.ft().dict_del("custom_dict", *res)


@pytest.mark.redismod
async def test_phonetic_matcher(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("name"),))
    await decoded_r.hset("doc1", mapping={"name": "Jon"})
    await decoded_r.hset("doc2", mapping={"name": "John"})

    res = await decoded_r.ft().search(Query("Jon"))
    if is_resp2_connection(decoded_r):
        assert 1 == len(res.docs)
        assert "Jon" == res.docs[0].name
    else:
        assert 1 == res["total_results"]
        assert "Jon" == res["results"][0]["extra_attributes"]["name"]

    # Drop and create index with phonetic matcher
    await decoded_r.flushdb()

    await decoded_r.ft().create_index((TextField("name", phonetic_matcher="dm:en"),))
    await decoded_r.hset("doc1", mapping={"name": "Jon"})
    await decoded_r.hset("doc2", mapping={"name": "John"})

    res = await decoded_r.ft().search(Query("Jon"))
    if is_resp2_connection(decoded_r):
        assert 2 == len(res.docs)
        assert ["John", "Jon"] == sorted(d.name for d in res.docs)
    else:
        assert 2 == res["total_results"]
        assert ["John", "Jon"] == sorted(
            d["extra_attributes"]["name"] for d in res["results"]
        )


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_scorer(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("description"),))

    await decoded_r.hset(
        "doc1", mapping={"description": "The quick brown fox jumps over the lazy dog"}
    )
    await decoded_r.hset(
        "doc2",
        mapping={
            "description": "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do."  # noqa
        },
    )

    if is_resp2_connection(decoded_r):
        # default scorer is TFIDF
        res = await decoded_r.ft().search(Query("quick").with_scores())
        assert 1.0 == res.docs[0].score
        res = await decoded_r.ft().search(Query("quick").scorer("TFIDF").with_scores())
        assert 1.0 == res.docs[0].score
        res = await decoded_r.ft().search(
            Query("quick").scorer("TFIDF.DOCNORM").with_scores()
        )
        assert 0.14285714285714285 == res.docs[0].score
        res = await decoded_r.ft().search(Query("quick").scorer("BM25").with_scores())
        assert 0.22471909420069797 == res.docs[0].score
        res = await decoded_r.ft().search(Query("quick").scorer("DISMAX").with_scores())
        assert 2.0 == res.docs[0].score
        res = await decoded_r.ft().search(
            Query("quick").scorer("DOCSCORE").with_scores()
        )
        assert 1.0 == res.docs[0].score
        res = await decoded_r.ft().search(
            Query("quick").scorer("HAMMING").with_scores()
        )
        assert 0.0 == res.docs[0].score
    else:
        res = await decoded_r.ft().search(Query("quick").with_scores())
        assert 1.0 == res["results"][0]["score"]
        res = await decoded_r.ft().search(Query("quick").scorer("TFIDF").with_scores())
        assert 1.0 == res["results"][0]["score"]
        res = await decoded_r.ft().search(
            Query("quick").scorer("TFIDF.DOCNORM").with_scores()
        )
        assert 0.14285714285714285 == res["results"][0]["score"]
        res = await decoded_r.ft().search(Query("quick").scorer("BM25").with_scores())
        assert 0.22471909420069797 == res["results"][0]["score"]
        res = await decoded_r.ft().search(Query("quick").scorer("DISMAX").with_scores())
        assert 2.0 == res["results"][0]["score"]
        res = await decoded_r.ft().search(
            Query("quick").scorer("DOCSCORE").with_scores()
        )
        assert 1.0 == res["results"][0]["score"]
        res = await decoded_r.ft().search(
            Query("quick").scorer("HAMMING").with_scores()
        )
        assert 0.0 == res["results"][0]["score"]


@pytest.mark.redismod
async def test_get(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("f1"), TextField("f2")))

    assert [None] == await decoded_r.ft().get("doc1")
    assert [None, None] == await decoded_r.ft().get("doc2", "doc1")

    await decoded_r.hset(
        "doc1", mapping={"f1": "some valid content dd1", "f2": "this is sample text f1"}
    )
    await decoded_r.hset(
        "doc2", mapping={"f1": "some valid content dd2", "f2": "this is sample text f2"}
    )

    assert [
        ["f1", "some valid content dd2", "f2", "this is sample text f2"]
    ] == await decoded_r.ft().get("doc2")
    assert [
        ["f1", "some valid content dd1", "f2", "this is sample text f1"],
        ["f1", "some valid content dd2", "f2", "this is sample text f2"],
    ] == await decoded_r.ft().get("doc1", "doc2")


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("2.2.0", "search")
async def test_config(decoded_r: redis.Redis):
    assert await decoded_r.ft().config_set("TIMEOUT", "100")
    with pytest.raises(redis.ResponseError):
        await decoded_r.ft().config_set("TIMEOUT", "null")
    res = await decoded_r.ft().config_get("*")
    assert "100" == res["TIMEOUT"]
    res = await decoded_r.ft().config_get("TIMEOUT")
    assert "100" == res["TIMEOUT"]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_aggregations_groupby(decoded_r: redis.Redis):
    # Creating the index definition and schema
    await decoded_r.ft().create_index(
        (
            NumericField("random_num"),
            TextField("title"),
            TextField("body"),
            TextField("parent"),
        )
    )

    # Indexing a document
    await decoded_r.hset(
        "search",
        mapping={
            "title": "RediSearch",
            "body": "Redisearch impements a search engine on top of redis",
            "parent": "redis",
            "random_num": 10,
        },
    )
    await decoded_r.hset(
        "ai",
        mapping={
            "title": "RedisAI",
            "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
            "parent": "redis",
            "random_num": 3,
        },
    )
    await decoded_r.hset(
        "json",
        mapping={
            "title": "RedisJson",
            "body": "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
            "parent": "redis",
            "random_num": 8,
        },
    )

    for dialect in [1, 2]:
        if is_resp2_connection(decoded_r):
            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.count())
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.count_distinct("@title"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.count_distinctish("@title"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.sum("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "21"  # 10+8+3

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.min("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3"  # min(10,8,3)

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.max("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "10"  # max(10,8,3)

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.avg("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "7"  # (10+3+8)/3

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.stddev("random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "3.60555127546"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.quantile("@random_num", 0.5))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[3] == "8"  # median of 3,8,10

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.tolist("@title"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert set(res[3]) == {"RediSearch", "RedisAI", "RedisJson"}

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.first_value("@title").alias("first"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res == ["parent", "redis", "first", "RediSearch"]

            req = (
                aggregations.AggregateRequest("redis")
                .group_by(
                    "@parent", reducers.random_sample("@title", 2).alias("random")
                )
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req)).rows[0]
            assert res[1] == "redis"
            assert res[2] == "random"
            assert len(res[3]) == 2
            assert res[3][0] in ["RediSearch", "RedisAI", "RedisJson"]
        else:
            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.count())
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliascount"] == "3"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.count_distinct("@title"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliascount_distincttitle"] == "3"
            )

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.count_distinctish("@title"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliascount_distinctishtitle"]
                == "3"
            )

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.sum("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliassumrandom_num"] == "21"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.min("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliasminrandom_num"] == "3"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.max("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliasmaxrandom_num"] == "10"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.avg("@random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert res["extra_attributes"]["__generated_aliasavgrandom_num"] == "7"

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.stddev("random_num"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliasstddevrandom_num"]
                == "3.60555127546"
            )

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.quantile("@random_num", 0.5))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert (
                res["extra_attributes"]["__generated_aliasquantilerandom_num,0.5"]
                == "8"
            )

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.tolist("@title"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert set(res["extra_attributes"]["__generated_aliastolisttitle"]) == {
                "RediSearch",
                "RedisAI",
                "RedisJson",
            }

            req = (
                aggregations.AggregateRequest("redis")
                .group_by("@parent", reducers.first_value("@title").alias("first"))
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"] == {"parent": "redis", "first": "RediSearch"}

            req = (
                aggregations.AggregateRequest("redis")
                .group_by(
                    "@parent", reducers.random_sample("@title", 2).alias("random")
                )
                .dialect(dialect)
            )

            res = (await decoded_r.ft().aggregate(req))["results"][0]
            assert res["extra_attributes"]["parent"] == "redis"
            assert "random" in res["extra_attributes"].keys()
            assert len(res["extra_attributes"]["random"]) == 2
            assert res["extra_attributes"]["random"][0] in [
                "RediSearch",
                "RedisAI",
                "RedisJson",
            ]


@pytest.mark.redismod
async def test_aggregations_sort_by_and_limit(decoded_r: redis.Redis):
    await decoded_r.ft().create_index((TextField("t1"), TextField("t2")))

    await decoded_r.ft().client.hset("doc1", mapping={"t1": "a", "t2": "b"})
    await decoded_r.ft().client.hset("doc2", mapping={"t1": "b", "t2": "a"})

    if is_resp2_connection(decoded_r):
        # test sort_by using SortDirection
        req = aggregations.AggregateRequest("*").sort_by(
            aggregations.Asc("@t2"), aggregations.Desc("@t1")
        )
        res = await decoded_r.ft().aggregate(req)
        assert res.rows[0] == ["t2", "a", "t1", "b"]
        assert res.rows[1] == ["t2", "b", "t1", "a"]

        # test sort_by without SortDirection
        req = aggregations.AggregateRequest("*").sort_by("@t1")
        res = await decoded_r.ft().aggregate(req)
        assert res.rows[0] == ["t1", "a"]
        assert res.rows[1] == ["t1", "b"]

        # test sort_by with max
        req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
        res = await decoded_r.ft().aggregate(req)
        assert len(res.rows) == 1

        # test limit
        req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
        res = await decoded_r.ft().aggregate(req)
        assert len(res.rows) == 1
        assert res.rows[0] == ["t1", "b"]
    else:
        # test sort_by using SortDirection
        req = aggregations.AggregateRequest("*").sort_by(
            aggregations.Asc("@t2"), aggregations.Desc("@t1")
        )
        res = (await decoded_r.ft().aggregate(req))["results"]
        assert res[0]["extra_attributes"] == {"t2": "a", "t1": "b"}
        assert res[1]["extra_attributes"] == {"t2": "b", "t1": "a"}

        # test sort_by without SortDirection
        req = aggregations.AggregateRequest("*").sort_by("@t1")
        res = (await decoded_r.ft().aggregate(req))["results"]
        assert res[0]["extra_attributes"] == {"t1": "a"}
        assert res[1]["extra_attributes"] == {"t1": "b"}

        # test sort_by with max
        req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
        res = await decoded_r.ft().aggregate(req)
        assert len(res["results"]) == 1

        # test limit
        req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
        res = await decoded_r.ft().aggregate(req)
        assert len(res["results"]) == 1
        assert res["results"][0]["extra_attributes"] == {"t1": "b"}


@pytest.mark.redismod
@pytest.mark.experimental
async def test_withsuffixtrie(decoded_r: redis.Redis):
    # create index
    assert await decoded_r.ft().create_index((TextField("txt"),))
    await waitForIndex(decoded_r, getattr(decoded_r.ft(), "index_name", "idx"))
    if is_resp2_connection(decoded_r):
        info = await decoded_r.ft().info()
        assert "WITHSUFFIXTRIE" not in info["attributes"][0]
        assert await decoded_r.ft().dropindex("idx")

        # create withsuffixtrie index (text field)
        assert await decoded_r.ft().create_index(TextField("t", withsuffixtrie=True))
        await waitForIndex(decoded_r, getattr(decoded_r.ft(), "index_name", "idx"))
        info = await decoded_r.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]
        assert await decoded_r.ft().dropindex("idx")

        # create withsuffixtrie index (tag field)
        assert await decoded_r.ft().create_index(TagField("t", withsuffixtrie=True))
        await waitForIndex(decoded_r, getattr(decoded_r.ft(), "index_name", "idx"))
        info = await decoded_r.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]
    else:
        info = await decoded_r.ft().info()
        assert "WITHSUFFIXTRIE" not in info["attributes"][0]["flags"]
        assert await decoded_r.ft().dropindex("idx")

        # create withsuffixtrie index (text fields)
        assert await decoded_r.ft().create_index(TextField("t", withsuffixtrie=True))
        await waitForIndex(decoded_r, getattr(decoded_r.ft(), "index_name", "idx"))
        info = await decoded_r.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]
        assert await decoded_r.ft().dropindex("idx")

        # create withsuffixtrie index (tag field)
        assert await decoded_r.ft().create_index(TagField("t", withsuffixtrie=True))
        await waitForIndex(decoded_r, getattr(decoded_r.ft(), "index_name", "idx"))
        info = await decoded_r.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.10.05", "search")
async def test_aggregations_add_scores(decoded_r: redis.Redis):
    assert await decoded_r.ft().create_index(
        (
            TextField("name", sortable=True, weight=5.0),
            NumericField("age", sortable=True),
        )
    )

    assert await decoded_r.hset("doc1", mapping={"name": "bar", "age": "25"})
    assert await decoded_r.hset("doc2", mapping={"name": "foo", "age": "19"})

    req = aggregations.AggregateRequest("*").add_scores()
    res = await decoded_r.ft().aggregate(req)

    if isinstance(res, dict):
        assert len(res["results"]) == 2
        assert res["results"][0]["extra_attributes"] == {"__score": "0.2"}
        assert res["results"][1]["extra_attributes"] == {"__score": "0.2"}
    else:
        assert len(res.rows) == 2
        assert res.rows[0] == ["__score", "0.2"]
        assert res.rows[1] == ["__score", "0.2"]


@pytest.mark.redismod
@skip_if_redis_enterprise()
async def test_search_commands_in_pipeline(decoded_r: redis.Redis):
    p = await decoded_r.ft().pipeline()
    p.create_index((TextField("txt"),))
    p.hset("doc1", mapping={"txt": "foo bar"})
    p.hset("doc2", mapping={"txt": "foo bar"})
    q = Query("foo bar").with_payloads()
    await p.search(q)
    res = await p.execute()
    if is_resp2_connection(decoded_r):
        assert res[:3] == ["OK", True, True]
        assert 2 == res[3][0]
        assert "doc1" == res[3][1]
        assert "doc2" == res[3][4]
        assert res[3][5] is None
        assert res[3][3] == res[3][6] == ["txt", "foo bar"]
    else:
        assert res[:3] == ["OK", True, True]
        assert 2 == res[3]["total_results"]
        assert "doc1" == res[3]["results"][0]["id"]
        assert "doc2" == res[3]["results"][1]["id"]
        assert res[3]["results"][0]["payload"] is None
        assert (
            res[3]["results"][0]["extra_attributes"]
            == res[3]["results"][1]["extra_attributes"]
            == {"txt": "foo bar"}
        )


@pytest.mark.redismod
async def test_query_timeout(decoded_r: redis.Redis):
    q1 = Query("foo").timeout(5000)
    assert q1.get_args() == ["foo", "TIMEOUT", 5000, "LIMIT", 0, 10]
    q2 = Query("foo").timeout("not_a_number")
    with pytest.raises(redis.ResponseError):
        await decoded_r.ft().search(q2)


@pytest.mark.redismod
@skip_if_resp_version(3)
async def test_binary_and_text_fields(decoded_r: redis.Redis):
    fake_vec = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)

    index_name = "mixed_index"
    mixed_data = {"first_name": "🐍python", "vector_emb": fake_vec.tobytes()}
    await decoded_r.hset(f"{index_name}:1", mapping=mixed_data)

    schema = (
        TagField("first_name"),
        VectorField(
            "embeddings_bio",
            algorithm="HNSW",
            attributes={
                "TYPE": "FLOAT32",
                "DIM": 4,
                "DISTANCE_METRIC": "COSINE",
            },
        ),
    )

    await decoded_r.ft(index_name).create_index(
        fields=schema,
        definition=IndexDefinition(
            prefix=[f"{index_name}:"], index_type=IndexType.HASH
        ),
    )

    query = (
        Query("*")
        .return_field("vector_emb", decode_field=False)
        .return_field("first_name")
    )
    result = await decoded_r.ft(index_name).search(query=query, query_params={})
    docs = result.docs

    decoded_vec_from_search_results = np.frombuffer(
        docs[0]["vector_emb"], dtype=np.float32
    )

    assert np.array_equal(
        decoded_vec_from_search_results, fake_vec
    ), "The vectors are not equal"

    assert (
        docs[0]["first_name"] == mixed_data["first_name"]
    ), "The text field is not decoded correctly"
