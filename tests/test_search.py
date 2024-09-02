import bz2
import csv
import os
import time
from io import TextIOWrapper

import numpy as np
import pytest
import redis
import redis.commands.search
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.json.path import Path
from redis.commands.search import Search
from redis.commands.search.field import (
    GeoField,
    GeoShapeField,
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import GeoFilter, NumericFilter, Query
from redis.commands.search.result import Result
from redis.commands.search.suggestion import Suggestion

from .conftest import (
    _get_client,
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


def waitForIndex(env, idx, timeout=None):
    delay = 0.1
    while True:
        res = env.execute_command("FT.INFO", idx)
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


def getClient(client):
    """
    Gets a client client attached to an index name which is ready to be
    created
    """
    return client


def createIndex(client, num_docs=100, definition=None):
    try:
        client.create_index(
            (TextField("play", weight=5.0), TextField("txt"), NumericField("chapter")),
            definition=definition,
        )
    except redis.ResponseError:
        client.dropindex(delete_documents=True)
        return createIndex(client, num_docs=num_docs, definition=definition)

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

    indexer = client.batch_indexer(chunk_size=50)
    assert isinstance(indexer, Search.BatchIndexer)
    assert 50 == indexer.chunk_size

    for key, doc in chapters.items():
        indexer.client.client.hset(key, mapping=doc)
    indexer.commit()


@pytest.fixture
def client(request, stack_url):
    r = _get_client(redis.Redis, request, decode_responses=True, from_url=stack_url)
    r.flushdb()
    return r


@pytest.mark.redismod
def test_client(client):
    num_docs = 500
    createIndex(client.ft(), num_docs=num_docs)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
    # verify info
    info = client.ft().info()
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

    assert client.ft().index_name == info["index_name"]
    assert num_docs == int(info["num_docs"])

    res = client.ft().search("henry iv")
    if is_resp2_connection(client):
        assert isinstance(res, Result)
        assert 225 == res.total
        assert 10 == len(res.docs)
        assert res.duration > 0

        for doc in res.docs:
            assert doc.id
            assert doc["id"]
            assert doc.play == "Henry IV"
            assert doc["play"] == "Henry IV"
            assert len(doc.txt) > 0

        # test no content
        res = client.ft().search(Query("king").no_content())
        assert 194 == res.total
        assert 10 == len(res.docs)
        for doc in res.docs:
            assert "txt" not in doc.__dict__
            assert "play" not in doc.__dict__

        # test verbatim vs no verbatim
        total = client.ft().search(Query("kings").no_content()).total
        vtotal = client.ft().search(Query("kings").no_content().verbatim()).total
        assert total > vtotal

        # test in fields
        txt_total = (
            client.ft().search(Query("henry").no_content().limit_fields("txt")).total
        )
        play_total = (
            client.ft().search(Query("henry").no_content().limit_fields("play")).total
        )
        both_total = (
            client.ft()
            .search(Query("henry").no_content().limit_fields("play", "txt"))
            .total
        )
        assert 129 == txt_total
        assert 494 == play_total
        assert 494 == both_total

        # test load_document
        doc = client.ft().load_document("henry vi part 3:62")
        assert doc is not None
        assert "henry vi part 3:62" == doc.id
        assert doc.play == "Henry VI Part 3"
        assert len(doc.txt) > 0

        # test in-keys
        ids = [x.id for x in client.ft().search(Query("henry")).docs]
        assert 10 == len(ids)
        subset = ids[:5]
        docs = client.ft().search(Query("henry").limit_ids(*subset))
        assert len(subset) == docs.total
        ids = [x.id for x in docs.docs]
        assert set(ids) == set(subset)

        # test slop and in order
        assert 193 == client.ft().search(Query("henry king")).total
        assert 3 == client.ft().search(Query("henry king").slop(0).in_order()).total
        assert 52 == client.ft().search(Query("king henry").slop(0).in_order()).total
        assert 53 == client.ft().search(Query("henry king").slop(0)).total
        assert 167 == client.ft().search(Query("henry king").slop(100)).total

        # test delete document
        client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = client.ft().search(Query("death of a salesman"))
        assert 1 == res.total

        assert 1 == client.ft().delete_document("doc-5ghs2")
        res = client.ft().search(Query("death of a salesman"))
        assert 0 == res.total
        assert 0 == client.ft().delete_document("doc-5ghs2")

        client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = client.ft().search(Query("death of a salesman"))
        assert 1 == res.total
        client.ft().delete_document("doc-5ghs2")
    else:
        assert isinstance(res, dict)
        assert 225 == res["total_results"]
        assert 10 == len(res["results"])

        for doc in res["results"]:
            assert doc["id"]
            assert doc["extra_attributes"]["play"] == "Henry IV"
            assert len(doc["extra_attributes"]["txt"]) > 0

        # test no content
        res = client.ft().search(Query("king").no_content())
        assert 194 == res["total_results"]
        assert 10 == len(res["results"])
        for doc in res["results"]:
            assert "extra_attributes" not in doc.keys()

        # test verbatim vs no verbatim
        total = client.ft().search(Query("kings").no_content())["total_results"]
        vtotal = client.ft().search(Query("kings").no_content().verbatim())[
            "total_results"
        ]
        assert total > vtotal

        # test in fields
        txt_total = client.ft().search(Query("henry").no_content().limit_fields("txt"))[
            "total_results"
        ]
        play_total = client.ft().search(
            Query("henry").no_content().limit_fields("play")
        )["total_results"]
        both_total = client.ft().search(
            Query("henry").no_content().limit_fields("play", "txt")
        )["total_results"]
        assert 129 == txt_total
        assert 494 == play_total
        assert 494 == both_total

        # test load_document
        doc = client.ft().load_document("henry vi part 3:62")
        assert doc is not None
        assert "henry vi part 3:62" == doc.id
        assert doc.play == "Henry VI Part 3"
        assert len(doc.txt) > 0

        # test in-keys
        ids = [x["id"] for x in client.ft().search(Query("henry"))["results"]]
        assert 10 == len(ids)
        subset = ids[:5]
        docs = client.ft().search(Query("henry").limit_ids(*subset))
        assert len(subset) == docs["total_results"]
        ids = [x["id"] for x in docs["results"]]
        assert set(ids) == set(subset)

        # test slop and in order
        assert 193 == client.ft().search(Query("henry king"))["total_results"]
        assert (
            3
            == client.ft().search(Query("henry king").slop(0).in_order())[
                "total_results"
            ]
        )
        assert (
            52
            == client.ft().search(Query("king henry").slop(0).in_order())[
                "total_results"
            ]
        )
        assert 53 == client.ft().search(Query("henry king").slop(0))["total_results"]
        assert 167 == client.ft().search(Query("henry king").slop(100))["total_results"]

        # test delete document
        client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = client.ft().search(Query("death of a salesman"))
        assert 1 == res["total_results"]

        assert 1 == client.ft().delete_document("doc-5ghs2")
        res = client.ft().search(Query("death of a salesman"))
        assert 0 == res["total_results"]
        assert 0 == client.ft().delete_document("doc-5ghs2")

        client.hset("doc-5ghs2", mapping={"play": "Death of a Salesman"})
        res = client.ft().search(Query("death of a salesman"))
        assert 1 == res["total_results"]
        client.ft().delete_document("doc-5ghs2")


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_scores(client):
    client.ft().create_index((TextField("txt"),))

    client.hset("doc1", mapping={"txt": "foo baz"})
    client.hset("doc2", mapping={"txt": "foo bar"})

    q = Query("foo ~bar").with_scores()
    res = client.ft().search(q)
    if is_resp2_connection(client):
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
def test_stopwords(client):
    client.ft().create_index((TextField("txt"),), stopwords=["foo", "bar", "baz"])
    client.hset("doc1", mapping={"txt": "foo bar"})
    client.hset("doc2", mapping={"txt": "hello world"})
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    q1 = Query("foo bar").no_content()
    q2 = Query("foo bar hello world").no_content()
    res1, res2 = client.ft().search(q1), client.ft().search(q2)
    if is_resp2_connection(client):
        assert 0 == res1.total
        assert 1 == res2.total
    else:
        assert 0 == res1["total_results"]
        assert 1 == res2["total_results"]


@pytest.mark.redismod
def test_filters(client):
    client.ft().create_index((TextField("txt"), NumericField("num"), GeoField("loc")))
    client.hset(
        "doc1", mapping={"txt": "foo bar", "num": 3.141, "loc": "-0.441,51.458"}
    )
    client.hset("doc2", mapping={"txt": "foo baz", "num": 2, "loc": "-0.1,51.2"})

    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
    # Test numerical filter
    q1 = Query("foo").add_filter(NumericFilter("num", 0, 2)).no_content()
    q2 = (
        Query("foo")
        .add_filter(NumericFilter("num", 2, NumericFilter.INF, minExclusive=True))
        .no_content()
    )
    res1, res2 = client.ft().search(q1), client.ft().search(q2)
    if is_resp2_connection(client):
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
    res1, res2 = client.ft().search(q1), client.ft().search(q2)

    if is_resp2_connection(client):
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
def test_sort_by(client):
    client.ft().create_index((TextField("txt"), NumericField("num", sortable=True)))
    client.hset("doc1", mapping={"txt": "foo bar", "num": 1})
    client.hset("doc2", mapping={"txt": "foo baz", "num": 2})
    client.hset("doc3", mapping={"txt": "foo qux", "num": 3})

    # Test sort
    q1 = Query("foo").sort_by("num", asc=True).no_content()
    q2 = Query("foo").sort_by("num", asc=False).no_content()
    res1, res2 = client.ft().search(q1), client.ft().search(q2)

    if is_resp2_connection(client):
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
def test_drop_index(client):
    """
    Ensure the index gets dropped by data remains by default
    """
    for x in range(20):
        for keep_docs in [[True, {}], [False, {"name": "haveit"}]]:
            idx = "HaveIt"
            index = getClient(client)
            index.hset("index:haveit", mapping={"name": "haveit"})
            idef = IndexDefinition(prefix=["index:"])
            index.ft(idx).create_index((TextField("name"),), definition=idef)
            waitForIndex(index, idx)
            index.ft(idx).dropindex(delete_documents=keep_docs[0])
            i = index.hgetall("index:haveit")
            assert i == keep_docs[1]


@pytest.mark.redismod
def test_example(client):
    # Creating the index definition and schema
    client.ft().create_index((TextField("title", weight=5.0), TextField("body")))

    # Indexing a document
    client.hset(
        "doc1",
        mapping={
            "title": "RediSearch",
            "body": "Redisearch impements a search engine on top of redis",
        },
    )

    # Searching with complex parameters:
    q = Query("search engine").verbatim().no_content().paging(0, 5)

    res = client.ft().search(q)
    assert res is not None


@pytest.mark.redismod
@skip_if_redis_enterprise()
def test_auto_complete(client):
    n = 0
    with open(TITLES_CSV) as f:
        cr = csv.reader(f)

        for row in cr:
            n += 1
            term, score = row[0], float(row[1])
            assert n == client.ft().sugadd("ac", Suggestion(term, score=score))

    assert n == client.ft().suglen("ac")
    ret = client.ft().sugget("ac", "bad", with_scores=True)
    assert 2 == len(ret)
    assert "badger" == ret[0].string
    assert isinstance(ret[0].score, float)
    assert 1.0 != ret[0].score
    assert "badalte rishtey" == ret[1].string
    assert isinstance(ret[1].score, float)
    assert 1.0 != ret[1].score

    ret = client.ft().sugget("ac", "bad", fuzzy=True, num=10)
    assert 10 == len(ret)
    assert 1.0 == ret[0].score
    strs = {x.string for x in ret}

    for sug in strs:
        assert 1 == client.ft().sugdel("ac", sug)
    # make sure a second delete returns 0
    for sug in strs:
        assert 0 == client.ft().sugdel("ac", sug)

    # make sure they were actually deleted
    ret2 = client.ft().sugget("ac", "bad", fuzzy=True, num=10)
    for sug in ret2:
        assert sug.string not in strs

    # Test with payload
    client.ft().sugadd("ac", Suggestion("pay1", payload="pl1"))
    client.ft().sugadd("ac", Suggestion("pay2", payload="pl2"))
    client.ft().sugadd("ac", Suggestion("pay3", payload="pl3"))

    sugs = client.ft().sugget("ac", "pay", with_payloads=True, with_scores=True)
    assert 3 == len(sugs)
    for sug in sugs:
        assert sug.payload
        assert sug.payload.startswith("pl")


@pytest.mark.redismod
def test_no_index(client):
    client.ft().create_index(
        (
            TextField("field"),
            TextField("text", no_index=True, sortable=True),
            NumericField("numeric", no_index=True, sortable=True),
            GeoField("geo", no_index=True, sortable=True),
            TagField("tag", no_index=True, sortable=True),
        )
    )

    client.hset(
        "doc1",
        mapping={"field": "aaa", "text": "1", "numeric": "1", "geo": "1,1", "tag": "1"},
    )
    client.hset(
        "doc2",
        mapping={"field": "aab", "text": "2", "numeric": "2", "geo": "2,2", "tag": "2"},
    )
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    if is_resp2_connection(client):
        res = client.ft().search(Query("@text:aa*"))
        assert 0 == res.total

        res = client.ft().search(Query("@field:aa*"))
        assert 2 == res.total

        res = client.ft().search(Query("*").sort_by("text", asc=False))
        assert 2 == res.total
        assert "doc2" == res.docs[0].id

        res = client.ft().search(Query("*").sort_by("text", asc=True))
        assert "doc1" == res.docs[0].id

        res = client.ft().search(Query("*").sort_by("numeric", asc=True))
        assert "doc1" == res.docs[0].id

        res = client.ft().search(Query("*").sort_by("geo", asc=True))
        assert "doc1" == res.docs[0].id

        res = client.ft().search(Query("*").sort_by("tag", asc=True))
        assert "doc1" == res.docs[0].id
    else:
        res = client.ft().search(Query("@text:aa*"))
        assert 0 == res["total_results"]

        res = client.ft().search(Query("@field:aa*"))
        assert 2 == res["total_results"]

        res = client.ft().search(Query("*").sort_by("text", asc=False))
        assert 2 == res["total_results"]
        assert "doc2" == res["results"][0]["id"]

        res = client.ft().search(Query("*").sort_by("text", asc=True))
        assert "doc1" == res["results"][0]["id"]

        res = client.ft().search(Query("*").sort_by("numeric", asc=True))
        assert "doc1" == res["results"][0]["id"]

        res = client.ft().search(Query("*").sort_by("geo", asc=True))
        assert "doc1" == res["results"][0]["id"]

        res = client.ft().search(Query("*").sort_by("tag", asc=True))
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
def test_explain(client):
    client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
    res = client.ft().explain("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert res


@pytest.mark.redismod
def test_explaincli(client):
    with pytest.raises(NotImplementedError):
        client.ft().explain_cli("foo")


@pytest.mark.redismod
def test_summarize(client):
    createIndex(client.ft())
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    q = Query("king henry").paging(0, 1)
    q.highlight(fields=("play", "txt"), tags=("<b>", "</b>"))
    q.summarize("txt")

    if is_resp2_connection(client):
        doc = sorted(client.ft().search(q).docs)[0]
        assert "<b>Henry</b> IV" == doc.play
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc.txt
        )

        q = Query("king henry").paging(0, 1).summarize().highlight()

        doc = sorted(client.ft().search(q).docs)[0]
        assert "<b>Henry</b> ... " == doc.play
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc.txt
        )
    else:
        doc = sorted(client.ft().search(q)["results"])[0]
        assert "<b>Henry</b> IV" == doc["extra_attributes"]["play"]
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc["extra_attributes"]["txt"]
        )

        q = Query("king henry").paging(0, 1).summarize().highlight()

        doc = sorted(client.ft().search(q)["results"])[0]
        assert "<b>Henry</b> ... " == doc["extra_attributes"]["play"]
        assert (
            "ACT I SCENE I. London. The palace. Enter <b>KING</b> <b>HENRY</b>, LORD JOHN OF LANCASTER, the EARL of WESTMORELAND, SIR... "  # noqa
            == doc["extra_attributes"]["txt"]
        )


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
def test_alias(client):
    index1 = getClient(client)
    index2 = getClient(client)

    def1 = IndexDefinition(prefix=["index1:"])
    def2 = IndexDefinition(prefix=["index2:"])

    ftindex1 = index1.ft("testAlias")
    ftindex2 = index2.ft("testAlias2")
    ftindex1.create_index((TextField("name"),), definition=def1)
    ftindex2.create_index((TextField("name"),), definition=def2)

    index1.hset("index1:lonestar", mapping={"name": "lonestar"})
    index2.hset("index2:yogurt", mapping={"name": "yogurt"})

    if is_resp2_connection(client):
        res = ftindex1.search("*").docs[0]
        assert "index1:lonestar" == res.id

        # create alias and check for results
        ftindex1.aliasadd("spaceballs")
        alias_client = getClient(client).ft("spaceballs")
        res = alias_client.search("*").docs[0]
        assert "index1:lonestar" == res.id

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            ftindex2.aliasadd("spaceballs")

        # update alias and ensure new results
        ftindex2.aliasupdate("spaceballs")
        alias_client2 = getClient(client).ft("spaceballs")

        res = alias_client2.search("*").docs[0]
        assert "index2:yogurt" == res.id
    else:
        res = ftindex1.search("*")["results"][0]
        assert "index1:lonestar" == res["id"]

        # create alias and check for results
        ftindex1.aliasadd("spaceballs")
        alias_client = getClient(client).ft("spaceballs")
        res = alias_client.search("*")["results"][0]
        assert "index1:lonestar" == res["id"]

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            ftindex2.aliasadd("spaceballs")

        # update alias and ensure new results
        ftindex2.aliasupdate("spaceballs")
        alias_client2 = getClient(client).ft("spaceballs")

        res = alias_client2.search("*")["results"][0]
        assert "index2:yogurt" == res["id"]

    ftindex2.aliasdel("spaceballs")
    with pytest.raises(Exception):
        alias_client2.search("*").docs[0]


@pytest.mark.redismod
@pytest.mark.xfail(strict=False)
def test_alias_basic(client):
    # Creating a client with one index
    index1 = getClient(client).ft("testAlias")

    index1.create_index((TextField("txt"),))
    index1.client.hset("doc1", mapping={"txt": "text goes here"})

    index2 = getClient(client).ft("testAlias2")
    index2.create_index((TextField("txt"),))
    index2.client.hset("doc2", mapping={"txt": "text goes here"})

    # add the actual alias and check
    index1.aliasadd("myalias")
    alias_client = getClient(client).ft("myalias")
    if is_resp2_connection(client):
        res = sorted(alias_client.search("*").docs, key=lambda x: x.id)
        assert "doc1" == res[0].id

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            index2.aliasadd("myalias")

        # update the alias and ensure we get doc2
        index2.aliasupdate("myalias")
        alias_client2 = getClient(client).ft("myalias")
        res = sorted(alias_client2.search("*").docs, key=lambda x: x.id)
        assert "doc1" == res[0].id
    else:
        res = sorted(alias_client.search("*")["results"], key=lambda x: x["id"])
        assert "doc1" == res[0]["id"]

        # Throw an exception when trying to add an alias that already exists
        with pytest.raises(Exception):
            index2.aliasadd("myalias")

        # update the alias and ensure we get doc2
        index2.aliasupdate("myalias")
        alias_client2 = getClient(client).ft("myalias")
        res = sorted(alias_client2.search("*")["results"], key=lambda x: x["id"])
        assert "doc1" == res[0]["id"]

    # delete the alias and expect an error if we try to query again
    index2.aliasdel("myalias")
    with pytest.raises(Exception):
        _ = alias_client2.search("*").docs[0]


@pytest.mark.redismod
def test_textfield_sortable_nostem(client):
    # Creating the index definition with sortable and no_stem
    client.ft().create_index((TextField("txt", sortable=True, no_stem=True),))

    # Now get the index info to confirm its contents
    response = client.ft().info()
    if is_resp2_connection(client):
        assert "SORTABLE" in response["attributes"][0]
        assert "NOSTEM" in response["attributes"][0]
    else:
        assert "SORTABLE" in response["attributes"][0]["flags"]
        assert "NOSTEM" in response["attributes"][0]["flags"]


@pytest.mark.redismod
def test_alter_schema_add(client):
    # Creating the index definition and schema
    client.ft().create_index(TextField("title"))

    # Using alter to add a field
    client.ft().alter_schema_add(TextField("body"))

    # Indexing a document
    client.hset(
        "doc1", mapping={"title": "MyTitle", "body": "Some content only in the body"}
    )

    # Searching with parameter only in the body (the added field)
    q = Query("only in the body")

    # Ensure we find the result searching on the added body field
    res = client.ft().search(q)
    if is_resp2_connection(client):
        assert 1 == res.total
    else:
        assert 1 == res["total_results"]


@pytest.mark.redismod
def test_spell_check(client):
    client.ft().create_index((TextField("f1"), TextField("f2")))

    client.hset(
        "doc1", mapping={"f1": "some valid content", "f2": "this is sample text"}
    )
    client.hset("doc2", mapping={"f1": "very important", "f2": "lorem ipsum"})
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    if is_resp2_connection(client):
        # test spellcheck
        res = client.ft().spellcheck("impornant")
        assert "important" == res["impornant"][0]["suggestion"]

        res = client.ft().spellcheck("contnt")
        assert "content" == res["contnt"][0]["suggestion"]

        # test spellcheck with Levenshtein distance
        res = client.ft().spellcheck("vlis")
        assert res == {}
        res = client.ft().spellcheck("vlis", distance=2)
        assert "valid" == res["vlis"][0]["suggestion"]

        # test spellcheck include
        client.ft().dict_add("dict", "lore", "lorem", "lorm")
        res = client.ft().spellcheck("lorm", include="dict")
        assert len(res["lorm"]) == 3
        assert (
            res["lorm"][0]["suggestion"],
            res["lorm"][1]["suggestion"],
            res["lorm"][2]["suggestion"],
        ) == ("lorem", "lore", "lorm")
        assert (res["lorm"][0]["score"], res["lorm"][1]["score"]) == ("0.5", "0")

        # test spellcheck exclude
        res = client.ft().spellcheck("lorm", exclude="dict")
        assert res == {}
    else:
        # test spellcheck
        res = client.ft().spellcheck("impornant")
        assert "important" in res["results"]["impornant"][0].keys()

        res = client.ft().spellcheck("contnt")
        assert "content" in res["results"]["contnt"][0].keys()

        # test spellcheck with Levenshtein distance
        res = client.ft().spellcheck("vlis")
        assert res == {"results": {"vlis": []}}
        res = client.ft().spellcheck("vlis", distance=2)
        assert "valid" in res["results"]["vlis"][0].keys()

        # test spellcheck include
        client.ft().dict_add("dict", "lore", "lorem", "lorm")
        res = client.ft().spellcheck("lorm", include="dict")
        assert len(res["results"]["lorm"]) == 3
        assert "lorem" in res["results"]["lorm"][0].keys()
        assert "lore" in res["results"]["lorm"][1].keys()
        assert "lorm" in res["results"]["lorm"][2].keys()
        assert (
            res["results"]["lorm"][0]["lorem"],
            res["results"]["lorm"][1]["lore"],
        ) == (0.5, 0)

        # test spellcheck exclude
        res = client.ft().spellcheck("lorm", exclude="dict")
        assert res == {"results": {}}


@pytest.mark.redismod
def test_dict_operations(client):
    client.ft().create_index((TextField("f1"), TextField("f2")))
    # Add three items
    res = client.ft().dict_add("custom_dict", "item1", "item2", "item3")
    assert 3 == res

    # Remove one item
    res = client.ft().dict_del("custom_dict", "item2")
    assert 1 == res

    # Dump dict and inspect content
    res = client.ft().dict_dump("custom_dict")
    assert res == ["item1", "item3"]

    # Remove rest of the items before reload
    client.ft().dict_del("custom_dict", *res)


@pytest.mark.redismod
def test_phonetic_matcher(client):
    client.ft().create_index((TextField("name"),))
    client.hset("doc1", mapping={"name": "Jon"})
    client.hset("doc2", mapping={"name": "John"})

    res = client.ft().search(Query("Jon"))
    if is_resp2_connection(client):
        assert 1 == len(res.docs)
        assert "Jon" == res.docs[0].name
    else:
        assert 1 == res["total_results"]
        assert "Jon" == res["results"][0]["extra_attributes"]["name"]

    # Drop and create index with phonetic matcher
    client.flushdb()

    client.ft().create_index((TextField("name", phonetic_matcher="dm:en"),))
    client.hset("doc1", mapping={"name": "Jon"})
    client.hset("doc2", mapping={"name": "John"})

    res = client.ft().search(Query("Jon"))
    if is_resp2_connection(client):
        assert 2 == len(res.docs)
        assert ["John", "Jon"] == sorted(d.name for d in res.docs)
    else:
        assert 2 == res["total_results"]
        assert ["John", "Jon"] == sorted(
            d["extra_attributes"]["name"] for d in res["results"]
        )


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_scorer(client):
    client.ft().create_index((TextField("description"),))

    client.hset(
        "doc1", mapping={"description": "The quick brown fox jumps over the lazy dog"}
    )
    client.hset(
        "doc2",
        mapping={
            "description": "Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do."  # noqa
        },
    )

    # default scorer is TFIDF
    if is_resp2_connection(client):
        res = client.ft().search(Query("quick").with_scores())
        assert 1.0 == res.docs[0].score
        res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
        assert 1.0 == res.docs[0].score
        res = client.ft().search(Query("quick").scorer("TFIDF.DOCNORM").with_scores())
        assert 0.14285714285714285 == res.docs[0].score
        res = client.ft().search(Query("quick").scorer("BM25").with_scores())
        assert 0.22471909420069797 == res.docs[0].score
        res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
        assert 2.0 == res.docs[0].score
        res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
        assert 1.0 == res.docs[0].score
        res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
        assert 0.0 == res.docs[0].score
    else:
        res = client.ft().search(Query("quick").with_scores())
        assert 1.0 == res["results"][0]["score"]
        res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
        assert 1.0 == res["results"][0]["score"]
        res = client.ft().search(Query("quick").scorer("TFIDF.DOCNORM").with_scores())
        assert 0.14285714285714285 == res["results"][0]["score"]
        res = client.ft().search(Query("quick").scorer("BM25").with_scores())
        assert 0.22471909420069797 == res["results"][0]["score"]
        res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
        assert 2.0 == res["results"][0]["score"]
        res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
        assert 1.0 == res["results"][0]["score"]
        res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
        assert 0.0 == res["results"][0]["score"]


@pytest.mark.redismod
def test_get(client):
    client.ft().create_index((TextField("f1"), TextField("f2")))

    assert [None] == client.ft().get("doc1")
    assert [None, None] == client.ft().get("doc2", "doc1")

    client.hset(
        "doc1", mapping={"f1": "some valid content dd1", "f2": "this is sample text f1"}
    )
    client.hset(
        "doc2", mapping={"f1": "some valid content dd2", "f2": "this is sample text f2"}
    )

    assert [
        ["f1", "some valid content dd2", "f2", "this is sample text f2"]
    ] == client.ft().get("doc2")
    assert [
        ["f1", "some valid content dd1", "f2", "this is sample text f1"],
        ["f1", "some valid content dd2", "f2", "this is sample text f2"],
    ] == client.ft().get("doc1", "doc2")


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("2.2.0", "search")
def test_config(client):
    assert client.ft().config_set("TIMEOUT", "100")
    with pytest.raises(redis.ResponseError):
        client.ft().config_set("TIMEOUT", "null")
    res = client.ft().config_get("*")
    assert "100" == res["TIMEOUT"]
    res = client.ft().config_get("TIMEOUT")
    assert "100" == res["TIMEOUT"]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_aggregations_groupby(client):
    # Creating the index definition and schema
    client.ft().create_index(
        (
            NumericField("random_num"),
            TextField("title"),
            TextField("body"),
            TextField("parent"),
        )
    )

    # Indexing a document
    client.hset(
        "search",
        mapping={
            "title": "RediSearch",
            "body": "Redisearch impements a search engine on top of redis",
            "parent": "redis",
            "random_num": 10,
        },
    )
    client.hset(
        "ai",
        mapping={
            "title": "RedisAI",
            "body": "RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
            "parent": "redis",
            "random_num": 3,
        },
    )
    client.hset(
        "json",
        mapping={
            "title": "RedisJson",
            "body": "RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
            "parent": "redis",
            "random_num": 8,
        },
    )

    if is_resp2_connection(client):
        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count()
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count_distinct("@title")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count_distinctish("@title")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.sum("@random_num")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "21"  # 10+8+3

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.min("@random_num")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "3"  # min(10,8,3)

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.max("@random_num")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "10"  # max(10,8,3)

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.avg("@random_num")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        index = res.index("__generated_aliasavgrandom_num")
        assert res[index + 1] == "7"  # (10+3+8)/3

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.stddev("random_num")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "3.60555127546"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.quantile("@random_num", 0.5)
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[3] == "8"  # median of 3,8,10

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.tolist("@title")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert set(res[3]) == {"RediSearch", "RedisAI", "RedisJson"}

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.first_value("@title").alias("first")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res == ["parent", "redis", "first", "RediSearch"]

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.random_sample("@title", 2).alias("random")
        )

        res = client.ft().aggregate(req).rows[0]
        assert res[1] == "redis"
        assert res[2] == "random"
        assert len(res[3]) == 2
        assert res[3][0] in ["RediSearch", "RedisAI", "RedisJson"]
    else:
        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count()
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliascount"] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count_distinct("@title")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliascount_distincttitle"] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.count_distinctish("@title")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliascount_distinctishtitle"] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.sum("@random_num")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliassumrandom_num"] == "21"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.min("@random_num")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliasminrandom_num"] == "3"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.max("@random_num")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliasmaxrandom_num"] == "10"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.avg("@random_num")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliasavgrandom_num"] == "7"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.stddev("random_num")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert (
            res["extra_attributes"]["__generated_aliasstddevrandom_num"]
            == "3.60555127546"
        )

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.quantile("@random_num", 0.5)
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert res["extra_attributes"]["__generated_aliasquantilerandom_num,0.5"] == "8"

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.tolist("@title")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert set(res["extra_attributes"]["__generated_aliastolisttitle"]) == {
            "RediSearch",
            "RedisAI",
            "RedisJson",
        }

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.first_value("@title").alias("first")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"] == {"parent": "redis", "first": "RediSearch"}

        req = aggregations.AggregateRequest("redis").group_by(
            "@parent", reducers.random_sample("@title", 2).alias("random")
        )

        res = client.ft().aggregate(req)["results"][0]
        assert res["extra_attributes"]["parent"] == "redis"
        assert "random" in res["extra_attributes"].keys()
        assert len(res["extra_attributes"]["random"]) == 2
        assert res["extra_attributes"]["random"][0] in [
            "RediSearch",
            "RedisAI",
            "RedisJson",
        ]


@pytest.mark.redismod
def test_aggregations_sort_by_and_limit(client):
    client.ft().create_index((TextField("t1"), TextField("t2")))

    client.ft().client.hset("doc1", mapping={"t1": "a", "t2": "b"})
    client.ft().client.hset("doc2", mapping={"t1": "b", "t2": "a"})

    if is_resp2_connection(client):
        # test sort_by using SortDirection
        req = aggregations.AggregateRequest("*").sort_by(
            aggregations.Asc("@t2"), aggregations.Desc("@t1")
        )
        res = client.ft().aggregate(req)
        assert res.rows[0] == ["t2", "a", "t1", "b"]
        assert res.rows[1] == ["t2", "b", "t1", "a"]

        # test sort_by without SortDirection
        req = aggregations.AggregateRequest("*").sort_by("@t1")
        res = client.ft().aggregate(req)
        assert res.rows[0] == ["t1", "a"]
        assert res.rows[1] == ["t1", "b"]

        # test sort_by with max
        req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
        res = client.ft().aggregate(req)
        assert len(res.rows) == 1

        # test limit
        req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
        res = client.ft().aggregate(req)
        assert len(res.rows) == 1
        assert res.rows[0] == ["t1", "b"]
    else:
        # test sort_by using SortDirection
        req = aggregations.AggregateRequest("*").sort_by(
            aggregations.Asc("@t2"), aggregations.Desc("@t1")
        )
        res = client.ft().aggregate(req)["results"]
        assert res[0]["extra_attributes"] == {"t2": "a", "t1": "b"}
        assert res[1]["extra_attributes"] == {"t2": "b", "t1": "a"}

        # test sort_by without SortDirection
        req = aggregations.AggregateRequest("*").sort_by("@t1")
        res = client.ft().aggregate(req)["results"]
        assert res[0]["extra_attributes"] == {"t1": "a"}
        assert res[1]["extra_attributes"] == {"t1": "b"}

        # test sort_by with max
        req = aggregations.AggregateRequest("*").sort_by("@t1", max=1)
        res = client.ft().aggregate(req)
        assert len(res["results"]) == 1

        # test limit
        req = aggregations.AggregateRequest("*").sort_by("@t1").limit(1, 1)
        res = client.ft().aggregate(req)
        assert len(res["results"]) == 1
        assert res["results"][0]["extra_attributes"] == {"t1": "b"}


@pytest.mark.redismod
def test_aggregations_load(client):
    client.ft().create_index((TextField("t1"), TextField("t2")))

    client.ft().client.hset("doc1", mapping={"t1": "hello", "t2": "world"})

    if is_resp2_connection(client):
        # load t1
        req = aggregations.AggregateRequest("*").load("t1")
        res = client.ft().aggregate(req)
        assert res.rows[0] == ["t1", "hello"]

        # load t2
        req = aggregations.AggregateRequest("*").load("t2")
        res = client.ft().aggregate(req)
        assert res.rows[0] == ["t2", "world"]

        # load all
        req = aggregations.AggregateRequest("*").load()
        res = client.ft().aggregate(req)
        assert res.rows[0] == ["t1", "hello", "t2", "world"]
    else:
        # load t1
        req = aggregations.AggregateRequest("*").load("t1")
        res = client.ft().aggregate(req)
        assert res["results"][0]["extra_attributes"] == {"t1": "hello"}

        # load t2
        req = aggregations.AggregateRequest("*").load("t2")
        res = client.ft().aggregate(req)
        assert res["results"][0]["extra_attributes"] == {"t2": "world"}

        # load all
        req = aggregations.AggregateRequest("*").load()
        res = client.ft().aggregate(req)
        assert res["results"][0]["extra_attributes"] == {"t1": "hello", "t2": "world"}


@pytest.mark.redismod
def test_aggregations_apply(client):
    client.ft().create_index(
        (
            TextField("PrimaryKey", sortable=True),
            NumericField("CreatedDateTimeUTC", sortable=True),
        )
    )

    client.ft().client.hset(
        "doc1",
        mapping={"PrimaryKey": "9::362330", "CreatedDateTimeUTC": "637387878524969984"},
    )
    client.ft().client.hset(
        "doc2",
        mapping={"PrimaryKey": "9::362329", "CreatedDateTimeUTC": "637387875859270016"},
    )

    req = aggregations.AggregateRequest("*").apply(
        CreatedDateTimeUTC="@CreatedDateTimeUTC * 10"
    )
    res = client.ft().aggregate(req)
    if is_resp2_connection(client):
        res_set = {res.rows[0][1], res.rows[1][1]}
        assert res_set == {"6373878785249699840", "6373878758592700416"}
    else:
        res_set = {
            res["results"][0]["extra_attributes"]["CreatedDateTimeUTC"],
            res["results"][1]["extra_attributes"]["CreatedDateTimeUTC"],
        }
        assert res_set == {"6373878785249699840", "6373878758592700416"}


@pytest.mark.redismod
def test_aggregations_filter(client):
    client.ft().create_index(
        (TextField("name", sortable=True), NumericField("age", sortable=True))
    )

    client.ft().client.hset("doc1", mapping={"name": "bar", "age": "25"})
    client.ft().client.hset("doc2", mapping={"name": "foo", "age": "19"})

    for dialect in [1, 2]:
        req = (
            aggregations.AggregateRequest("*")
            .filter("@name=='foo' && @age < 20")
            .dialect(dialect)
        )
        res = client.ft().aggregate(req)
        if is_resp2_connection(client):
            assert len(res.rows) == 1
            assert res.rows[0] == ["name", "foo", "age", "19"]

            req = (
                aggregations.AggregateRequest("*")
                .filter("@age > 15")
                .sort_by("@age")
                .dialect(dialect)
            )
            res = client.ft().aggregate(req)
            assert len(res.rows) == 2
            assert res.rows[0] == ["age", "19"]
            assert res.rows[1] == ["age", "25"]
        else:
            assert len(res["results"]) == 1
            assert res["results"][0]["extra_attributes"] == {"name": "foo", "age": "19"}

            req = (
                aggregations.AggregateRequest("*")
                .filter("@age > 15")
                .sort_by("@age")
                .dialect(dialect)
            )
            res = client.ft().aggregate(req)
            assert len(res["results"]) == 2
            assert res["results"][0]["extra_attributes"] == {"age": "19"}
            assert res["results"][1]["extra_attributes"] == {"age": "25"}


@pytest.mark.redismod
@skip_ifmodversion_lt("2.10.05", "search")
def test_aggregations_add_scores(client):
    client.ft().create_index(
        (
            TextField("name", sortable=True, weight=5.0),
            NumericField("age", sortable=True),
        )
    )

    client.hset("doc1", mapping={"name": "bar", "age": "25"})
    client.hset("doc2", mapping={"name": "foo", "age": "19"})

    req = aggregations.AggregateRequest("*").add_scores()
    res = client.ft().aggregate(req)

    if isinstance(res, dict):
        assert len(res["results"]) == 2
        assert res["results"][0]["extra_attributes"] == {"__score": "0.2"}
        assert res["results"][1]["extra_attributes"] == {"__score": "0.2"}
    else:
        assert len(res.rows) == 2
        assert res.rows[0] == ["__score", "0.2"]
        assert res.rows[1] == ["__score", "0.2"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
def test_index_definition(client):
    """
    Create definition and test its args
    """
    with pytest.raises(RuntimeError):
        IndexDefinition(prefix=["hset:", "henry"], index_type="json")

    definition = IndexDefinition(
        prefix=["hset:", "henry"],
        filter="@f1==32",
        language="English",
        language_field="play",
        score_field="chapter",
        score=0.5,
        payload_field="txt",
        index_type=IndexType.JSON,
    )

    assert [
        "ON",
        "JSON",
        "PREFIX",
        2,
        "hset:",
        "henry",
        "FILTER",
        "@f1==32",
        "LANGUAGE_FIELD",
        "play",
        "LANGUAGE",
        "English",
        "SCORE_FIELD",
        "chapter",
        "SCORE",
        0.5,
        "PAYLOAD_FIELD",
        "txt",
    ] == definition.args

    createIndex(client.ft(), num_docs=500, definition=definition)


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_redis_enterprise()
def test_expire(client):
    client.ft().create_index((TextField("txt", sortable=True),), temporary=4)
    ttl = client.execute_command("ft.debug", "TTL", "idx")
    assert ttl > 2

    while ttl > 2:
        ttl = client.execute_command("ft.debug", "TTL", "idx")
        time.sleep(0.01)


@pytest.mark.redismod
def test_skip_initial_scan(client):
    client.hset("doc1", "foo", "bar")
    q = Query("@foo:bar")

    client.ft().create_index((TextField("foo"),), skip_initial_scan=True)
    res = client.ft().search(q)
    if is_resp2_connection(client):
        assert res.total == 0
    else:
        assert res["total_results"] == 0


@pytest.mark.redismod
def test_summarize_disabled_nooffset(client):
    client.ft().create_index((TextField("txt"),), no_term_offsets=True)
    client.hset("doc1", mapping={"txt": "foo bar"})
    with pytest.raises(Exception):
        client.ft().search(Query("foo").summarize(fields=["txt"]))


@pytest.mark.redismod
def test_summarize_disabled_nohl(client):
    client.ft().create_index((TextField("txt"),), no_highlight=True)
    client.hset("doc1", mapping={"txt": "foo bar"})
    with pytest.raises(Exception):
        client.ft().search(Query("foo").summarize(fields=["txt"]))


@pytest.mark.redismod
def test_max_text_fields(client):
    # Creating the index definition
    client.ft().create_index((TextField("f0"),))
    for x in range(1, 32):
        client.ft().alter_schema_add((TextField(f"f{x}"),))

    # Should be too many indexes
    with pytest.raises(redis.ResponseError):
        client.ft().alter_schema_add((TextField(f"f{x}"),))

    client.ft().dropindex("idx")
    # Creating the index definition
    client.ft().create_index((TextField("f0"),), max_text_fields=True)
    # Fill the index with fields
    for x in range(1, 50):
        client.ft().alter_schema_add((TextField(f"f{x}"),))


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
def test_create_client_definition(client):
    """
    Create definition with no index type provided,
    and use hset to test the client definition (the default is HASH).
    """
    definition = IndexDefinition(prefix=["hset:", "henry"])
    createIndex(client.ft(), num_docs=500, definition=definition)

    info = client.ft().info()
    assert 494 == int(info["num_docs"])

    client.ft().client.hset("hset:1", "f1", "v1")
    info = client.ft().info()
    assert 495 == int(info["num_docs"])


@pytest.mark.redismod
@skip_ifmodversion_lt("2.0.0", "search")
def test_create_client_definition_hash(client):
    """
    Create definition with IndexType.HASH as index type (ON HASH),
    and use hset to test the client definition.
    """
    definition = IndexDefinition(prefix=["hset:", "henry"], index_type=IndexType.HASH)
    createIndex(client.ft(), num_docs=500, definition=definition)

    info = client.ft().info()
    assert 494 == int(info["num_docs"])

    client.ft().client.hset("hset:1", "f1", "v1")
    info = client.ft().info()
    assert 495 == int(info["num_docs"])


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_create_client_definition_json(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON),
    and use json client to test it.
    """
    definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
    client.ft().create_index((TextField("$.name"),), definition=definition)

    client.json().set("king:1", Path.root_path(), {"name": "henry"})
    client.json().set("king:2", Path.root_path(), {"name": "james"})

    res = client.ft().search("henry")
    if is_resp2_connection(client):
        assert res.docs[0].id == "king:1"
        assert res.docs[0].payload is None
        assert res.docs[0].json == '{"name":"henry"}'
        assert res.total == 1
    else:
        assert res["results"][0]["id"] == "king:1"
        assert res["results"][0]["extra_attributes"]["$"] == '{"name":"henry"}'
        assert res["total_results"] == 1


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_fields_as_name(client):
    # create index
    SCHEMA = (
        TextField("$.name", sortable=True, as_name="name"),
        NumericField("$.age", as_name="just_a_number"),
    )
    definition = IndexDefinition(index_type=IndexType.JSON)
    client.ft().create_index(SCHEMA, definition=definition)

    # insert json data
    res = client.json().set("doc:1", Path.root_path(), {"name": "Jon", "age": 25})
    assert res

    res = client.ft().search(Query("Jon").return_fields("name", "just_a_number"))
    if is_resp2_connection(client):
        assert 1 == len(res.docs)
        assert "doc:1" == res.docs[0].id
        assert "Jon" == res.docs[0].name
        assert "25" == res.docs[0].just_a_number
    else:
        assert 1 == len(res["results"])
        assert "doc:1" == res["results"][0]["id"]
        assert "Jon" == res["results"][0]["extra_attributes"]["name"]
        assert "25" == res["results"][0]["extra_attributes"]["just_a_number"]


@pytest.mark.redismod
def test_casesensitive(client):
    # create index
    SCHEMA = (TagField("t", case_sensitive=False),)
    client.ft().create_index(SCHEMA)
    client.ft().client.hset("1", "t", "HELLO")
    client.ft().client.hset("2", "t", "hello")

    res = client.ft().search("@t:{HELLO}")

    if is_resp2_connection(client):
        assert 2 == len(res.docs)
        assert "1" == res.docs[0].id
        assert "2" == res.docs[1].id
    else:
        assert 2 == len(res["results"])
        assert "1" == res["results"][0]["id"]
        assert "2" == res["results"][1]["id"]

    # create casesensitive index
    client.ft().dropindex()
    SCHEMA = (TagField("t", case_sensitive=True),)
    client.ft().create_index(SCHEMA)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    res = client.ft().search("@t:{HELLO}")
    if is_resp2_connection(client):
        assert 1 == len(res.docs)
        assert "1" == res.docs[0].id
    else:
        assert 1 == len(res["results"])
        assert "1" == res["results"][0]["id"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_search_return_fields(client):
    res = client.json().set(
        "doc:1",
        Path.root_path(),
        {"t": "riceratops", "t2": "telmatosaurus", "n": 9072, "flt": 97.2},
    )
    assert res

    # create index on
    definition = IndexDefinition(index_type=IndexType.JSON)
    SCHEMA = (TextField("$.t"), NumericField("$.flt"))
    client.ft().create_index(SCHEMA, definition=definition)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    if is_resp2_connection(client):
        total = client.ft().search(Query("*").return_field("$.t", as_field="txt")).docs
        assert 1 == len(total)
        assert "doc:1" == total[0].id
        assert "riceratops" == total[0].txt

        total = client.ft().search(Query("*").return_field("$.t2", as_field="txt")).docs
        assert 1 == len(total)
        assert "doc:1" == total[0].id
        assert "telmatosaurus" == total[0].txt
    else:
        total = client.ft().search(Query("*").return_field("$.t", as_field="txt"))
        assert 1 == len(total["results"])
        assert "doc:1" == total["results"][0]["id"]
        assert "riceratops" == total["results"][0]["extra_attributes"]["txt"]

        total = client.ft().search(Query("*").return_field("$.t2", as_field="txt"))
        assert 1 == len(total["results"])
        assert "doc:1" == total["results"][0]["id"]
        assert "telmatosaurus" == total["results"][0]["extra_attributes"]["txt"]


@pytest.mark.redismod
@skip_if_resp_version(3)
def test_binary_and_text_fields(client):
    fake_vec = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)

    index_name = "mixed_index"
    mixed_data = {"first_name": "🐍python", "vector_emb": fake_vec.tobytes()}
    client.hset(f"{index_name}:1", mapping=mixed_data)

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

    client.ft(index_name).create_index(
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
    docs = client.ft(index_name).search(query=query, query_params={}).docs
    decoded_vec_from_search_results = np.frombuffer(
        docs[0]["vector_emb"], dtype=np.float32
    )

    assert np.array_equal(
        decoded_vec_from_search_results, fake_vec
    ), "The vectors are not equal"

    assert (
        docs[0]["first_name"] == mixed_data["first_name"]
    ), "The text field is not decoded correctly"


@pytest.mark.redismod
def test_synupdate(client):
    definition = IndexDefinition(index_type=IndexType.HASH)
    client.ft().create_index(
        (TextField("title"), TextField("body")), definition=definition
    )

    client.ft().synupdate("id1", True, "boy", "child", "offspring")
    client.hset("doc1", mapping={"title": "he is a baby", "body": "this is a test"})

    client.ft().synupdate("id1", True, "baby")
    client.hset("doc2", mapping={"title": "he is another baby", "body": "another test"})

    res = client.ft().search(Query("child").expander("SYNONYM"))
    if is_resp2_connection(client):
        assert res.docs[0].id == "doc2"
        assert res.docs[0].title == "he is another baby"
        assert res.docs[0].body == "another test"
    else:
        assert res["results"][0]["id"] == "doc2"
        assert res["results"][0]["extra_attributes"]["title"] == "he is another baby"
        assert res["results"][0]["extra_attributes"]["body"] == "another test"


@pytest.mark.redismod
def test_syndump(client):
    definition = IndexDefinition(index_type=IndexType.HASH)
    client.ft().create_index(
        (TextField("title"), TextField("body")), definition=definition
    )

    client.ft().synupdate("id1", False, "boy", "child", "offspring")
    client.ft().synupdate("id2", False, "baby", "child")
    client.ft().synupdate("id3", False, "tree", "wood")
    res = client.ft().syndump()
    assert res == {
        "boy": ["id1"],
        "tree": ["id3"],
        "wood": ["id3"],
        "child": ["id1", "id2"],
        "baby": ["id2"],
        "offspring": ["id1"],
    }


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_create_json_with_alias(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON) with two
    fields with aliases, and use json client to test it.
    """
    definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
    client.ft().create_index(
        (TextField("$.name", as_name="name"), NumericField("$.num", as_name="num")),
        definition=definition,
    )

    client.json().set("king:1", Path.root_path(), {"name": "henry", "num": 42})
    client.json().set("king:2", Path.root_path(), {"name": "james", "num": 3.14})

    if is_resp2_connection(client):
        res = client.ft().search("@name:henry")
        assert res.docs[0].id == "king:1"
        assert res.docs[0].json == '{"name":"henry","num":42}'
        assert res.total == 1

        res = client.ft().search("@num:[0 10]")
        assert res.docs[0].id == "king:2"
        assert res.docs[0].json == '{"name":"james","num":3.14}'
        assert res.total == 1
    else:
        res = client.ft().search("@name:henry")
        assert res["results"][0]["id"] == "king:1"
        assert res["results"][0]["extra_attributes"]["$"] == '{"name":"henry","num":42}'
        assert res["total_results"] == 1

        res = client.ft().search("@num:[0 10]")
        assert res["results"][0]["id"] == "king:2"
        assert (
            res["results"][0]["extra_attributes"]["$"] == '{"name":"james","num":3.14}'
        )
        assert res["total_results"] == 1

    # Tests returns an error if path contain special characters (user should
    # use an alias)
    with pytest.raises(Exception):
        client.ft().search("@$.name:henry")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_json_with_multipath(client):
    """
    Create definition with IndexType.JSON as index type (ON JSON),
    and use json client to test it.
    """
    definition = IndexDefinition(prefix=["king:"], index_type=IndexType.JSON)
    client.ft().create_index(
        (TagField("$..name", as_name="name")), definition=definition
    )

    client.json().set(
        "king:1", Path.root_path(), {"name": "henry", "country": {"name": "england"}}
    )

    if is_resp2_connection(client):
        res = client.ft().search("@name:{henry}")
        assert res.docs[0].id == "king:1"
        assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
        assert res.total == 1

        res = client.ft().search("@name:{england}")
        assert res.docs[0].id == "king:1"
        assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
        assert res.total == 1
    else:
        res = client.ft().search("@name:{henry}")
        assert res["results"][0]["id"] == "king:1"
        assert (
            res["results"][0]["extra_attributes"]["$"]
            == '{"name":"henry","country":{"name":"england"}}'
        )
        assert res["total_results"] == 1

        res = client.ft().search("@name:{england}")
        assert res["results"][0]["id"] == "king:1"
        assert (
            res["results"][0]["extra_attributes"]["$"]
            == '{"name":"henry","country":{"name":"england"}}'
        )
        assert res["total_results"] == 1


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_json_with_jsonpath(client):
    definition = IndexDefinition(index_type=IndexType.JSON)
    client.ft().create_index(
        (
            TextField('$["prod:name"]', as_name="name"),
            TextField("$.prod:name", as_name="name_unsupported"),
        ),
        definition=definition,
    )

    client.json().set("doc:1", Path.root_path(), {"prod:name": "RediSearch"})

    if is_resp2_connection(client):
        # query for a supported field succeeds
        res = client.ft().search(Query("@name:RediSearch"))
        assert res.total == 1
        assert res.docs[0].id == "doc:1"
        assert res.docs[0].json == '{"prod:name":"RediSearch"}'

        # query for an unsupported field
        res = client.ft().search("@name_unsupported:RediSearch")
        assert res.total == 1

        # return of a supported field succeeds
        res = client.ft().search(Query("@name:RediSearch").return_field("name"))
        assert res.total == 1
        assert res.docs[0].id == "doc:1"
        assert res.docs[0].name == "RediSearch"
    else:
        # query for a supported field succeeds
        res = client.ft().search(Query("@name:RediSearch"))
        assert res["total_results"] == 1
        assert res["results"][0]["id"] == "doc:1"
        assert (
            res["results"][0]["extra_attributes"]["$"] == '{"prod:name":"RediSearch"}'
        )

        # query for an unsupported field
        res = client.ft().search("@name_unsupported:RediSearch")
        assert res["total_results"] == 1

        # return of a supported field succeeds
        res = client.ft().search(Query("@name:RediSearch").return_field("name"))
        assert res["total_results"] == 1
        assert res["results"][0]["id"] == "doc:1"
        assert res["results"][0]["extra_attributes"]["name"] == "RediSearch"


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_redis_enterprise()
def test_profile(client):
    client.ft().create_index((TextField("t"),))
    client.ft().client.hset("1", "t", "hello")
    client.ft().client.hset("2", "t", "world")

    # check using Query
    q = Query("hello|world").no_content()
    if is_resp2_connection(client):
        res, det = client.ft().profile(q)
        assert det["Iterators profile"]["Counter"] == 2.0
        assert len(det["Iterators profile"]["Child iterators"]) == 2
        assert det["Iterators profile"]["Type"] == "UNION"
        assert det["Parsing time"] < 0.5
        assert len(res.docs) == 2  # check also the search result

        # check using AggregateRequest
        req = (
            aggregations.AggregateRequest("*")
            .load("t")
            .apply(prefix="startswith(@t, 'hel')")
        )
        res, det = client.ft().profile(req)
        assert det["Iterators profile"]["Counter"] == 2
        assert det["Iterators profile"]["Type"] == "WILDCARD"
        assert isinstance(det["Parsing time"], float)
        assert len(res.rows) == 2  # check also the search result
    else:
        res = client.ft().profile(q)
        assert res["profile"]["Iterators profile"][0]["Counter"] == 2.0
        assert res["profile"]["Iterators profile"][0]["Type"] == "UNION"
        assert res["profile"]["Parsing time"] < 0.5
        assert len(res["results"]) == 2  # check also the search result

        # check using AggregateRequest
        req = (
            aggregations.AggregateRequest("*")
            .load("t")
            .apply(prefix="startswith(@t, 'hel')")
        )
        res = client.ft().profile(req)
        assert res["profile"]["Iterators profile"][0]["Counter"] == 2
        assert res["profile"]["Iterators profile"][0]["Type"] == "WILDCARD"
        assert isinstance(res["profile"]["Parsing time"], float)
        assert len(res["results"]) == 2  # check also the search result


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_profile_limited(client):
    client.ft().create_index((TextField("t"),))
    client.ft().client.hset("1", "t", "hello")
    client.ft().client.hset("2", "t", "hell")
    client.ft().client.hset("3", "t", "help")
    client.ft().client.hset("4", "t", "helowa")

    q = Query("%hell% hel*")
    if is_resp2_connection(client):
        res, det = client.ft().profile(q, limited=True)
        assert (
            det["Iterators profile"]["Child iterators"][0]["Child iterators"]
            == "The number of iterators in the union is 3"
        )
        assert (
            det["Iterators profile"]["Child iterators"][1]["Child iterators"]
            == "The number of iterators in the union is 4"
        )
        assert det["Iterators profile"]["Type"] == "INTERSECT"
        assert len(res.docs) == 3  # check also the search result
    else:
        res = client.ft().profile(q, limited=True)
        iterators_profile = res["profile"]["Iterators profile"]
        assert (
            iterators_profile[0]["Child iterators"][0]["Child iterators"]
            == "The number of iterators in the union is 3"
        )
        assert (
            iterators_profile[0]["Child iterators"][1]["Child iterators"]
            == "The number of iterators in the union is 4"
        )
        assert iterators_profile[0]["Type"] == "INTERSECT"
        assert len(res["results"]) == 3  # check also the search result


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_profile_query_params(client):
    client.ft().create_index(
        (
            VectorField(
                "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
            ),
        )
    )
    client.hset("a", "v", "aaaaaaaa")
    client.hset("b", "v", "aaaabaaa")
    client.hset("c", "v", "aaaaabaa")
    query = "*=>[KNN 2 @v $vec]"
    q = Query(query).return_field("__v_score").sort_by("__v_score", True).dialect(2)
    if is_resp2_connection(client):
        res, det = client.ft().profile(q, query_params={"vec": "aaaaaaaa"})
        assert det["Iterators profile"]["Counter"] == 2.0
        assert det["Iterators profile"]["Type"] == "VECTOR"
        assert res.total == 2
        assert "a" == res.docs[0].id
        assert "0" == res.docs[0].__getattribute__("__v_score")
    else:
        res = client.ft().profile(q, query_params={"vec": "aaaaaaaa"})
        assert res["profile"]["Iterators profile"][0]["Counter"] == 2
        assert res["profile"]["Iterators profile"][0]["Type"] == "VECTOR"
        assert res["total_results"] == 2
        assert "a" == res["results"][0]["id"]
        assert "0" == res["results"][0]["extra_attributes"]["__v_score"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_vector_field(client):
    client.flushdb()
    client.ft().create_index(
        (
            VectorField(
                "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
            ),
        )
    )
    client.hset("a", "v", "aaaaaaaa")
    client.hset("b", "v", "aaaabaaa")
    client.hset("c", "v", "aaaaabaa")

    query = "*=>[KNN 2 @v $vec]"
    q = Query(query).return_field("__v_score").sort_by("__v_score", True).dialect(2)
    res = client.ft().search(q, query_params={"vec": "aaaaaaaa"})

    if is_resp2_connection(client):
        assert "a" == res.docs[0].id
        assert "0" == res.docs[0].__getattribute__("__v_score")
    else:
        assert "a" == res["results"][0]["id"]
        assert "0" == res["results"][0]["extra_attributes"]["__v_score"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_vector_field_error(r):
    r.flushdb()

    # sortable tag
    with pytest.raises(Exception):
        r.ft().create_index((VectorField("v", "HNSW", {}, sortable=True),))

    # not supported algorithm
    with pytest.raises(Exception):
        r.ft().create_index((VectorField("v", "SORT", {}),))


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_text_params(client):
    client.flushdb()
    client.ft().create_index((TextField("name"),))

    client.hset("doc1", mapping={"name": "Alice"})
    client.hset("doc2", mapping={"name": "Bob"})
    client.hset("doc3", mapping={"name": "Carol"})

    params_dict = {"name1": "Alice", "name2": "Bob"}
    q = Query("@name:($name1 | $name2 )").dialect(2)
    res = client.ft().search(q, query_params=params_dict)
    if is_resp2_connection(client):
        assert 2 == res.total
        assert "doc1" == res.docs[0].id
        assert "doc2" == res.docs[1].id
    else:
        assert 2 == res["total_results"]
        assert "doc1" == res["results"][0]["id"]
        assert "doc2" == res["results"][1]["id"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_numeric_params(client):
    client.flushdb()
    client.ft().create_index((NumericField("numval"),))

    client.hset("doc1", mapping={"numval": 101})
    client.hset("doc2", mapping={"numval": 102})
    client.hset("doc3", mapping={"numval": 103})

    params_dict = {"min": 101, "max": 102}
    q = Query("@numval:[$min $max]").dialect(2)
    res = client.ft().search(q, query_params=params_dict)

    if is_resp2_connection(client):
        assert 2 == res.total
        assert "doc1" == res.docs[0].id
        assert "doc2" == res.docs[1].id
    else:
        assert 2 == res["total_results"]
        assert "doc1" == res["results"][0]["id"]
        assert "doc2" == res["results"][1]["id"]


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_geo_params(client):
    client.ft().create_index(GeoField("g"))
    client.hset("doc1", mapping={"g": "29.69465, 34.95126"})
    client.hset("doc2", mapping={"g": "29.69350, 34.94737"})
    client.hset("doc3", mapping={"g": "29.68746, 34.94882"})

    params_dict = {"lat": "34.95126", "lon": "29.69465", "radius": 1000, "units": "km"}
    q = Query("@g:[$lon $lat $radius $units]").dialect(2)
    res = client.ft().search(q, query_params=params_dict)
    _assert_search_result(client, res, ["doc1", "doc2", "doc3"])


@pytest.mark.redismod
def test_geoshapes_query_intersects_and_disjoint(client):
    client.ft().create_index((GeoShapeField("g", coord_system=GeoShapeField.FLAT)))
    client.hset("doc_point1", mapping={"g": "POINT (10 10)"})
    client.hset("doc_point2", mapping={"g": "POINT (50 50)"})
    client.hset("doc_polygon1", mapping={"g": "POLYGON ((20 20, 25 35, 35 25, 20 20))"})
    client.hset(
        "doc_polygon2", mapping={"g": "POLYGON ((60 60, 65 75, 70 70, 65 55, 60 60))"}
    )

    intersection = client.ft().search(
        Query("@g:[intersects $shape]").dialect(3),
        query_params={"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
    )
    _assert_search_result(client, intersection, ["doc_point2", "doc_polygon1"])

    disjunction = client.ft().search(
        Query("@g:[disjoint $shape]").dialect(3),
        query_params={"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
    )
    _assert_search_result(client, disjunction, ["doc_point1", "doc_polygon2"])


@pytest.mark.redismod
@skip_ifmodversion_lt("2.10.0", "search")
def test_geoshapes_query_contains_and_within(client):
    client.ft().create_index((GeoShapeField("g", coord_system=GeoShapeField.FLAT)))
    client.hset("doc_point1", mapping={"g": "POINT (10 10)"})
    client.hset("doc_point2", mapping={"g": "POINT (50 50)"})
    client.hset("doc_polygon1", mapping={"g": "POLYGON ((20 20, 25 35, 35 25, 20 20))"})
    client.hset(
        "doc_polygon2", mapping={"g": "POLYGON ((60 60, 65 75, 70 70, 65 55, 60 60))"}
    )

    contains_a = client.ft().search(
        Query("@g:[contains $shape]").dialect(3),
        query_params={"shape": "POINT(25 25)"},
    )
    _assert_search_result(client, contains_a, ["doc_polygon1"])

    contains_b = client.ft().search(
        Query("@g:[contains $shape]").dialect(3),
        query_params={"shape": "POLYGON((24 24, 24 26, 25 25, 24 24))"},
    )
    _assert_search_result(client, contains_b, ["doc_polygon1"])

    within = client.ft().search(
        Query("@g:[within $shape]").dialect(3),
        query_params={"shape": "POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))"},
    )
    _assert_search_result(client, within, ["doc_point2", "doc_polygon1"])


@pytest.mark.redismod
@skip_if_redis_enterprise()
def test_search_commands_in_pipeline(client):
    p = client.ft().pipeline()
    p.create_index((TextField("txt"),))
    p.hset("doc1", mapping={"txt": "foo bar"})
    p.hset("doc2", mapping={"txt": "foo bar"})
    q = Query("foo bar").with_payloads()
    p.search(q)
    res = p.execute()
    if is_resp2_connection(client):
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
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("2.4.3", "search")
def test_dialect_config(client):
    assert client.ft().config_get("DEFAULT_DIALECT")
    client.ft().config_set("DEFAULT_DIALECT", 2)
    assert client.ft().config_get("DEFAULT_DIALECT") == {"DEFAULT_DIALECT": "2"}
    with pytest.raises(redis.ResponseError):
        client.ft().config_set("DEFAULT_DIALECT", 0)


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_dialect(client):
    client.ft().create_index(
        (
            TagField("title"),
            TextField("t1"),
            TextField("t2"),
            NumericField("num"),
            VectorField(
                "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 1, "DISTANCE_METRIC": "COSINE"}
            ),
        )
    )
    client.hset("h", "t1", "hello")
    with pytest.raises(redis.ResponseError) as err:
        client.ft().explain(Query("(*)").dialect(1))
    assert "Syntax error" in str(err)
    assert "WILDCARD" in client.ft().explain(Query("(*)").dialect(2))

    with pytest.raises(redis.ResponseError) as err:
        client.ft().explain(Query("$hello").dialect(1))
    assert "Syntax error" in str(err)
    q = Query("$hello").dialect(2)
    expected = "UNION {\n  hello\n  +hello(expanded)\n}\n"
    assert expected in client.ft().explain(q, query_params={"hello": "hello"})

    expected = "NUMERIC {0.000000 <= @num <= 10.000000}\n"
    assert expected in client.ft().explain(Query("@title:(@num:[0 10])").dialect(1))
    with pytest.raises(redis.ResponseError) as err:
        client.ft().explain(Query("@title:(@num:[0 10])").dialect(2))
    assert "Syntax error" in str(err)


@pytest.mark.redismod
def test_expire_while_search(client: redis.Redis):
    client.ft().create_index((TextField("txt"),))
    client.hset("hset:1", "txt", "a")
    client.hset("hset:2", "txt", "b")
    client.hset("hset:3", "txt", "c")
    if is_resp2_connection(client):
        assert 3 == client.ft().search(Query("*")).total
        client.pexpire("hset:2", 300)
        for _ in range(500):
            client.ft().search(Query("*")).docs[1]
        time.sleep(1)
        assert 2 == client.ft().search(Query("*")).total
    else:
        assert 3 == client.ft().search(Query("*"))["total_results"]
        client.pexpire("hset:2", 300)
        for _ in range(500):
            client.ft().search(Query("*"))["results"][1]
        time.sleep(1)
        assert 2 == client.ft().search(Query("*"))["total_results"]


@pytest.mark.redismod
@pytest.mark.experimental
def test_withsuffixtrie(client: redis.Redis):
    # create index
    assert client.ft().create_index((TextField("txt"),))
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
    if is_resp2_connection(client):
        info = client.ft().info()
        assert "WITHSUFFIXTRIE" not in info["attributes"][0]
        assert client.ft().dropindex("idx")

        # create withsuffixtrie index (text fields)
        assert client.ft().create_index(TextField("t", withsuffixtrie=True))
        waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        info = client.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]
        assert client.ft().dropindex("idx")

        # create withsuffixtrie index (tag field)
        assert client.ft().create_index(TagField("t", withsuffixtrie=True))
        waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        info = client.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]
    else:
        info = client.ft().info()
        assert "WITHSUFFIXTRIE" not in info["attributes"][0]["flags"]
        assert client.ft().dropindex("idx")

        # create withsuffixtrie index (text fields)
        assert client.ft().create_index(TextField("t", withsuffixtrie=True))
        waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        info = client.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]
        assert client.ft().dropindex("idx")

        # create withsuffixtrie index (tag field)
        assert client.ft().create_index(TagField("t", withsuffixtrie=True))
        waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
        info = client.ft().info()
        assert "WITHSUFFIXTRIE" in info["attributes"][0]["flags"]


@pytest.mark.redismod
def test_query_timeout(r: redis.Redis):
    q1 = Query("foo").timeout(5000)
    assert q1.get_args() == ["foo", "TIMEOUT", 5000, "LIMIT", 0, 10]
    q1 = Query("foo").timeout(0)
    assert q1.get_args() == ["foo", "TIMEOUT", 0, "LIMIT", 0, 10]
    q2 = Query("foo").timeout("not_a_number")
    with pytest.raises(redis.ResponseError):
        r.ft().search(q2)


@pytest.mark.redismod
def test_geoshape(client: redis.Redis):
    client.ft().create_index(GeoShapeField("geom", GeoShapeField.FLAT))
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
    client.hset("small", "geom", "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))")
    client.hset("large", "geom", "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))")
    q1 = Query("@geom:[WITHIN $poly]").dialect(3)
    qp1 = {"poly": "POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))"}
    q2 = Query("@geom:[CONTAINS $poly]").dialect(3)
    qp2 = {"poly": "POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))"}
    result = client.ft().search(q1, query_params=qp1)
    _assert_search_result(client, result, ["small"])
    result = client.ft().search(q2, query_params=qp2)
    _assert_search_result(client, result, ["small", "large"])


@pytest.mark.redismod
def test_search_missing_fields(client):
    definition = IndexDefinition(prefix=["property:"], index_type=IndexType.HASH)

    fields = [
        TextField("title", sortable=True),
        TagField("features", index_missing=True),
        TextField("description", index_missing=True),
    ]

    client.ft().create_index(fields, definition=definition)

    # All fields present
    client.hset(
        "property:1",
        mapping={
            "title": "Luxury Villa in Malibu",
            "features": "pool,sea view,modern",
            "description": "A stunning modern villa overlooking the Pacific Ocean.",
        },
    )

    # Missing features
    client.hset(
        "property:2",
        mapping={
            "title": "Downtown Flat",
            "description": "Modern flat in central Paris with easy access to metro.",
        },
    )

    # Missing description
    client.hset(
        "property:3",
        mapping={
            "title": "Beachfront Bungalow",
            "features": "beachfront,sun deck",
        },
    )

    with pytest.raises(redis.exceptions.ResponseError) as e:
        client.ft().search(
            Query("ismissing(@title)").dialect(2).return_field("id").no_content()
        )
    assert "to be defined with 'INDEXMISSING'" in e.value.args[0]

    res = client.ft().search(
        Query("ismissing(@features)").dialect(2).return_field("id").no_content()
    )
    _assert_search_result(client, res, ["property:2"])

    res = client.ft().search(
        Query("-ismissing(@features)").dialect(2).return_field("id").no_content()
    )
    _assert_search_result(client, res, ["property:1", "property:3"])

    res = client.ft().search(
        Query("ismissing(@description)").dialect(2).return_field("id").no_content()
    )
    _assert_search_result(client, res, ["property:3"])

    res = client.ft().search(
        Query("-ismissing(@description)").dialect(2).return_field("id").no_content()
    )
    _assert_search_result(client, res, ["property:1", "property:2"])


@pytest.mark.redismod
def test_search_empty_fields(client):
    definition = IndexDefinition(prefix=["property:"], index_type=IndexType.HASH)

    fields = [
        TextField("title", sortable=True),
        TagField("features", index_empty=True),
        TextField("description", index_empty=True),
    ]

    client.ft().create_index(fields, definition=definition)

    # All fields present
    client.hset(
        "property:1",
        mapping={
            "title": "Luxury Villa in Malibu",
            "features": "pool,sea view,modern",
            "description": "A stunning modern villa overlooking the Pacific Ocean.",
        },
    )

    # Empty features
    client.hset(
        "property:2",
        mapping={
            "title": "Downtown Flat",
            "features": "",
            "description": "Modern flat in central Paris with easy access to metro.",
        },
    )

    # Empty description
    client.hset(
        "property:3",
        mapping={
            "title": "Beachfront Bungalow",
            "features": "beachfront,sun deck",
            "description": "",
        },
    )

    with pytest.raises(redis.exceptions.ResponseError) as e:
        client.ft().search(
            Query("@title:''").dialect(2).return_field("id").no_content()
        )
    assert "Use `INDEXEMPTY` in field creation" in e.value.args[0]

    res = client.ft().search(
        Query("@features:{$empty}").dialect(2).return_field("id").no_content(),
        query_params={"empty": ""},
    )
    _assert_search_result(client, res, ["property:2"])

    res = client.ft().search(
        Query("-@features:{$empty}").dialect(2).return_field("id").no_content(),
        query_params={"empty": ""},
    )
    _assert_search_result(client, res, ["property:1", "property:3"])

    res = client.ft().search(
        Query("@description:''").dialect(2).return_field("id").no_content()
    )
    _assert_search_result(client, res, ["property:3"])

    res = client.ft().search(
        Query("-@description:''").dialect(2).return_field("id").no_content()
    )
    _assert_search_result(client, res, ["property:1", "property:2"])


@pytest.mark.redismod
def test_special_characters_in_fields(client):
    definition = IndexDefinition(prefix=["resource:"], index_type=IndexType.HASH)

    fields = [
        TagField("uuid"),
        TagField("tags", separator="|"),
        TextField("description"),
        NumericField("rating"),
    ]

    client.ft().create_index(fields, definition=definition)

    client.hset(
        "resource:1",
        mapping={
            "uuid": "123e4567-e89b-12d3-a456-426614174000",
            "tags": "finance|crypto|$btc|blockchain",
            "description": "Analysis of blockchain technologies & Bitcoin's potential.",
            "rating": 5,
        },
    )

    client.hset(
        "resource:2",
        mapping={
            "uuid": "987e6543-e21c-12d3-a456-426614174999",
            "tags": "health|well-being|fitness|new-year's-resolutions",
            "description": "Health trends for the new year, including fitness regimes.",
            "rating": 4,
        },
    )

    # no need to escape - when using params
    res = client.ft().search(
        Query("@uuid:{$uuid}").dialect(2),
        query_params={"uuid": "123e4567-e89b-12d3-a456-426614174000"},
    )
    _assert_search_result(client, res, ["resource:1"])

    # with double quotes exact match no need to escape the - even without params
    res = client.ft().search(
        Query('@uuid:{"123e4567-e89b-12d3-a456-426614174000"}').dialect(2)
    )
    _assert_search_result(client, res, ["resource:1"])

    res = client.ft().search(Query('@tags:{"new-year\'s-resolutions"}').dialect(2))
    _assert_search_result(client, res, ["resource:2"])

    # possible to search numeric fields by single value
    res = client.ft().search(Query("@rating:[4]").dialect(2))
    _assert_search_result(client, res, ["resource:2"])

    # some chars still need escaping
    res = client.ft().search(Query(r"@tags:{\$btc}").dialect(2))
    _assert_search_result(client, res, ["resource:1"])


def _assert_search_result(client, result, expected_doc_ids):
    """
    Make sure the result of a geo search is as expected, taking into account the RESP
    version being used.
    """
    if is_resp2_connection(client):
        assert set([doc.id for doc in result.docs]) == set(expected_doc_ids)
    else:
        assert set([doc["id"] for doc in result["results"]]) == set(expected_doc_ids)
