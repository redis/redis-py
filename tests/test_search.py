import bz2
import csv
import os
import time
from io import TextIOWrapper

import pytest

import redis
import redis.commands.search
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.json.path import Path
from redis.commands.search import Search
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

from .conftest import skip_if_redis_enterprise, skip_ifmodversion_lt

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
            res.index("indexing")
        except ValueError:
            break

        if int(res[res.index("indexing") + 1]) == 0:
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
        indexer.add_document(key, **doc)
    indexer.commit()


@pytest.fixture
def client(modclient):
    modclient.flushdb()
    return modclient


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
    assert isinstance(res, Result)
    assert 225 == res.total
    assert 10 == len(res.docs)
    assert res.duration > 0

    for doc in res.docs:
        assert doc.id
        assert doc.play == "Henry IV"
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
    client.ft().add_document("doc-5ghs2", play="Death of a Salesman")
    res = client.ft().search(Query("death of a salesman"))
    assert 1 == res.total

    assert 1 == client.ft().delete_document("doc-5ghs2")
    res = client.ft().search(Query("death of a salesman"))
    assert 0 == res.total
    assert 0 == client.ft().delete_document("doc-5ghs2")

    client.ft().add_document("doc-5ghs2", play="Death of a Salesman")
    res = client.ft().search(Query("death of a salesman"))
    assert 1 == res.total
    client.ft().delete_document("doc-5ghs2")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.2.0", "search")
def test_payloads(client):
    client.ft().create_index((TextField("txt"),))

    client.ft().add_document("doc1", payload="foo baz", txt="foo bar")
    client.ft().add_document("doc2", txt="foo bar")

    q = Query("foo bar").with_payloads()
    res = client.ft().search(q)
    assert 2 == res.total
    assert "doc1" == res.docs[0].id
    assert "doc2" == res.docs[1].id
    assert "foo baz" == res.docs[0].payload
    assert res.docs[1].payload is None


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_scores(client):
    client.ft().create_index((TextField("txt"),))

    client.ft().add_document("doc1", txt="foo baz")
    client.ft().add_document("doc2", txt="foo bar")

    q = Query("foo ~bar").with_scores()
    res = client.ft().search(q)
    assert 2 == res.total
    assert "doc2" == res.docs[0].id
    assert 3.0 == res.docs[0].score
    assert "doc1" == res.docs[1].id
    # todo: enable once new RS version is tagged
    # self.assertEqual(0.2, res.docs[1].score)


@pytest.mark.redismod
def test_replace(client):
    client.ft().create_index((TextField("txt"),))

    client.ft().add_document("doc1", txt="foo bar")
    client.ft().add_document("doc2", txt="foo bar")
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    res = client.ft().search("foo bar")
    assert 2 == res.total
    client.ft().add_document("doc1", replace=True, txt="this is a replaced doc")

    res = client.ft().search("foo bar")
    assert 1 == res.total
    assert "doc2" == res.docs[0].id

    res = client.ft().search("replaced doc")
    assert 1 == res.total
    assert "doc1" == res.docs[0].id


@pytest.mark.redismod
def test_stopwords(client):
    client.ft().create_index((TextField("txt"),), stopwords=["foo", "bar", "baz"])
    client.ft().add_document("doc1", txt="foo bar")
    client.ft().add_document("doc2", txt="hello world")
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    q1 = Query("foo bar").no_content()
    q2 = Query("foo bar hello world").no_content()
    res1, res2 = client.ft().search(q1), client.ft().search(q2)
    assert 0 == res1.total
    assert 1 == res2.total


@pytest.mark.redismod
def test_filters(client):
    client.ft().create_index((TextField("txt"), NumericField("num"), GeoField("loc")))
    client.ft().add_document("doc1", txt="foo bar", num=3.141, loc="-0.441,51.458")
    client.ft().add_document("doc2", txt="foo baz", num=2, loc="-0.1,51.2")

    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))
    # Test numerical filter
    q1 = Query("foo").add_filter(NumericFilter("num", 0, 2)).no_content()
    q2 = (
        Query("foo")
        .add_filter(NumericFilter("num", 2, NumericFilter.INF, minExclusive=True))
        .no_content()
    )
    res1, res2 = client.ft().search(q1), client.ft().search(q2)

    assert 1 == res1.total
    assert 1 == res2.total
    assert "doc2" == res1.docs[0].id
    assert "doc1" == res2.docs[0].id

    # Test geo filter
    q1 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 10)).no_content()
    q2 = Query("foo").add_filter(GeoFilter("loc", -0.44, 51.45, 100)).no_content()
    res1, res2 = client.ft().search(q1), client.ft().search(q2)

    assert 1 == res1.total
    assert 2 == res2.total
    assert "doc1" == res1.docs[0].id

    # Sort results, after RDB reload order may change
    res = [res2.docs[0].id, res2.docs[1].id]
    res.sort()
    assert ["doc1", "doc2"] == res


@pytest.mark.redismod
def test_payloads_with_no_content(client):
    client.ft().create_index((TextField("txt"),))
    client.ft().add_document("doc1", payload="foo baz", txt="foo bar")
    client.ft().add_document("doc2", payload="foo baz2", txt="foo bar")

    q = Query("foo bar").with_payloads().no_content()
    res = client.ft().search(q)
    assert 2 == len(res.docs)


@pytest.mark.redismod
def test_sort_by(client):
    client.ft().create_index((TextField("txt"), NumericField("num", sortable=True)))
    client.ft().add_document("doc1", txt="foo bar", num=1)
    client.ft().add_document("doc2", txt="foo baz", num=2)
    client.ft().add_document("doc3", txt="foo qux", num=3)

    # Test sort
    q1 = Query("foo").sort_by("num", asc=True).no_content()
    q2 = Query("foo").sort_by("num", asc=False).no_content()
    res1, res2 = client.ft().search(q1), client.ft().search(q2)

    assert 3 == res1.total
    assert "doc1" == res1.docs[0].id
    assert "doc2" == res1.docs[1].id
    assert "doc3" == res1.docs[2].id
    assert 3 == res2.total
    assert "doc1" == res2.docs[2].id
    assert "doc2" == res2.docs[1].id
    assert "doc3" == res2.docs[0].id


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
    client.ft().add_document(
        "doc1",
        title="RediSearch",
        body="Redisearch impements a search engine on top of redis",
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

    client.ft().add_document(
        "doc1", field="aaa", text="1", numeric="1", geo="1,1", tag="1"
    )
    client.ft().add_document(
        "doc2", field="aab", text="2", numeric="2", geo="2,2", tag="2"
    )
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

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
def test_partial(client):
    client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
    client.ft().add_document("doc1", f1="f1_val", f2="f2_val")
    client.ft().add_document("doc2", f1="f1_val", f2="f2_val")
    client.ft().add_document("doc1", f3="f3_val", partial=True)
    client.ft().add_document("doc2", f3="f3_val", replace=True)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    # Search for f3 value. All documents should have it
    res = client.ft().search("@f3:f3_val")
    assert 2 == res.total

    # Only the document updated with PARTIAL should still have f1 and f2 values
    res = client.ft().search("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert 1 == res.total


@pytest.mark.redismod
def test_no_create(client):
    client.ft().create_index((TextField("f1"), TextField("f2"), TextField("f3")))
    client.ft().add_document("doc1", f1="f1_val", f2="f2_val")
    client.ft().add_document("doc2", f1="f1_val", f2="f2_val")
    client.ft().add_document("doc1", f3="f3_val", no_create=True)
    client.ft().add_document("doc2", f3="f3_val", no_create=True, partial=True)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    # Search for f3 value. All documents should have it
    res = client.ft().search("@f3:f3_val")
    assert 2 == res.total

    # Only the document updated with PARTIAL should still have f1 and f2 values
    res = client.ft().search("@f3:f3_val @f2:f2_val @f1:f1_val")
    assert 1 == res.total

    with pytest.raises(redis.ResponseError):
        client.ft().add_document("doc3", f2="f2_val", f3="f3_val", no_create=True)


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

    ftindex2.aliasdel("spaceballs")
    with pytest.raises(Exception):
        alias_client2.search("*").docs[0]


@pytest.mark.redismod
def test_alias_basic(client):
    # Creating a client with one index
    getClient(client).flushdb()
    index1 = getClient(client).ft("testAlias")

    index1.create_index((TextField("txt"),))
    index1.add_document("doc1", txt="text goes here")

    index2 = getClient(client).ft("testAlias2")
    index2.create_index((TextField("txt"),))
    index2.add_document("doc2", txt="text goes here")

    # add the actual alias and check
    index1.aliasadd("myalias")
    alias_client = getClient(client).ft("myalias")
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

    # delete the alias and expect an error if we try to query again
    index2.aliasdel("myalias")
    with pytest.raises(Exception):
        _ = alias_client2.search("*").docs[0]


@pytest.mark.redismod
def test_tags(client):
    client.ft().create_index((TextField("txt"), TagField("tags")))
    tags = "foo,foo bar,hello;world"
    tags2 = "soba,ramen"

    client.ft().add_document("doc1", txt="fooz barz", tags=tags)
    client.ft().add_document("doc2", txt="noodles", tags=tags2)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    q = Query("@tags:{foo}")
    res = client.ft().search(q)
    assert 1 == res.total

    q = Query("@tags:{foo bar}")
    res = client.ft().search(q)
    assert 1 == res.total

    q = Query("@tags:{foo\\ bar}")
    res = client.ft().search(q)
    assert 1 == res.total

    q = Query("@tags:{hello\\;world}")
    res = client.ft().search(q)
    assert 1 == res.total

    q2 = client.ft().tagvals("tags")
    assert (tags.split(",") + tags2.split(",")).sort() == q2.sort()


@pytest.mark.redismod
def test_textfield_sortable_nostem(client):
    # Creating the index definition with sortable and no_stem
    client.ft().create_index((TextField("txt", sortable=True, no_stem=True),))

    # Now get the index info to confirm its contents
    response = client.ft().info()
    assert "SORTABLE" in response["attributes"][0]
    assert "NOSTEM" in response["attributes"][0]


@pytest.mark.redismod
def test_alter_schema_add(client):
    # Creating the index definition and schema
    client.ft().create_index(TextField("title"))

    # Using alter to add a field
    client.ft().alter_schema_add(TextField("body"))

    # Indexing a document
    client.ft().add_document(
        "doc1", title="MyTitle", body="Some content only in the body"
    )

    # Searching with parameter only in the body (the added field)
    q = Query("only in the body")

    # Ensure we find the result searching on the added body field
    res = client.ft().search(q)
    assert 1 == res.total


@pytest.mark.redismod
def test_spell_check(client):
    client.ft().create_index((TextField("f1"), TextField("f2")))

    client.ft().add_document("doc1", f1="some valid content", f2="this is sample text")
    client.ft().add_document("doc2", f1="very important", f2="lorem ipsum")
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

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
    assert ["item1", "item3"] == res

    # Remove rest of the items before reload
    client.ft().dict_del("custom_dict", *res)


@pytest.mark.redismod
def test_phonetic_matcher(client):
    client.ft().create_index((TextField("name"),))
    client.ft().add_document("doc1", name="Jon")
    client.ft().add_document("doc2", name="John")

    res = client.ft().search(Query("Jon"))
    assert 1 == len(res.docs)
    assert "Jon" == res.docs[0].name

    # Drop and create index with phonetic matcher
    client.flushdb()

    client.ft().create_index((TextField("name", phonetic_matcher="dm:en"),))
    client.ft().add_document("doc1", name="Jon")
    client.ft().add_document("doc2", name="John")

    res = client.ft().search(Query("Jon"))
    assert 2 == len(res.docs)
    assert ["John", "Jon"] == sorted(d.name for d in res.docs)


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_scorer(client):
    client.ft().create_index((TextField("description"),))

    client.ft().add_document(
        "doc1", description="The quick brown fox jumps over the lazy dog"
    )
    client.ft().add_document(
        "doc2",
        description="Quick alice was beginning to get very tired of sitting by her quick sister on the bank, and of having nothing to do.",  # noqa
    )

    # default scorer is TFIDF
    res = client.ft().search(Query("quick").with_scores())
    assert 1.0 == res.docs[0].score
    res = client.ft().search(Query("quick").scorer("TFIDF").with_scores())
    assert 1.0 == res.docs[0].score
    res = client.ft().search(Query("quick").scorer("TFIDF.DOCNORM").with_scores())
    assert 0.1111111111111111 == res.docs[0].score
    res = client.ft().search(Query("quick").scorer("BM25").with_scores())
    assert 0.17699114465425977 == res.docs[0].score
    res = client.ft().search(Query("quick").scorer("DISMAX").with_scores())
    assert 2.0 == res.docs[0].score
    res = client.ft().search(Query("quick").scorer("DOCSCORE").with_scores())
    assert 1.0 == res.docs[0].score
    res = client.ft().search(Query("quick").scorer("HAMMING").with_scores())
    assert 0.0 == res.docs[0].score


@pytest.mark.redismod
def test_get(client):
    client.ft().create_index((TextField("f1"), TextField("f2")))

    assert [None] == client.ft().get("doc1")
    assert [None, None] == client.ft().get("doc2", "doc1")

    client.ft().add_document(
        "doc1", f1="some valid content dd1", f2="this is sample text ff1"
    )
    client.ft().add_document(
        "doc2", f1="some valid content dd2", f2="this is sample text ff2"
    )

    assert [
        ["f1", "some valid content dd2", "f2", "this is sample text ff2"]
    ] == client.ft().get("doc2")
    assert [
        ["f1", "some valid content dd1", "f2", "this is sample text ff1"],
        ["f1", "some valid content dd2", "f2", "this is sample text ff2"],
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
    client.ft().add_document(
        "search",
        title="RediSearch",
        body="Redisearch impements a search engine on top of redis",
        parent="redis",
        random_num=10,
    )
    client.ft().add_document(
        "ai",
        title="RedisAI",
        body="RedisAI executes Deep Learning/Machine Learning models and managing their data.",  # noqa
        parent="redis",
        random_num=3,
    )
    client.ft().add_document(
        "json",
        title="RedisJson",
        body="RedisJSON implements ECMA-404 The JSON Data Interchange Standard as a native data type.",  # noqa
        parent="redis",
        random_num=8,
    )

    req = aggregations.AggregateRequest("redis").group_by("@parent", reducers.count())

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


@pytest.mark.redismod
def test_aggregations_sort_by_and_limit(client):
    client.ft().create_index((TextField("t1"), TextField("t2")))

    client.ft().client.hset("doc1", mapping={"t1": "a", "t2": "b"})
    client.ft().client.hset("doc2", mapping={"t1": "b", "t2": "a"})

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


@pytest.mark.redismod
def test_aggregations_load(client):
    client.ft().create_index((TextField("t1"), TextField("t2")))

    client.ft().client.hset("doc1", mapping={"t1": "hello", "t2": "world"})

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
    res_set = set([res.rows[0][1], res.rows[1][1]])
    assert res_set == set(["6373878785249699840", "6373878758592700416"])


@pytest.mark.redismod
def test_aggregations_filter(client):
    client.ft().create_index(
        (TextField("name", sortable=True), NumericField("age", sortable=True))
    )

    client.ft().client.hset("doc1", mapping={"name": "bar", "age": "25"})
    client.ft().client.hset("doc2", mapping={"name": "foo", "age": "19"})

    req = aggregations.AggregateRequest("*").filter("@name=='foo' && @age < 20")
    res = client.ft().aggregate(req)
    assert len(res.rows) == 1
    assert res.rows[0] == ["name", "foo", "age", "19"]

    req = aggregations.AggregateRequest("*").filter("@age > 15").sort_by("@age")
    res = client.ft().aggregate(req)
    assert len(res.rows) == 2
    assert res.rows[0] == ["age", "19"]
    assert res.rows[1] == ["age", "25"]


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
def testExpire(client):
    client.ft().create_index((TextField("txt", sortable=True),), temporary=4)
    ttl = client.execute_command("ft.debug", "TTL", "idx")
    assert ttl > 2

    while ttl > 2:
        ttl = client.execute_command("ft.debug", "TTL", "idx")
        time.sleep(0.01)

    # add document - should reset the ttl
    client.ft().add_document("doc", txt="foo bar", text="this is a simple test")
    ttl = client.execute_command("ft.debug", "TTL", "idx")
    assert ttl > 2
    try:
        while True:
            ttl = client.execute_command("ft.debug", "TTL", "idx")
            time.sleep(0.5)
    except redis.exceptions.ResponseError:
        assert ttl == 0


@pytest.mark.redismod
def testSkipInitialScan(client):
    client.hset("doc1", "foo", "bar")
    q = Query("@foo:bar")

    client.ft().create_index((TextField("foo"),), skip_initial_scan=True)
    assert 0 == client.ft().search(q).total


@pytest.mark.redismod
def testSummarizeDisabled_nooffset(client):
    client.ft().create_index((TextField("txt"),), no_term_offsets=True)
    client.ft().add_document("doc1", txt="foo bar")
    with pytest.raises(Exception):
        client.ft().search(Query("foo").summarize(fields=["txt"]))


@pytest.mark.redismod
def testSummarizeDisabled_nohl(client):
    client.ft().create_index((TextField("txt"),), no_highlight=True)
    client.ft().add_document("doc1", txt="foo bar")
    with pytest.raises(Exception):
        client.ft().search(Query("foo").summarize(fields=["txt"]))


@pytest.mark.redismod
def testMaxTextFields(client):
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
    assert res.docs[0].id == "king:1"
    assert res.docs[0].payload is None
    assert res.docs[0].json == '{"name":"henry"}'
    assert res.total == 1


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

    total = client.ft().search(Query("Jon").return_fields("name", "just_a_number")).docs
    assert 1 == len(total)
    assert "doc:1" == total[0].id
    assert "Jon" == total[0].name
    assert "25" == total[0].just_a_number


@pytest.mark.redismod
def test_casesensitive(client):
    # create index
    SCHEMA = (TagField("t", case_sensitive=False),)
    client.ft().create_index(SCHEMA)
    client.ft().client.hset("1", "t", "HELLO")
    client.ft().client.hset("2", "t", "hello")

    res = client.ft().search("@t:{HELLO}").docs

    assert 2 == len(res)
    assert "1" == res[0].id
    assert "2" == res[1].id

    # create casesensitive index
    client.ft().dropindex()
    SCHEMA = (TagField("t", case_sensitive=True),)
    client.ft().create_index(SCHEMA)
    waitForIndex(client, getattr(client.ft(), "index_name", "idx"))

    res = client.ft().search("@t:{HELLO}").docs
    assert 1 == len(res)
    assert "1" == res[0].id


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

    total = client.ft().search(Query("*").return_field("$.t", as_field="txt")).docs
    assert 1 == len(total)
    assert "doc:1" == total[0].id
    assert "riceratops" == total[0].txt

    total = client.ft().search(Query("*").return_field("$.t2", as_field="txt")).docs
    assert 1 == len(total)
    assert "doc:1" == total[0].id
    assert "telmatosaurus" == total[0].txt


@pytest.mark.redismod
def test_synupdate(client):
    definition = IndexDefinition(index_type=IndexType.HASH)
    client.ft().create_index(
        (TextField("title"), TextField("body")), definition=definition
    )

    client.ft().synupdate("id1", True, "boy", "child", "offspring")
    client.ft().add_document("doc1", title="he is a baby", body="this is a test")

    client.ft().synupdate("id1", True, "baby")
    client.ft().add_document("doc2", title="he is another baby", body="another test")

    res = client.ft().search(Query("child").expander("SYNONYM"))
    assert res.docs[0].id == "doc2"
    assert res.docs[0].title == "he is another baby"
    assert res.docs[0].body == "another test"


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

    res = client.ft().search("@name:henry")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].json == '{"name":"henry","num":42}'
    assert res.total == 1

    res = client.ft().search("@num:[0 10]")
    assert res.docs[0].id == "king:2"
    assert res.docs[0].json == '{"name":"james","num":3.14}'
    assert res.total == 1

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

    res = client.ft().search("@name:{henry}")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
    assert res.total == 1

    res = client.ft().search("@name:{england}")
    assert res.docs[0].id == "king:1"
    assert res.docs[0].json == '{"name":"henry","country":{"name":"england"}}'
    assert res.total == 1


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

    # query for a supported field succeeds
    res = client.ft().search(Query("@name:RediSearch"))
    assert res.total == 1
    assert res.docs[0].id == "doc:1"
    assert res.docs[0].json == '{"prod:name":"RediSearch"}'

    # query for an unsupported field fails
    res = client.ft().search("@name_unsupported:RediSearch")
    assert res.total == 0

    # return of a supported field succeeds
    res = client.ft().search(Query("@name:RediSearch").return_field("name"))
    assert res.total == 1
    assert res.docs[0].id == "doc:1"
    assert res.docs[0].name == "RediSearch"

    # return of an unsupported field fails
    res = client.ft().search(Query("@name:RediSearch").return_field("name_unsupported"))
    assert res.total == 1
    assert res.docs[0].id == "doc:1"
    with pytest.raises(Exception):
        res.docs[0].name_unsupported


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_if_redis_enterprise()
def test_profile(client):
    client.ft().create_index((TextField("t"),))
    client.ft().client.hset("1", "t", "hello")
    client.ft().client.hset("2", "t", "world")

    # check using Query
    q = Query("hello|world").no_content()
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
    assert det["Iterators profile"]["Counter"] == 2.0
    assert det["Iterators profile"]["Type"] == "WILDCARD"
    assert isinstance(det["Parsing time"], float)
    assert len(res.rows) == 2  # check also the search result


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_profile_limited(client):
    client.ft().create_index((TextField("t"),))
    client.ft().client.hset("1", "t", "hello")
    client.ft().client.hset("2", "t", "hell")
    client.ft().client.hset("3", "t", "help")
    client.ft().client.hset("4", "t", "helowa")

    q = Query("%hell% hel*")
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


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_profile_query_params(modclient: redis.Redis):
    modclient.flushdb()
    modclient.ft().create_index(
        (
            VectorField(
                "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
            ),
        )
    )
    modclient.hset("a", "v", "aaaaaaaa")
    modclient.hset("b", "v", "aaaabaaa")
    modclient.hset("c", "v", "aaaaabaa")
    query = "*=>[KNN 2 @v $vec]"
    q = Query(query).return_field("__v_score").sort_by("__v_score", True).dialect(2)
    res, det = modclient.ft().profile(q, query_params={"vec": "aaaaaaaa"})
    assert det["Iterators profile"]["Counter"] == 2.0
    assert det["Iterators profile"]["Type"] == "VECTOR"
    assert res.total == 2
    assert "a" == res.docs[0].id
    assert "0" == res.docs[0].__getattribute__("__v_score")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_vector_field(modclient):
    modclient.flushdb()
    modclient.ft().create_index(
        (
            VectorField(
                "v", "HNSW", {"TYPE": "FLOAT32", "DIM": 2, "DISTANCE_METRIC": "L2"}
            ),
        )
    )
    modclient.hset("a", "v", "aaaaaaaa")
    modclient.hset("b", "v", "aaaabaaa")
    modclient.hset("c", "v", "aaaaabaa")

    query = "*=>[KNN 2 @v $vec]"
    q = Query(query).return_field("__v_score").sort_by("__v_score", True).dialect(2)
    res = modclient.ft().search(q, query_params={"vec": "aaaaaaaa"})

    assert "a" == res.docs[0].id
    assert "0" == res.docs[0].__getattribute__("__v_score")


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_vector_field_error(modclient):
    modclient.flushdb()

    # sortable tag
    with pytest.raises(Exception):
        modclient.ft().create_index((VectorField("v", "HNSW", {}, sortable=True),))

    # not supported algorithm
    with pytest.raises(Exception):
        modclient.ft().create_index((VectorField("v", "SORT", {}),))


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_text_params(modclient):
    modclient.flushdb()
    modclient.ft().create_index((TextField("name"),))

    modclient.ft().add_document("doc1", name="Alice")
    modclient.ft().add_document("doc2", name="Bob")
    modclient.ft().add_document("doc3", name="Carol")

    params_dict = {"name1": "Alice", "name2": "Bob"}
    q = Query("@name:($name1 | $name2 )").dialect(2)
    res = modclient.ft().search(q, query_params=params_dict)
    assert 2 == res.total
    assert "doc1" == res.docs[0].id
    assert "doc2" == res.docs[1].id


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_numeric_params(modclient):
    modclient.flushdb()
    modclient.ft().create_index((NumericField("numval"),))

    modclient.ft().add_document("doc1", numval=101)
    modclient.ft().add_document("doc2", numval=102)
    modclient.ft().add_document("doc3", numval=103)

    params_dict = {"min": 101, "max": 102}
    q = Query("@numval:[$min $max]").dialect(2)
    res = modclient.ft().search(q, query_params=params_dict)

    assert 2 == res.total
    assert "doc1" == res.docs[0].id
    assert "doc2" == res.docs[1].id


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_geo_params(modclient):

    modclient.flushdb()
    modclient.ft().create_index((GeoField("g")))
    modclient.ft().add_document("doc1", g="29.69465, 34.95126")
    modclient.ft().add_document("doc2", g="29.69350, 34.94737")
    modclient.ft().add_document("doc3", g="29.68746, 34.94882")

    params_dict = {"lat": "34.95126", "lon": "29.69465", "radius": 1000, "units": "km"}
    q = Query("@g:[$lon $lat $radius $units]").dialect(2)
    res = modclient.ft().search(q, query_params=params_dict)
    assert 3 == res.total
    assert "doc1" == res.docs[0].id
    assert "doc2" == res.docs[1].id
    assert "doc3" == res.docs[2].id


@pytest.mark.redismod
@skip_if_redis_enterprise()
def test_search_commands_in_pipeline(client):
    p = client.ft().pipeline()
    p.create_index((TextField("txt"),))
    p.add_document("doc1", payload="foo baz", txt="foo bar")
    p.add_document("doc2", txt="foo bar")
    q = Query("foo bar").with_payloads()
    p.search(q)
    res = p.execute()
    assert res[:3] == ["OK", "OK", "OK"]
    assert 2 == res[3][0]
    assert "doc1" == res[3][1]
    assert "doc2" == res[3][4]
    assert "foo baz" == res[3][2]
    assert res[3][5] is None
    assert res[3][3] == res[3][6] == ["txt", "foo bar"]


@pytest.mark.redismod
@pytest.mark.onlynoncluster
@skip_ifmodversion_lt("2.4.3", "search")
def test_dialect_config(modclient: redis.Redis):
    assert modclient.ft().config_get("DEFAULT_DIALECT") == {"DEFAULT_DIALECT": "1"}
    assert modclient.ft().config_set("DEFAULT_DIALECT", 2)
    assert modclient.ft().config_get("DEFAULT_DIALECT") == {"DEFAULT_DIALECT": "2"}
    with pytest.raises(redis.ResponseError):
        modclient.ft().config_set("DEFAULT_DIALECT", 0)


@pytest.mark.redismod
@skip_ifmodversion_lt("2.4.3", "search")
def test_dialect(modclient: redis.Redis):
    modclient.ft().create_index(
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
    modclient.hset("h", "t1", "hello")
    with pytest.raises(redis.ResponseError) as err:
        modclient.ft().explain(Query("(*)").dialect(1))
    assert "Syntax error" in str(err)
    assert "WILDCARD" in modclient.ft().explain(Query("(*)").dialect(2))

    with pytest.raises(redis.ResponseError) as err:
        modclient.ft().explain(Query("$hello").dialect(1))
    assert "Syntax error" in str(err)
    q = Query("$hello").dialect(2)
    expected = "UNION {\n  hello\n  +hello(expanded)\n}\n"
    assert expected in modclient.ft().explain(q, query_params={"hello": "hello"})

    expected = "NUMERIC {0.000000 <= @num <= 10.000000}\n"
    assert expected in modclient.ft().explain(Query("@title:(@num:[0 10])").dialect(1))
    with pytest.raises(redis.ResponseError) as err:
        modclient.ft().explain(Query("@title:(@num:[0 10])").dialect(2))
    assert "Syntax error" in str(err)


@pytest.mark.redismod
def test_expire_while_search(modclient: redis.Redis):
    modclient.ft().create_index((TextField("txt"),))
    modclient.hset("hset:1", "txt", "a")
    modclient.hset("hset:2", "txt", "b")
    modclient.hset("hset:3", "txt", "c")
    assert 3 == modclient.ft().search(Query("*")).total
    modclient.pexpire("hset:2", 300)
    for _ in range(500):
        modclient.ft().search(Query("*")).docs[1]
    time.sleep(1)
    assert 2 == modclient.ft().search(Query("*")).total


@pytest.mark.redismod
@pytest.mark.experimental
def test_withsuffixtrie(modclient: redis.Redis):
    # create index
    assert modclient.ft().create_index((TextField("txt"),))
    waitForIndex(modclient, getattr(modclient.ft(), "index_name", "idx"))
    info = modclient.ft().info()
    assert "WITHSUFFIXTRIE" not in info["attributes"][0]
    assert modclient.ft().dropindex("idx")

    # create withsuffixtrie index (text fiels)
    assert modclient.ft().create_index((TextField("t", withsuffixtrie=True)))
    waitForIndex(modclient, getattr(modclient.ft(), "index_name", "idx"))
    info = modclient.ft().info()
    assert "WITHSUFFIXTRIE" in info["attributes"][0]
    assert modclient.ft().dropindex("idx")

    # create withsuffixtrie index (tag field)
    assert modclient.ft().create_index((TagField("t", withsuffixtrie=True)))
    waitForIndex(modclient, getattr(modclient.ft(), "index_name", "idx"))
    info = modclient.ft().info()
    assert "WITHSUFFIXTRIE" in info["attributes"][0]
