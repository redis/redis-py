from unittest.mock import patch

import pytest

from redis.commands.graph import Edge, Node, Path
from redis.commands.graph.execution_plan import Operation
from redis.commands.graph.query_result import (
    CACHED_EXECUTION,
    INDICES_CREATED,
    INDICES_DELETED,
    INTERNAL_EXECUTION_TIME,
    LABELS_ADDED,
    LABELS_REMOVED,
    NODES_CREATED,
    NODES_DELETED,
    PROPERTIES_REMOVED,
    PROPERTIES_SET,
    RELATIONSHIPS_CREATED,
    RELATIONSHIPS_DELETED,
    QueryResult,
)
from redis.exceptions import ResponseError
from tests.conftest import skip_if_redis_enterprise


@pytest.fixture
def client(modclient):
    modclient.flushdb()
    return modclient


@pytest.mark.redismod
def test_bulk(client):
    with pytest.raises(NotImplementedError):
        client.graph().bulk()
        client.graph().bulk(foo="bar!")


@pytest.mark.redismod
def test_graph_creation(client):
    graph = client.graph()

    john = Node(
        label="person",
        properties={
            "name": "John Doe",
            "age": 33,
            "gender": "male",
            "status": "single",
        },
    )
    graph.add_node(john)
    japan = Node(label="country", properties={"name": "Japan"})

    graph.add_node(japan)
    edge = Edge(john, "visited", japan, properties={"purpose": "pleasure"})
    graph.add_edge(edge)

    graph.commit()

    query = (
        'MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country) '
        "RETURN p, v, c"
    )

    result = graph.query(query)

    person = result.result_set[0][0]
    visit = result.result_set[0][1]
    country = result.result_set[0][2]

    assert person == john
    assert visit.properties == edge.properties
    assert country == japan

    query = """RETURN [1, 2.3, "4", true, false, null]"""
    result = graph.query(query)
    assert [1, 2.3, "4", True, False, None] == result.result_set[0][0]

    # All done, remove graph.
    graph.delete()


@pytest.mark.redismod
def test_array_functions(client):
    query = """CREATE (p:person{name:'a',age:32, array:[0,1,2]})"""
    client.graph().query(query)

    query = """WITH [0,1,2] as x return x"""
    result = client.graph().query(query)
    assert [0, 1, 2] == result.result_set[0][0]

    query = """MATCH(n) return collect(n)"""
    result = client.graph().query(query)

    a = Node(
        node_id=0,
        label="person",
        properties={"name": "a", "age": 32, "array": [0, 1, 2]},
    )

    assert [a] == result.result_set[0][0]


@pytest.mark.redismod
def test_path(client):
    node0 = Node(node_id=0, label="L1")
    node1 = Node(node_id=1, label="L1")
    edge01 = Edge(node0, "R1", node1, edge_id=0, properties={"value": 1})

    graph = client.graph()
    graph.add_node(node0)
    graph.add_node(node1)
    graph.add_edge(edge01)
    graph.flush()

    path01 = Path.new_empty_path().add_node(node0).add_edge(edge01).add_node(node1)
    expected_results = [[path01]]

    query = "MATCH p=(:L1)-[:R1]->(:L1) RETURN p ORDER BY p"
    result = graph.query(query)
    assert expected_results == result.result_set


@pytest.mark.redismod
def test_param(client):
    params = [1, 2.3, "str", True, False, None, [0, 1, 2]]
    query = "RETURN $param"
    for param in params:
        result = client.graph().query(query, {"param": param})
        expected_results = [[param]]
        assert expected_results == result.result_set


@pytest.mark.redismod
def test_map(client):
    query = "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}"

    actual = client.graph().query(query).result_set[0][0]
    expected = {
        "a": 1,
        "b": "str",
        "c": None,
        "d": [1, 2, 3],
        "e": True,
        "f": {"x": 1, "y": 2},
    }

    assert actual == expected


@pytest.mark.redismod
def test_point(client):
    query = "RETURN point({latitude: 32.070794860, longitude: 34.820751118})"
    expected_lat = 32.070794860
    expected_lon = 34.820751118
    actual = client.graph().query(query).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    query = "RETURN point({latitude: 32, longitude: 34.0})"
    expected_lat = 32
    expected_lon = 34
    actual = client.graph().query(query).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001


@pytest.mark.redismod
def test_index_response(client):
    result_set = client.graph().query("CREATE INDEX ON :person(age)")
    assert 1 == result_set.indices_created

    result_set = client.graph().query("CREATE INDEX ON :person(age)")
    assert 0 == result_set.indices_created

    result_set = client.graph().query("DROP INDEX ON :person(age)")
    assert 1 == result_set.indices_deleted

    with pytest.raises(ResponseError):
        client.graph().query("DROP INDEX ON :person(age)")


@pytest.mark.redismod
def test_stringify_query_result(client):
    graph = client.graph()

    john = Node(
        alias="a",
        label="person",
        properties={
            "name": "John Doe",
            "age": 33,
            "gender": "male",
            "status": "single",
        },
    )
    graph.add_node(john)

    japan = Node(alias="b", label="country", properties={"name": "Japan"})
    graph.add_node(japan)

    edge = Edge(john, "visited", japan, properties={"purpose": "pleasure"})
    graph.add_edge(edge)

    assert (
        str(john)
        == """(a:person{age:33,gender:"male",name:"John Doe",status:"single"})"""  # noqa
    )
    assert (
        str(edge)
        == """(a:person{age:33,gender:"male",name:"John Doe",status:"single"})"""  # noqa
        + """-[:visited{purpose:"pleasure"}]->"""
        + """(b:country{name:"Japan"})"""
    )
    assert str(japan) == """(b:country{name:"Japan"})"""

    graph.commit()

    query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
            RETURN p, v, c"""

    result = client.graph().query(query)
    person = result.result_set[0][0]
    visit = result.result_set[0][1]
    country = result.result_set[0][2]

    assert (
        str(person)
        == """(:person{age:33,gender:"male",name:"John Doe",status:"single"})"""  # noqa
    )
    assert str(visit) == """()-[:visited{purpose:"pleasure"}]->()"""
    assert str(country) == """(:country{name:"Japan"})"""

    graph.delete()


@pytest.mark.redismod
def test_optional_match(client):
    # Build a graph of form (a)-[R]->(b)
    node0 = Node(node_id=0, label="L1", properties={"value": "a"})
    node1 = Node(node_id=1, label="L1", properties={"value": "b"})

    edge01 = Edge(node0, "R", node1, edge_id=0)

    graph = client.graph()
    graph.add_node(node0)
    graph.add_node(node1)
    graph.add_edge(edge01)
    graph.flush()

    # Issue a query that collects all outgoing edges from both nodes
    # (the second has none)
    query = """MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY a.value"""  # noqa
    expected_results = [[node0, edge01, node1], [node1, None, None]]

    result = client.graph().query(query)
    assert expected_results == result.result_set

    graph.delete()


@pytest.mark.redismod
def test_cached_execution(client):
    client.graph().query("CREATE ()")

    uncached_result = client.graph().query("MATCH (n) RETURN n, $param", {"param": [0]})
    assert uncached_result.cached_execution is False

    # loop to make sure the query is cached on each thread on server
    for x in range(0, 64):
        cached_result = client.graph().query(
            "MATCH (n) RETURN n, $param", {"param": [0]}
        )
        assert uncached_result.result_set == cached_result.result_set

    # should be cached on all threads by now
    assert cached_result.cached_execution


@pytest.mark.redismod
def test_slowlog(client):
    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
    (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
    (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    client.graph().query(create_query)

    results = client.graph().slowlog()
    assert results[0][1] == "GRAPH.QUERY"
    assert results[0][2] == create_query


@pytest.mark.redismod
def test_query_timeout(client):
    # Build a sample graph with 1000 nodes.
    client.graph().query("UNWIND range(0,1000) as val CREATE ({v: val})")
    # Issue a long-running query with a 1-millisecond timeout.
    with pytest.raises(ResponseError):
        client.graph().query("MATCH (a), (b), (c), (d) RETURN *", timeout=1)
        assert False is False

    with pytest.raises(Exception):
        client.graph().query("RETURN 1", timeout="str")
        assert False is False


@pytest.mark.redismod
def test_read_only_query(client):
    with pytest.raises(Exception):
        # Issue a write query, specifying read-only true,
        # this call should fail.
        client.graph().query("CREATE (p:person {name:'a'})", read_only=True)
        assert False is False


@pytest.mark.redismod
def test_profile(client):
    q = """UNWIND range(1, 3) AS x CREATE (p:Person {v:x})"""
    profile = client.graph().profile(q).result_set
    assert "Create | Records produced: 3" in profile
    assert "Unwind | Records produced: 3" in profile

    q = "MATCH (p:Person) WHERE p.v > 1 RETURN p"
    profile = client.graph().profile(q).result_set
    assert "Results | Records produced: 2" in profile
    assert "Project | Records produced: 2" in profile
    assert "Filter | Records produced: 2" in profile
    assert "Node By Label Scan | (p:Person) | Records produced: 3" in profile


@pytest.mark.redismod
@skip_if_redis_enterprise()
def test_config(client):
    config_name = "RESULTSET_SIZE"
    config_value = 3

    # Set configuration
    response = client.graph().config(config_name, config_value, set=True)
    assert response == "OK"

    # Make sure config been updated.
    response = client.graph().config(config_name, set=False)
    expected_response = [config_name, config_value]
    assert response == expected_response

    config_name = "QUERY_MEM_CAPACITY"
    config_value = 1 << 20  # 1MB

    # Set configuration
    response = client.graph().config(config_name, config_value, set=True)
    assert response == "OK"

    # Make sure config been updated.
    response = client.graph().config(config_name, set=False)
    expected_response = [config_name, config_value]
    assert response == expected_response

    # reset to default
    client.graph().config("QUERY_MEM_CAPACITY", 0, set=True)
    client.graph().config("RESULTSET_SIZE", -100, set=True)


@pytest.mark.redismod
@pytest.mark.onlynoncluster
def test_list_keys(client):
    result = client.graph().list_keys()
    assert result == []

    client.graph("G").query("CREATE (n)")
    result = client.graph().list_keys()
    assert result == ["G"]

    client.graph("X").query("CREATE (m)")
    result = client.graph().list_keys()
    assert result == ["G", "X"]

    client.delete("G")
    client.rename("X", "Z")
    result = client.graph().list_keys()
    assert result == ["Z"]

    client.delete("Z")
    result = client.graph().list_keys()
    assert result == []


@pytest.mark.redismod
def test_multi_label(client):
    redis_graph = client.graph("g")

    node = Node(label=["l", "ll"])
    redis_graph.add_node(node)
    redis_graph.commit()

    query = "MATCH (n) RETURN n"
    result = redis_graph.query(query)
    result_node = result.result_set[0][0]
    assert result_node == node

    try:
        Node(label=1)
        assert False
    except AssertionError:
        assert True

    try:
        Node(label=["l", 1])
        assert False
    except AssertionError:
        assert True


@pytest.mark.redismod
def test_cache_sync(client):
    pass
    return
    # This test verifies that client internal graph schema cache stays
    # in sync with the graph schema
    #
    # Client B will try to get Client A out of sync by:
    # 1. deleting the graph
    # 2. reconstructing the graph in a different order, this will casuse
    #    a differance in the current mapping between string IDs and the
    #    mapping Client A is aware of
    #
    # Client A should pick up on the changes by comparing graph versions
    # and resyncing its cache.

    A = client.graph("cache-sync")
    B = client.graph("cache-sync")

    # Build order:
    # 1. introduce label 'L' and 'K'
    # 2. introduce attribute 'x' and 'q'
    # 3. introduce relationship-type 'R' and 'S'

    A.query("CREATE (:L)")
    B.query("CREATE (:K)")
    A.query("MATCH (n) SET n.x = 1")
    B.query("MATCH (n) SET n.q = 1")
    A.query("MATCH (n) CREATE (n)-[:R]->()")
    B.query("MATCH (n) CREATE (n)-[:S]->()")

    # Cause client A to populate its cache
    A.query("MATCH (n)-[e]->() RETURN n, e")

    assert len(A._labels) == 2
    assert len(A._properties) == 2
    assert len(A._relationship_types) == 2
    assert A._labels[0] == "L"
    assert A._labels[1] == "K"
    assert A._properties[0] == "x"
    assert A._properties[1] == "q"
    assert A._relationship_types[0] == "R"
    assert A._relationship_types[1] == "S"

    # Have client B reconstruct the graph in a different order.
    B.delete()

    # Build order:
    # 1. introduce relationship-type 'R'
    # 2. introduce label 'L'
    # 3. introduce attribute 'x'
    B.query("CREATE ()-[:S]->()")
    B.query("CREATE ()-[:R]->()")
    B.query("CREATE (:K)")
    B.query("CREATE (:L)")
    B.query("MATCH (n) SET n.q = 1")
    B.query("MATCH (n) SET n.x = 1")

    # A's internal cached mapping is now out of sync
    # issue a query and make sure A's cache is synced.
    A.query("MATCH (n)-[e]->() RETURN n, e")

    assert len(A._labels) == 2
    assert len(A._properties) == 2
    assert len(A._relationship_types) == 2
    assert A._labels[0] == "K"
    assert A._labels[1] == "L"
    assert A._properties[0] == "q"
    assert A._properties[1] == "x"
    assert A._relationship_types[0] == "S"
    assert A._relationship_types[1] == "R"


@pytest.mark.redismod
def test_execution_plan(client):
    redis_graph = client.graph("execution_plan")
    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
    (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
    (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    redis_graph.query(create_query)

    result = redis_graph.execution_plan(
        "MATCH (r:Rider)-[:rides]->(t:Team) WHERE t.name = $name RETURN r.name, t.name, $params",  # noqa
        {"name": "Yehuda"},
    )
    expected = "Results\n    Project\n        Conditional Traverse | (t:Team)->(r:Rider)\n            Filter\n                Node By Label Scan | (t:Team)"  # noqa
    assert result == expected

    redis_graph.delete()


@pytest.mark.redismod
def test_explain(client):
    redis_graph = client.graph("execution_plan")
    # graph creation / population
    create_query = """CREATE
(:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
(:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
(:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    redis_graph.query(create_query)

    result = redis_graph.explain(
        """MATCH (r:Rider)-[:rides]->(t:Team)
WHERE t.name = $name
RETURN r.name, t.name
UNION
MATCH (r:Rider)-[:rides]->(t:Team)
WHERE t.name = $name
RETURN r.name, t.name""",
        {"name": "Yamaha"},
    )
    expected = """\
Results
Distinct
    Join
        Project
            Conditional Traverse | (t:Team)->(r:Rider)
                Filter
                    Node By Label Scan | (t:Team)
        Project
            Conditional Traverse | (t:Team)->(r:Rider)
                Filter
                    Node By Label Scan | (t:Team)"""
    assert str(result).replace(" ", "").replace("\n", "") == expected.replace(
        " ", ""
    ).replace("\n", "")

    expected = Operation("Results").append_child(
        Operation("Distinct").append_child(
            Operation("Join")
            .append_child(
                Operation("Project").append_child(
                    Operation(
                        "Conditional Traverse", "(t:Team)->(r:Rider)"
                    ).append_child(
                        Operation("Filter").append_child(
                            Operation("Node By Label Scan", "(t:Team)")
                        )
                    )
                )
            )
            .append_child(
                Operation("Project").append_child(
                    Operation(
                        "Conditional Traverse", "(t:Team)->(r:Rider)"
                    ).append_child(
                        Operation("Filter").append_child(
                            Operation("Node By Label Scan", "(t:Team)")
                        )
                    )
                )
            )
        )
    )

    assert result.structured_plan == expected

    result = redis_graph.explain(
        """MATCH (r:Rider), (t:Team)
                                    RETURN r.name, t.name"""
    )
    expected = """\
Results
Project
    Cartesian Product
        Node By Label Scan | (r:Rider)
        Node By Label Scan | (t:Team)"""
    assert str(result).replace(" ", "").replace("\n", "") == expected.replace(
        " ", ""
    ).replace("\n", "")

    expected = Operation("Results").append_child(
        Operation("Project").append_child(
            Operation("Cartesian Product")
            .append_child(Operation("Node By Label Scan"))
            .append_child(Operation("Node By Label Scan"))
        )
    )

    assert result.structured_plan == expected

    redis_graph.delete()


@pytest.mark.redismod
def test_resultset_statistics(client):
    with patch.object(target=QueryResult, attribute="_get_stat") as mock_get_stats:
        result = client.graph().query("RETURN 1")
        result.labels_added
        mock_get_stats.assert_called_with(LABELS_ADDED)
        result.labels_removed
        mock_get_stats.assert_called_with(LABELS_REMOVED)
        result.nodes_created
        mock_get_stats.assert_called_with(NODES_CREATED)
        result.nodes_deleted
        mock_get_stats.assert_called_with(NODES_DELETED)
        result.properties_set
        mock_get_stats.assert_called_with(PROPERTIES_SET)
        result.properties_removed
        mock_get_stats.assert_called_with(PROPERTIES_REMOVED)
        result.relationships_created
        mock_get_stats.assert_called_with(RELATIONSHIPS_CREATED)
        result.relationships_deleted
        mock_get_stats.assert_called_with(RELATIONSHIPS_DELETED)
        result.indices_created
        mock_get_stats.assert_called_with(INDICES_CREATED)
        result.indices_deleted
        mock_get_stats.assert_called_with(INDICES_DELETED)
        result.cached_execution
        mock_get_stats.assert_called_with(CACHED_EXECUTION)
        result.run_time_ms
        mock_get_stats.assert_called_with(INTERNAL_EXECUTION_TIME)
