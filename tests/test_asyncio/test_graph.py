import pytest

import redis.asyncio as redis
from redis.commands.graph import Edge, Node, Path
from redis.commands.graph.execution_plan import Operation
from redis.exceptions import ResponseError
from tests.conftest import skip_if_redis_enterprise


@pytest.mark.redismod
async def test_bulk(modclient):
    with pytest.raises(NotImplementedError):
        await modclient.graph().bulk()
        await modclient.graph().bulk(foo="bar!")


@pytest.mark.redismod
async def test_graph_creation(modclient: redis.Redis):
    graph = modclient.graph()

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

    await graph.commit()

    query = (
        'MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country) '
        "RETURN p, v, c"
    )

    result = await graph.query(query)

    person = result.result_set[0][0]
    visit = result.result_set[0][1]
    country = result.result_set[0][2]

    assert person == john
    assert visit.properties == edge.properties
    assert country == japan

    query = """RETURN [1, 2.3, "4", true, false, null]"""
    result = await graph.query(query)
    assert [1, 2.3, "4", True, False, None] == result.result_set[0][0]

    # All done, remove graph.
    await graph.delete()


@pytest.mark.redismod
async def test_array_functions(modclient: redis.Redis):
    graph = modclient.graph()

    query = """CREATE (p:person{name:'a',age:32, array:[0,1,2]})"""
    await graph.query(query)

    query = """WITH [0,1,2] as x return x"""
    result = await graph.query(query)
    assert [0, 1, 2] == result.result_set[0][0]

    query = """MATCH(n) return collect(n)"""
    result = await graph.query(query)

    a = Node(
        node_id=0,
        label="person",
        properties={"name": "a", "age": 32, "array": [0, 1, 2]},
    )

    assert [a] == result.result_set[0][0]


@pytest.mark.redismod
async def test_path(modclient: redis.Redis):
    node0 = Node(node_id=0, label="L1")
    node1 = Node(node_id=1, label="L1")
    edge01 = Edge(node0, "R1", node1, edge_id=0, properties={"value": 1})

    graph = modclient.graph()
    graph.add_node(node0)
    graph.add_node(node1)
    graph.add_edge(edge01)
    await graph.flush()

    path01 = Path.new_empty_path().add_node(node0).add_edge(edge01).add_node(node1)
    expected_results = [[path01]]

    query = "MATCH p=(:L1)-[:R1]->(:L1) RETURN p ORDER BY p"
    result = await graph.query(query)
    assert expected_results == result.result_set


@pytest.mark.redismod
async def test_param(modclient: redis.Redis):
    params = [1, 2.3, "str", True, False, None, [0, 1, 2]]
    query = "RETURN $param"
    for param in params:
        result = await modclient.graph().query(query, {"param": param})
        expected_results = [[param]]
        assert expected_results == result.result_set


@pytest.mark.redismod
async def test_map(modclient: redis.Redis):
    query = "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}"

    actual = (await modclient.graph().query(query)).result_set[0][0]
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
async def test_point(modclient: redis.Redis):
    query = "RETURN point({latitude: 32.070794860, longitude: 34.820751118})"
    expected_lat = 32.070794860
    expected_lon = 34.820751118
    actual = (await modclient.graph().query(query)).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    query = "RETURN point({latitude: 32, longitude: 34.0})"
    expected_lat = 32
    expected_lon = 34
    actual = (await modclient.graph().query(query)).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001


@pytest.mark.redismod
async def test_index_response(modclient: redis.Redis):
    result_set = await modclient.graph().query("CREATE INDEX ON :person(age)")
    assert 1 == result_set.indices_created

    result_set = await modclient.graph().query("CREATE INDEX ON :person(age)")
    assert 0 == result_set.indices_created

    result_set = await modclient.graph().query("DROP INDEX ON :person(age)")
    assert 1 == result_set.indices_deleted

    with pytest.raises(ResponseError):
        await modclient.graph().query("DROP INDEX ON :person(age)")


@pytest.mark.redismod
async def test_stringify_query_result(modclient: redis.Redis):
    graph = modclient.graph()

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

    await graph.commit()

    query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
            RETURN p, v, c"""

    result = await graph.query(query)
    person = result.result_set[0][0]
    visit = result.result_set[0][1]
    country = result.result_set[0][2]

    assert (
        str(person)
        == """(:person{age:33,gender:"male",name:"John Doe",status:"single"})"""  # noqa
    )
    assert str(visit) == """()-[:visited{purpose:"pleasure"}]->()"""
    assert str(country) == """(:country{name:"Japan"})"""

    await graph.delete()


@pytest.mark.redismod
async def test_optional_match(modclient: redis.Redis):
    # Build a graph of form (a)-[R]->(b)
    node0 = Node(node_id=0, label="L1", properties={"value": "a"})
    node1 = Node(node_id=1, label="L1", properties={"value": "b"})

    edge01 = Edge(node0, "R", node1, edge_id=0)

    graph = modclient.graph()
    graph.add_node(node0)
    graph.add_node(node1)
    graph.add_edge(edge01)
    await graph.flush()

    # Issue a query that collects all outgoing edges from both nodes
    # (the second has none)
    query = """MATCH (a) OPTIONAL MATCH (a)-[e]->(b) RETURN a, e, b ORDER BY a.value"""  # noqa
    expected_results = [[node0, edge01, node1], [node1, None, None]]

    result = await graph.query(query)
    assert expected_results == result.result_set

    await graph.delete()


@pytest.mark.redismod
async def test_cached_execution(modclient: redis.Redis):
    await modclient.graph().query("CREATE ()")

    uncached_result = await modclient.graph().query(
        "MATCH (n) RETURN n, $param", {"param": [0]}
    )
    assert uncached_result.cached_execution is False

    # loop to make sure the query is cached on each thread on server
    for x in range(0, 64):
        cached_result = await modclient.graph().query(
            "MATCH (n) RETURN n, $param", {"param": [0]}
        )
        assert uncached_result.result_set == cached_result.result_set

    # should be cached on all threads by now
    assert cached_result.cached_execution


@pytest.mark.redismod
async def test_slowlog(modclient: redis.Redis):
    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
    (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
    (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    await modclient.graph().query(create_query)

    results = await modclient.graph().slowlog()
    assert results[0][1] == "GRAPH.QUERY"
    assert results[0][2] == create_query


@pytest.mark.redismod
async def test_query_timeout(modclient: redis.Redis):
    # Build a sample graph with 1000 nodes.
    await modclient.graph().query("UNWIND range(0,1000) as val CREATE ({v: val})")
    # Issue a long-running query with a 1-millisecond timeout.
    with pytest.raises(ResponseError):
        await modclient.graph().query("MATCH (a), (b), (c), (d) RETURN *", timeout=1)
        assert False is False

    with pytest.raises(Exception):
        await modclient.graph().query("RETURN 1", timeout="str")
        assert False is False


@pytest.mark.redismod
async def test_read_only_query(modclient: redis.Redis):
    with pytest.raises(Exception):
        # Issue a write query, specifying read-only true,
        # this call should fail.
        await modclient.graph().query("CREATE (p:person {name:'a'})", read_only=True)
        assert False is False


@pytest.mark.redismod
async def test_profile(modclient: redis.Redis):
    q = """UNWIND range(1, 3) AS x CREATE (p:Person {v:x})"""
    profile = (await modclient.graph().profile(q)).result_set
    assert "Create | Records produced: 3" in profile
    assert "Unwind | Records produced: 3" in profile

    q = "MATCH (p:Person) WHERE p.v > 1 RETURN p"
    profile = (await modclient.graph().profile(q)).result_set
    assert "Results | Records produced: 2" in profile
    assert "Project | Records produced: 2" in profile
    assert "Filter | Records produced: 2" in profile
    assert "Node By Label Scan | (p:Person) | Records produced: 3" in profile


@pytest.mark.redismod
@skip_if_redis_enterprise()
async def test_config(modclient: redis.Redis):
    config_name = "RESULTSET_SIZE"
    config_value = 3

    # Set configuration
    response = await modclient.graph().config(config_name, config_value, set=True)
    assert response == "OK"

    # Make sure config been updated.
    response = await modclient.graph().config(config_name, set=False)
    expected_response = [config_name, config_value]
    assert response == expected_response

    config_name = "QUERY_MEM_CAPACITY"
    config_value = 1 << 20  # 1MB

    # Set configuration
    response = await modclient.graph().config(config_name, config_value, set=True)
    assert response == "OK"

    # Make sure config been updated.
    response = await modclient.graph().config(config_name, set=False)
    expected_response = [config_name, config_value]
    assert response == expected_response

    # reset to default
    await modclient.graph().config("QUERY_MEM_CAPACITY", 0, set=True)
    await modclient.graph().config("RESULTSET_SIZE", -100, set=True)


@pytest.mark.redismod
@pytest.mark.onlynoncluster
async def test_list_keys(modclient: redis.Redis):
    result = await modclient.graph().list_keys()
    assert result == []

    await modclient.graph("G").query("CREATE (n)")
    result = await modclient.graph().list_keys()
    assert result == ["G"]

    await modclient.graph("X").query("CREATE (m)")
    result = await modclient.graph().list_keys()
    assert result == ["G", "X"]

    await modclient.delete("G")
    await modclient.rename("X", "Z")
    result = await modclient.graph().list_keys()
    assert result == ["Z"]

    await modclient.delete("Z")
    result = await modclient.graph().list_keys()
    assert result == []


@pytest.mark.redismod
async def test_multi_label(modclient: redis.Redis):
    redis_graph = modclient.graph("g")

    node = Node(label=["l", "ll"])
    redis_graph.add_node(node)
    await redis_graph.commit()

    query = "MATCH (n) RETURN n"
    result = await redis_graph.query(query)
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
async def test_execution_plan(modclient: redis.Redis):
    redis_graph = modclient.graph("execution_plan")
    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
    (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
    (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    await redis_graph.query(create_query)

    result = await redis_graph.execution_plan(
        "MATCH (r:Rider)-[:rides]->(t:Team) WHERE t.name = $name RETURN r.name, t.name, $params",  # noqa
        {"name": "Yehuda"},
    )
    expected = "Results\n    Project\n        Conditional Traverse | (t:Team)->(r:Rider)\n            Filter\n                Node By Label Scan | (t:Team)"  # noqa
    assert result == expected

    await redis_graph.delete()


@pytest.mark.redismod
async def test_explain(modclient: redis.Redis):
    redis_graph = modclient.graph("execution_plan")
    # graph creation / population
    create_query = """CREATE
(:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
(:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
(:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    await redis_graph.query(create_query)

    result = await redis_graph.explain(
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

    result = await redis_graph.explain(
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

    await redis_graph.delete()
