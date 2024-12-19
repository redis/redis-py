# EXAMPLE: py_home_json
"""
JSON examples from redis-py "home" page"
 https://redis.io/docs/latest/develop/connect/clients/python/redis-py/#example-indexing-and-querying-json-documents
"""

# STEP_START import
import redis
from redis.commands.json.path import Path
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
import redis.exceptions
# STEP_END

# STEP_START connect
r = redis.Redis(decode_responses=True)
# STEP_END

# REMOVE_START
try:
    r.ft("idx:users").dropindex(True)
except redis.exceptions.ResponseError:
    pass

r.delete("user:1", "user:2", "user:3")
# REMOVE_END
# STEP_START create_data
user1 = {
    "name": "Paul John",
    "email": "paul.john@example.com",
    "age": 42,
    "city": "London"
}

user2 = {
    "name": "Eden Zamir",
    "email": "eden.zamir@example.com",
    "age": 29,
    "city": "Tel Aviv"
}

user3 = {
    "name": "Paul Zamir",
    "email": "paul.zamir@example.com",
    "age": 35,
    "city": "Tel Aviv"
}
# STEP_END

# STEP_START make_index
schema = (
    TextField("$.name", as_name="name"),
    TagField("$.city", as_name="city"),
    NumericField("$.age", as_name="age")
)

indexCreated = r.ft("idx:users").create_index(
    schema,
    definition=IndexDefinition(
        prefix=["user:"], index_type=IndexType.JSON
    )
)
# STEP_END
# Tests for 'make_index' step.
# REMOVE_START
assert indexCreated
# REMOVE_END

# STEP_START add_data
user1Set = r.json().set("user:1", Path.root_path(), user1)
user2Set = r.json().set("user:2", Path.root_path(), user2)
user3Set = r.json().set("user:3", Path.root_path(), user3)
# STEP_END
# Tests for 'add_data' step.
# REMOVE_START
assert user1Set
assert user2Set
assert user3Set
# REMOVE_END

# STEP_START query1
findPaulResult = r.ft("idx:users").search(
    Query("Paul @age:[30 40]")
)

print(findPaulResult)
# >>> Result{1 total, docs: [Document {'id': 'user:3', ...
# STEP_END
# Tests for 'query1' step.
# REMOVE_START
assert str(findPaulResult) == (
    "Result{1 total, docs: [Document {'id': 'user:3', 'payload': None, "
    + "'json': '{\"name\":\"Paul Zamir\",\"email\":"
    + "\"paul.zamir@example.com\",\"age\":35,\"city\":\"Tel Aviv\"}'}]}"
)
# REMOVE_END

# STEP_START query2
citiesResult = r.ft("idx:users").search(
    Query("Paul").return_field("$.city", as_field="city")
).docs

print(citiesResult)
# >>> [Document {'id': 'user:1', 'payload': None, ...
# STEP_END
# Tests for 'query2' step.
# REMOVE_START
citiesResult.sort(key=lambda doc: doc['id'])

assert str(citiesResult) == (
    "[Document {'id': 'user:1', 'payload': None, 'city': 'London'}, "
    + "Document {'id': 'user:3', 'payload': None, 'city': 'Tel Aviv'}]"
)
# REMOVE_END

# STEP_START query3
req = aggregations.AggregateRequest("*").group_by(
    '@city', reducers.count().alias('count')
)

aggResult = r.ft("idx:users").aggregate(req).rows
print(aggResult)
# >>> [['city', 'London', 'count', '1'], ['city', 'Tel Aviv', 'count', '2']]
# STEP_END
# Tests for 'query3' step.
# REMOVE_START
aggResult.sort(key=lambda row: row[1])

assert str(aggResult) == (
    "[['city', 'London', 'count', '1'], ['city', 'Tel Aviv', 'count', '2']]"
)
# REMOVE_END

r.close()
